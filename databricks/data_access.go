package databricks

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/smithy-go/ptr"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/hashicorp/go-multierror"
	"github.com/raito-io/cli/base/access_provider"
	"github.com/raito-io/cli/base/access_provider/sync_from_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"

	"cli-plugin-databricks/databricks/masks"
	"cli-plugin-databricks/databricks/repo"
	"cli-plugin-databricks/databricks/types"
	"cli-plugin-databricks/utils/array"
)

var _ wrappers.AccessProviderSyncer = (*AccessSyncer)(nil)

const (
	maskPrefix = "raito_"
	idAlphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

//go:generate go run github.com/vektra/mockery/v2 --name=dataAccessAccountRepository
type dataAccessAccountRepository interface {
	ListUsers(ctx context.Context, optFn ...func(options *repo.DatabricksUsersFilter)) <-chan interface{}
	ListGroups(ctx context.Context, optFn ...func(options *repo.DatabricksGroupsFilter)) <-chan interface{}
	ListWorkspaceAssignments(ctx context.Context, workspaceId int) ([]iam.PermissionAssignment, error)
	UpdateWorkspaceAssignment(ctx context.Context, workspaceId int, principalId int64, permission []iam.WorkspacePermission) error
	accountRepository
}

//go:generate go run github.com/vektra/mockery/v2 --name=dataAccessWorkspaceRepository
type dataAccessWorkspaceRepository interface {
	Ping(ctx context.Context) error
	GetPermissionsOnResource(ctx context.Context, securableType catalog.SecurableType, fullName string) (*catalog.PermissionsList, error)
	SetPermissionsOnResource(ctx context.Context, securableType catalog.SecurableType, fullName string, changes ...catalog.PermissionsChange) error
	SqlWarehouseRepository(warehouseId string) repo.WarehouseRepository
	workspaceRepository
}

type AccessSyncer struct {
	accountRepoFactory   func(accountId string, repoCredentials *repo.RepositoryCredentials) dataAccessAccountRepository
	workspaceRepoFactory func(host string, accountId string, repoCredentials *repo.RepositoryCredentials) (dataAccessWorkspaceRepository, error)

	privilegeCache types.PrivilegeCache

	apFeedbackObjects map[string]sync_to_target.AccessProviderSyncFeedback // Cache apFeedback objects
}

func NewAccessSyncer() *AccessSyncer {
	return &AccessSyncer{
		accountRepoFactory: func(accountId string, repoCredentials *repo.RepositoryCredentials) dataAccessAccountRepository {
			return repo.NewAccountRepository(repoCredentials, accountId)
		},
		workspaceRepoFactory: func(host string, accountId string, repoCredentials *repo.RepositoryCredentials) (dataAccessWorkspaceRepository, error) {
			return repo.NewWorkspaceRepository(host, accountId, repoCredentials)
		},

		privilegeCache: types.NewPrivilegeCache(),
	}
}

func (a *AccessSyncer) SyncAccessProvidersFromTarget(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, configMap *config.ConfigMap) (err error) {
	defer func() {
		if err != nil {
			logger.Error(fmt.Sprintf("SyncAccessProvidersFromTarget failed: %s", err.Error()))
		}
	}()

	accountId, repoCredentials, err := getAndValidateParameters(configMap)
	if err != nil {
		return err
	}

	traverser := NewDataObjectTraverser(nil, func() (accountRepository, error) {
		return a.accountRepoFactory(accountId, &repoCredentials), nil
	}, func(metastoreWorkspaces []string) (workspaceRepository, string, error) {
		return selectWorkspaceRepo(ctx, &repoCredentials, accountId, metastoreWorkspaces, a.workspaceRepoFactory)
	}, createFullName)

	masks := make(map[string][]string)

	err = traverser.Traverse(ctx, func(ctx context.Context, securableType string, parentObject interface{}, object interface{}, metastore *string) error {
		metastoreSync := func(f func(repo dataAccessWorkspaceRepository) error) error {
			client, err2 := a.workspaceRepoFactory(GetWorkspaceAddress(*metastore), accountId, &repoCredentials)
			if err2 != nil {
				return err2
			}

			return f(client)
		}

		switch securableType {
		case workspaceType:
			return a.syncFromTargetWorkspace(ctx, accessProviderHandler, accountId, &repoCredentials, object)
		case metastoreType:
			return metastoreSync(func(repo dataAccessWorkspaceRepository) error {
				return a.syncFromTargetMetastore(ctx, accessProviderHandler, repo, object)
			})
		case catalogType:
			return metastoreSync(func(repo dataAccessWorkspaceRepository) error {
				return a.syncFromTargetCatalog(ctx, accessProviderHandler, repo, object)
			})
		case data_source.Schema:
			return metastoreSync(func(repo dataAccessWorkspaceRepository) error {
				return a.syncFromTargetSchema(ctx, accessProviderHandler, repo, object)
			})
		case data_source.Table:
			return metastoreSync(func(repo dataAccessWorkspaceRepository) error {
				return a.syncFromTargetTable(ctx, accessProviderHandler, repo, object)
			})
		case data_source.Column:
			var cerr error
			masks, cerr = a.syncFromTargetColumn(ctx, masks, parentObject, object)

			return cerr
		case functionType:
			return metastoreSync(func(repo dataAccessWorkspaceRepository) error {
				return a.syncFromTargetFunction(ctx, accessProviderHandler, repo, masks, object)
			})
		}

		return fmt.Errorf("unsupported type %s", securableType)
	}, func(traverserOptions *DataObjectTraverserOptions) {
		traverserOptions.SecurableTypesToReturn = set.NewSet[string](workspaceType, metastoreType, catalogType, data_source.Schema, data_source.Table, data_source.Column, functionType)
	})

	if err != nil {
		return err
	}

	return nil
}

func (a *AccessSyncer) syncFromTargetWorkspace(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, accountId string, repoCredentials *repo.RepositoryCredentials, object interface{}) error {
	workspace, ok := object.(*repo.Workspace)
	if !ok {
		return fmt.Errorf("unable to parse Workspace. Expected *catalog.WorkspaceInfo but got %T", object)
	}

	accountClient := a.accountRepoFactory(accountId, repoCredentials)

	assignments, err := accountClient.ListWorkspaceAssignments(ctx, workspace.WorkspaceId)
	if err != nil {
		return err
	}

	privilegesToSync := make(map[string][]string)

	logger.Debug(fmt.Sprintf("Found %d workspace assignments for workspace %s", len(assignments), workspace.WorkspaceName))

	for _, assignment := range assignments {
		var principalId string

		if assignment.Principal.UserName != "" {
			principalId = assignment.Principal.UserName
		} else if assignment.Principal.GroupName != "" {
			principalId = assignment.Principal.GroupName
		} else if assignment.Principal.ServicePrincipalName != "" {
			logger.Warn(fmt.Sprintf("Service principles assignments are not supported at this moment. Skipping assignment for service principal %s", assignment.Principal.ServicePrincipalName))
			continue
		} else {
			logger.Error(fmt.Sprintf("Unknown principal assignment type %+v", assignment.Principal))

			continue
		}

		do := data_source.DataObjectReference{FullName: strconv.Itoa(workspace.WorkspaceId), Type: workspaceType}

		for _, permission := range assignment.Permissions {
			p := string(permission)
			if !a.privilegeCache.ContainsPrivilege(do, principalId, p) {
				privilegesToSync[p] = append(privilegesToSync[p], principalId)
			}
		}
	}

	for privilege, principleList := range privilegesToSync {
		apName := fmt.Sprintf("%s_%s", workspace.WorkspaceName, privilege)

		whoItems := sync_from_target.WhoItem{}

		for _, principal := range principleList {
			// We assume that a group doesn't contain an @ character
			if strings.Contains(principal, "@") {
				whoItems.Users = append(whoItems.Users, principal)
			} else {
				whoItems.Groups = append(whoItems.Groups, principal)
			}
		}

		err2 := accessProviderHandler.AddAccessProviders(
			&sync_from_target.AccessProvider{
				ExternalId: apName,
				Action:     sync_from_target.Grant,
				Name:       apName,
				NamingHint: apName,
				ActualName: apName,
				Type:       ptr.String(access_provider.AclSet),
				What: []sync_from_target.WhatItem{
					{
						DataObject:  &data_source.DataObjectReference{FullName: strconv.Itoa(workspace.WorkspaceId), Type: workspaceType},
						Permissions: []string{privilege},
					},
				},
				Who: &whoItems,
			},
		)
		if err2 != nil {
			return err2
		}
	}

	return nil
}

func (a *AccessSyncer) syncFromTargetMetastore(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, workspaceClient dataAccessWorkspaceRepository, object interface{}) error {
	metastore, ok := object.(*catalog.MetastoreInfo)
	if !ok {
		return fmt.Errorf("unable to parse Metastore. Expected *catalog.MetastoreInfo but got %T", object)
	}

	logger.Debug(fmt.Sprintf("Load permissions on metastore %q", metastore.MetastoreId))

	permissionsList, err := workspaceClient.GetPermissionsOnResource(ctx, catalog.SecurableTypeMetastore, metastore.MetastoreId)
	if err != nil {
		return err
	}

	logger.Debug(fmt.Sprintf("Process permission on metastore %q", metastore.Name))

	err = a.addPermissionIfNotSetByRaito(accessProviderHandler, &data_source.DataObjectReference{FullName: metastore.Name, Type: metastoreType}, permissionsList)
	if err != nil {
		return err
	}

	return nil
}

func (a *AccessSyncer) syncFromTargetCatalog(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, workspaceClient dataAccessWorkspaceRepository, object interface{}) error {
	c, ok := object.(*catalog.CatalogInfo)
	if !ok {
		return fmt.Errorf("unable to parse Catalog. Expected *catalog.CatalogInfo but got %T", object)
	}

	return a.syncAccessDataObjectFromTarget(ctx, accessProviderHandler, workspaceClient, c.MetastoreId, c.FullName, catalogType, catalog.SecurableTypeCatalog)
}

func (a *AccessSyncer) syncFromTargetSchema(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, workspaceClient dataAccessWorkspaceRepository, object interface{}) error {
	schema, ok := object.(*catalog.SchemaInfo)
	if !ok {
		return fmt.Errorf("unable to parse Schema. Expected *catalog.SchemaInfo but got %T", object)
	}

	return a.syncAccessDataObjectFromTarget(ctx, accessProviderHandler, workspaceClient, schema.MetastoreId, schema.FullName, data_source.Schema, catalog.SecurableTypeSchema)
}

func (a *AccessSyncer) syncFromTargetTable(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, workspaceClient dataAccessWorkspaceRepository, object interface{}) error {
	table, ok := object.(*catalog.TableInfo)
	if !ok {
		return fmt.Errorf("unable to parse Table. Expected *catalog.TableInfo but got %T", object)
	}

	return a.syncAccessDataObjectFromTarget(ctx, accessProviderHandler, workspaceClient, table.MetastoreId, table.FullName, data_source.Table, catalog.SecurableTypeTable)
}

func (a *AccessSyncer) syncFromTargetColumn(_ context.Context, masks map[string][]string, parent interface{}, object interface{}) (map[string][]string, error) {
	column, ok := object.(*catalog.ColumnInfo)
	if !ok {
		return masks, fmt.Errorf("unable to parse Column. Expected *catalog.ColumnInfo but got %T", object)
	}

	table, ok := parent.(*catalog.TableInfo)
	if !ok {
		return masks, fmt.Errorf("unable to parse Table. Expected *catalog.TableInfo but got %T", parent)
	}

	if column.Mask != nil {
		functionId := createUniqueId(table.MetastoreId, column.Mask.FunctionName)
		masks[functionId] = append(masks[functionId], createTableUniqueId(table.MetastoreId, table.FullName, column.Name))
	}

	return masks, nil
}

func (a *AccessSyncer) syncFromTargetFunction(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, workspaceClient dataAccessWorkspaceRepository, masks map[string][]string, object interface{}) error {
	function, ok := object.(*repo.FunctionInfo)
	if !ok {
		return fmt.Errorf("unable to parse Function. Expected *catalog.FunctionInfo but got %T", object)
	}

	functionId := createUniqueId(function.MetastoreId, function.FullName)
	if columns, found := masks[functionId]; found {
		what := make([]sync_from_target.WhatItem, 0, len(columns))

		for _, column := range columns {
			what = append(what, sync_from_target.WhatItem{
				DataObject: &data_source.DataObjectReference{FullName: column, Type: data_source.Column},
			})
		}

		return accessProviderHandler.AddAccessProviders(&sync_from_target.AccessProvider{
			ExternalId:        functionId,
			Name:              function.Name,
			ActualName:        functionId,
			Policy:            function.RoutineDefinition,
			Action:            sync_from_target.Mask,
			What:              what,
			NotInternalizable: true,
			Incomplete:        ptr.Bool(true),
		})
	} else {
		return a.syncAccessDataObjectFromTarget(ctx, accessProviderHandler, workspaceClient, function.MetastoreId, function.FullName, functionType, catalog.SecurableTypeFunction)
	}
}

func (a *AccessSyncer) syncAccessDataObjectFromTarget(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, workspaceClient dataAccessWorkspaceRepository, metastoreId, fullName string, doType string, securableType catalog.SecurableType) error {
	permissionsList, err := workspaceClient.GetPermissionsOnResource(ctx, securableType, fullName)
	if err != nil {
		return err
	}

	return a.addPermissionIfNotSetByRaito(accessProviderHandler, &data_source.DataObjectReference{FullName: createUniqueId(metastoreId, fullName), Type: doType}, permissionsList)
}

func (a *AccessSyncer) SyncAccessAsCodeToTarget(_ context.Context, _ *sync_to_target.AccessProviderImport, _ string, _ *config.ConfigMap) error {
	panic("Databricks plugin does not support syncing access as code to target")
}

func (a *AccessSyncer) SyncAccessProviderToTarget(ctx context.Context, accessProviders *sync_to_target.AccessProviderImport, accessProviderFeedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) (err error) {
	defer func() {
		if err != nil {
			logger.Error(fmt.Sprintf("SyncAccessProviderToTarget failed: %s", err.Error()))
		}
	}()

	accountId, repoCredentials, err := getAndValidateParameters(configMap)
	if err != nil {
		return err
	}

	accountRepo := a.accountRepoFactory(accountId, &repoCredentials)

	_, _, metastoreWorkspaceMap, err := a.loadMetastores(ctx, configMap)
	metastoreClientCache := make(map[string]dataAccessWorkspaceRepository)

	getMetastoreClient := func(metastoreId string) (dataAccessWorkspaceRepository, error) {
		if repo, ok := metastoreClientCache[metastoreId]; ok {
			return repo, nil
		}

		repo, _, werr := selectWorkspaceRepo(ctx, &repoCredentials, accountId, metastoreWorkspaceMap[metastoreId], a.workspaceRepoFactory)
		if werr != nil {
			return nil, werr
		}

		metastoreClientCache[metastoreId] = repo

		return repo, nil
	}

	permissionsChanges := types.NewPrivilegesChangeCollection()
	a.apFeedbackObjects = make(map[string]sync_to_target.AccessProviderSyncFeedback)

	for i := range accessProviders.AccessProviders {
		feedbackElement := a.syncAccessProviderToTarget(ctx, accessProviders.AccessProviders[i], &permissionsChanges, configMap)

		a.apFeedbackObjects[accessProviders.AccessProviders[i].Id] = feedbackElement
	}

	defer func() {
		for _, feedbackItem := range a.apFeedbackObjects {
			fbErr := accessProviderFeedbackHandler.AddAccessProviderFeedback(feedbackItem)
			if fbErr != nil {
				err = multierror.Append(err, fbErr)
			}
		}

		a.apFeedbackObjects = nil
	}()

	for item, principlePrivilegesMap := range permissionsChanges.M {
		if item.Type == workspaceType {
			a.storePrivilegesInComputePlane(ctx, item, principlePrivilegesMap, accountRepo)
		} else {
			a.storePrivilegesInDataplane(ctx, item, getMetastoreClient, principlePrivilegesMap)
		}
	}

	return nil
}

func (a *AccessSyncer) syncAccessProviderToTarget(ctx context.Context, accessProvider *sync_to_target.AccessProvider, permissionsChanges *types.PrivilegesChangeCollection, configMap *config.ConfigMap) sync_to_target.AccessProviderSyncFeedback {
	feedbackElement := sync_to_target.AccessProviderSyncFeedback{
		AccessProvider: accessProvider.Id,
	}

	switch accessProvider.Action {
	case sync_to_target.Mask:
		maskName, apErr := a.syncMaskToTarget(ctx, accessProvider, configMap)

		feedbackElement.ExternalId = &maskName
		feedbackElement.ActualName = maskName

		if apErr != nil {
			feedbackElement.Errors = append(feedbackElement.Errors, apErr.Error())
		}
	case sync_to_target.Grant, sync_to_target.Purpose:
		feedbackElement.ActualName = accessProvider.Id
		feedbackElement.Type = ptr.String(access_provider.AclSet)

		apErr := a.syncGrantToTarget(ctx, accessProvider, permissionsChanges)
		if apErr != nil {
			feedbackElement.Errors = append(feedbackElement.Errors, apErr.Error())
		}
	default:
		feedbackElement.Errors = append(feedbackElement.Errors, fmt.Sprintf("Unsupported action: %d", accessProvider.Action))
	}

	return feedbackElement
}

func (a *AccessSyncer) storePrivilegesInComputePlane(ctx context.Context, item types.SecurableItemKey, principlePrivilegesMap map[string]*types.PrivilegesChanges, repo dataAccessAccountRepository) {
	workspaceId, err := strconv.Atoi(item.FullName)
	if err != nil {
		for _, privilegesChanges := range principlePrivilegesMap {
			a.handleAccessProviderError(privilegesChanges, err)
		}

		return
	}

	for principal, privilegesChanges := range principlePrivilegesMap {
		a.storePrivilegesInComputePlaneForPrincipal(ctx, principal, repo, workspaceId, privilegesChanges)
	}
}

func (a *AccessSyncer) storePrivilegesInComputePlaneForPrincipal(ctx context.Context, principal string, repo dataAccessAccountRepository, workspaceId int, privilegesChanges *types.PrivilegesChanges) {
	var err error

	defer func() {
		if err != nil {
			a.handleAccessProviderError(privilegesChanges, err)
		}
	}()

	var principalId int64

	if strings.Contains(principal, "@") {
		var user *iam.User

		user, err = a.getUserFromEmail(ctx, principal, repo)
		if err != nil {
			return
		}

		principalId, err = strconv.ParseInt(user.Id, 10, 64)
		if err != nil {
			return
		}
	} else {
		var group *iam.Group

		group, err = a.getGroupIdFromName(ctx, principal, repo)
		if err != nil {
			return
		}

		principalId, err = strconv.ParseInt(group.Id, 10, 64)
		if err != nil {
			return
		}
	}

	err = repo.UpdateWorkspaceAssignment(ctx, workspaceId, principalId, workspacePermissionsToDatabricksPermissions(privilegesChanges.Add.Slice()))
	if err != nil {
		return
	}
}

func (a *AccessSyncer) handleAccessProviderError(privilegeChanges *types.PrivilegesChanges, err error) {
	for ap := range privilegeChanges.AssociatedAPs {
		fo := a.apFeedbackObjects[ap]
		fo.Errors = append(fo.Errors, err.Error())
		a.apFeedbackObjects[ap] = fo
	}
}

func (a *AccessSyncer) getUserFromEmail(ctx context.Context, email string, accountRepo dataAccessAccountRepository) (*iam.User, error) {
	cancelCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	users := accountRepo.ListUsers(cancelCtx, func(options *repo.DatabricksUsersFilter) { options.Username = &email })
	for user := range users {
		switch v := user.(type) {
		case error:
			return nil, v
		case iam.User:
			return &v, nil
		}
	}

	return nil, fmt.Errorf("no user found for email %q", email)
}

func (a *AccessSyncer) getGroupIdFromName(ctx context.Context, groupname string, accountRepo dataAccessAccountRepository) (*iam.Group, error) {
	cancelCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	groups := accountRepo.ListGroups(cancelCtx, func(options *repo.DatabricksGroupsFilter) { options.Groupname = &groupname })
	for user := range groups {
		switch v := user.(type) {
		case error:
			return nil, v
		case iam.Group:
			return &v, nil
		}
	}

	return nil, fmt.Errorf("no groupe found with name %q", groupname)
}

func (a *AccessSyncer) storePrivilegesInDataplane(ctx context.Context, item types.SecurableItemKey, getMetastoreClient func(metastoreId string) (dataAccessWorkspaceRepository, error), principlePrivilegesMap map[string]*types.PrivilegesChanges) {
	var metastore, fullname string
	var err error

	defer func() {
		if err != nil {
			for _, privilegesChanges := range principlePrivilegesMap {
				a.handleAccessProviderError(privilegesChanges, err)
			}
		}
	}()

	if item.Type == metastoreType {
		metastore = item.FullName
		fullname = item.FullName
	} else {
		metastore, fullname = getMetastoreAndFullnameOfUniqueId(item.FullName)
	}

	repo, err := getMetastoreClient(metastore)
	if err != nil {
		return
	}

	changes := make([]catalog.PermissionsChange, 0, len(principlePrivilegesMap))

	for principal, privilegesChanges := range principlePrivilegesMap {
		addSlice := privilegesChanges.Add.Slice()
		privilegesChanges.Remove.RemoveAll(addSlice...)

		addPrivilages := array.Map(addSlice, func(i *string) catalog.Privilege {
			return catalog.Privilege(*i)
		})

		changes = append(changes, catalog.PermissionsChange{
			Principal: principal,
			Add:       addPrivilages,
			Remove:    array.Map(privilegesChanges.Remove.Slice(), func(i *string) catalog.Privilege { return catalog.Privilege(*i) }),
		})
	}

	securableType, err := typeToSecurableType(item.Type)
	if err != nil {
		return
	}

	err = repo.SetPermissionsOnResource(ctx, securableType, fullname, changes...)
	if err != nil {
		return
	}
}

func (a *AccessSyncer) syncMaskToTarget(ctx context.Context, ap *sync_to_target.AccessProvider, configMap *config.ConfigMap) (maskName string, _ error) {
	// 0. Prepare mask update
	if ap.ExternalId != nil {
		maskName = *ap.ExternalId
	} else {
		maskName = raitoMaskName(ap.Name)
	}

	logger.Debug(fmt.Sprintf("Syncing mask %q to target", maskName))

	schemas := a.syncMasksGetSchema(ap)

	warehouseIdMap := make(map[string]types.WarehouseDetails)

	if found, err := configMap.Unmarshal(DatabricksSqlWarehouses, &warehouseIdMap); err != nil {
		return maskName, err
	} else if !found {
		return maskName, fmt.Errorf("no warehouses found in configmap")
	}

	accountId, repoCredentials, err := getAndValidateParameters(configMap)
	if err != nil {
		return maskName, err
	}

	// Load beneficiaries
	var beneficiaries *masks.MaskingBeneficiaries

	if !ap.Delete {
		beneficiaries = &masks.MaskingBeneficiaries{
			Users:  ap.Who.Users,
			Groups: ap.Who.Groups,
		}
	}

	// 1. Update masks per schema
	for schema, dos := range schemas {
		err = a.syncMaskInSchema(ctx, ap, schema, warehouseIdMap, maskName, accountId, repoCredentials, dos, beneficiaries)
		if err != nil {
			return maskName, err
		}
	}

	return maskName, nil
}

func (a *AccessSyncer) syncMaskInSchema(ctx context.Context, ap *sync_to_target.AccessProvider, schema string, warehouseIdMap map[string]types.WarehouseDetails, maskName string, accountId string, repoCredentials repo.RepositoryCredentials, dos types.MaskDataObjectsOfSchema, beneficiaries *masks.MaskingBeneficiaries) error {
	schemaNameSplit := strings.Split(schema, ".")
	metastore := schemaNameSplit[0]
	catalogName := schemaNameSplit[1]
	schemaName := schemaNameSplit[2]

	warehouseId, ok := warehouseIdMap[metastore]
	if !ok {
		return fmt.Errorf("no warehouse found for metastore %q", metastore)
	}

	repository, err := a.workspaceRepoFactory(GetWorkspaceAddress(warehouseId.Workspace), accountId, &repoCredentials)
	if err != nil {
		return err
	}

	sqlClient := repository.SqlWarehouseRepository(warehouseId.Warehouse)

	maskingFactory := masks.NewMaskFactory()

	if ap.Delete {
		err = a.deleteMaskInSchema(ctx, maskName, schema, dos, sqlClient, catalogName, schemaName)
		if err != nil {
			return err
		}
	} else {
		err := a.updateMaskInSchema(ctx, ap, dos, sqlClient, catalogName, schemaName, maskName, maskingFactory, beneficiaries)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *AccessSyncer) updateMaskInSchema(ctx context.Context, ap *sync_to_target.AccessProvider, dos types.MaskDataObjectsOfSchema, sqlClient repo.WarehouseRepository, catalogName string, schemaName string, maskName string, maskingFactory *masks.MaskFactory, beneficiaries *masks.MaskingBeneficiaries) error {
	for table, columns := range dos.DeletedDataObjects {
		tableInformation, err := sqlClient.GetTableInformation(ctx, catalogName, schemaName, table)
		if err != nil {
			return err
		}

		for _, column := range columns {
			if columnDetails, found := tableInformation[column]; found && columnDetails.Mask != nil && *columnDetails.Mask == maskName {
				err = sqlClient.DropMask(ctx, catalogName, schemaName, table, column)
				if err != nil {
					return err
				}
			}
		}
	}

	types := set.NewSet[string]()
	tableInformationMap := map[string]map[string]*repo.ColumnInformation{}

	for table, columns := range dos.DataObjects {
		tableInfo, err := sqlClient.GetTableInformation(ctx, catalogName, schemaName, table)
		if err != nil {
			return err
		}

		tableInformationMap[table] = tableInfo

		for _, column := range columns {
			if columnDetails, found := tableInfo[column]; found {
				types.Add(columnDetails.Type)
			}
		}
	}

	typeNameMap := make(map[string]string)

	for columnType := range types {
		functionName, functionStatment, err := maskingFactory.CreateMask(maskName, columnType, ap.Type, beneficiaries)
		if err != nil {
			return err
		}

		_, err = sqlClient.ExecuteStatement(ctx, catalogName, schemaName, string(functionStatment))
		if err != nil {
			return err
		}

		typeNameMap[columnType] = functionName
	}

	for table, columns := range dos.DataObjects {
		tableInfo := tableInformationMap[table]

		for _, column := range columns {
			columnInfo := tableInfo[column]
			functionName := typeNameMap[columnInfo.Type]

			err := sqlClient.SetMask(ctx, catalogName, schemaName, table, column, functionName)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *AccessSyncer) deleteMaskInSchema(ctx context.Context, maskName string, schema string, dos types.MaskDataObjectsOfSchema, sqlClient repo.WarehouseRepository, catalogName string, schemaName string) error {
	logger.Debug(fmt.Sprintf("Deleting mask %q for schema %q", maskName, schema))

	masks := set.NewSet[string]()

	// Delete mask from all columns
	for table, columns := range dos.AllDataObjects() {
		tableInformation, err := sqlClient.GetTableInformation(ctx, catalogName, schemaName, table)
		if err != nil {
			return err
		}

		logger.Debug(fmt.Sprintf("Table information for table '%s.%s.%s: %+v", catalogName, schemaName, table, tableInformation))

		for _, column := range columns {
			if columnDetails, found := tableInformation[column]; found && columnDetails.Mask != nil && strings.HasPrefix(*columnDetails.Mask, maskName) {
				err = sqlClient.DropMask(ctx, catalogName, schemaName, table, column)
				if err != nil {
					return err
				}

				masks.Add(*tableInformation[column].Mask)
			}
		}
	}

	for existingMaskName := range masks {
		err := sqlClient.DropFunction(ctx, catalogName, schemaName, existingMaskName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *AccessSyncer) syncMasksGetSchema(ap *sync_to_target.AccessProvider) map[string]types.MaskDataObjectsOfSchema {
	schemas := make(map[string]types.MaskDataObjectsOfSchema)

	for _, whatItem := range ap.What {
		doFullNameSplit := strings.Split(whatItem.DataObject.FullName, ".")
		schema := strings.Join(doFullNameSplit[:3], ".")
		table := doFullNameSplit[3]
		column := doFullNameSplit[4]

		if _, ok := schemas[schema]; !ok {
			schemas[schema] = types.MaskDataObjectsOfSchema{
				DataObjects:        map[string][]string{table: {column}},
				DeletedDataObjects: make(map[string][]string),
			}
		} else {
			dos := schemas[schema]
			dos.DataObjects[table] = append(dos.DataObjects[table], column)
			schemas[schema] = dos
		}
	}

	for _, whatItem := range ap.DeleteWhat {
		doFullNameSplit := strings.Split(whatItem.DataObject.FullName, ".")
		schema := strings.Join(doFullNameSplit[:3], ".")
		table := doFullNameSplit[3]
		column := doFullNameSplit[4]

		if _, ok := schemas[schema]; !ok {
			schemas[schema] = types.MaskDataObjectsOfSchema{
				DeletedDataObjects: map[string][]string{table: {column}},
			}
		} else {
			dos := schemas[schema]
			dos.DeletedDataObjects[table] = append(dos.DeletedDataObjects[table], column)
			schemas[schema] = dos
		}
	}

	return schemas
}

func (a *AccessSyncer) syncGrantToTarget(_ context.Context, ap *sync_to_target.AccessProvider, changeCollection *types.PrivilegesChangeCollection) error {
	logger.Debug(fmt.Sprintf("Syncing access provider %q to target", ap.Name))

	principals := make([]string, 0, len(ap.Who.Users)+len(ap.Who.Groups))
	principals = append(principals, ap.Who.Users...)
	principals = append(principals, ap.Who.Groups...)

	var deletedPrincipals []string

	if ap.DeletedWho != nil {
		deletedPrincipals = make([]string, 0, len(ap.DeletedWho.Users)+len(ap.DeletedWho.Groups))
		deletedPrincipals = append(deletedPrincipals, ap.DeletedWho.Users...)
		deletedPrincipals = append(deletedPrincipals, ap.DeletedWho.Groups...)
	}

	for i := range ap.What {
		removePrivilegesMap, addPrivilegesMap, err := permissionsToDatabricksPrivileges(&ap.What[i])
		if err != nil {
			return err
		}

		for do, privileges := range removePrivilegesMap {
			itemKey := types.SecurableItemKey{
				Type:     do.Type,
				FullName: do.FullName,
			}

			privilegesSlice := privileges.Slice()

			if ap.Delete {
				for _, principal := range principals {
					changeCollection.RemovePrivilege(itemKey, principal, privilegesSlice...)
				}
			}

			for _, deletedPrincipal := range deletedPrincipals {
				changeCollection.RemovePrivilege(itemKey, deletedPrincipal, privilegesSlice...)
			}
		}

		for do, privileges := range addPrivilegesMap {
			itemKey := types.SecurableItemKey{
				Type:     do.Type,
				FullName: do.FullName,
			}

			privilegesSlice := privileges.Slice()

			if !ap.Delete {
				for _, principal := range principals {
					changeCollection.AddPrivilege(itemKey, ap.Id, principal, privilegesSlice...)

					//Add to cache, it must be ignored in sync from target
					a.privilegeCache.AddPrivilege(do, principal, privilegesSlice...)
				}
			}
		}
	}

	for i := range ap.DeleteWhat {
		privilegesMap, _, err := permissionsToDatabricksPrivileges(&ap.DeleteWhat[i])
		if err != nil {
			return err
		}

		for do, privileges := range privilegesMap {
			itemKey := types.SecurableItemKey{
				Type:     do.Type,
				FullName: do.FullName,
			}

			privilegeSlice := privileges.Slice()

			for _, principal := range principals {
				changeCollection.RemovePrivilege(itemKey, principal, privilegeSlice...)
			}

			for _, deletedPrincipal := range deletedPrincipals {
				changeCollection.RemovePrivilege(itemKey, deletedPrincipal, privilegeSlice...)
			}
		}
	}

	return nil
}

func (a *AccessSyncer) addPermissionIfNotSetByRaito(accessProviderHandler wrappers.AccessProviderHandler, do *data_source.DataObjectReference, assignments *catalog.PermissionsList) error {
	if assignments == nil {
		return nil
	}

	privilegeToPrincipleMap := make(map[catalog.Privilege][]string)

	for _, assignment := range assignments.PrivilegeAssignments {
		for _, privilege := range assignment.Privileges {
			logger.Debug(fmt.Sprintf("Check if privilege was assigned by Raito: {%s, %s}, %s, %v", do.FullName, do.Type, assignment.Principal, privilege))

			if a.privilegeCache.ContainsPrivilege(*do, assignment.Principal, string(privilege)) {
				logger.Debug(fmt.Sprintf("Privilege was assigned by Raito and will be ignored: %v, %s, %v", *do, assignment.Principal, privilege))
				continue
			}

			privilegeToPrincipleMap[privilege] = append(privilegeToPrincipleMap[privilege], assignment.Principal)
		}
	}

	for privilege, principleList := range privilegeToPrincipleMap {
		apName := fmt.Sprintf("%s_%s", do.FullName, privilege.String())

		whoItems := sync_from_target.WhoItem{}

		for _, principal := range principleList {
			// We assume that a group doesn't contain an @ character
			if strings.Contains(principal, "@") {
				whoItems.Users = append(whoItems.Users, principal)
			} else {
				whoItems.Groups = append(whoItems.Groups, principal)
			}
		}

		err := accessProviderHandler.AddAccessProviders(
			&sync_from_target.AccessProvider{
				ExternalId: apName,
				Action:     sync_from_target.Grant,
				Name:       apName,
				NamingHint: apName,
				ActualName: apName,
				Type:       ptr.String(access_provider.AclSet),
				What: []sync_from_target.WhatItem{
					{
						DataObject:  do,
						Permissions: []string{strings.ToUpper(strings.ReplaceAll(privilege.String(), "_", " "))},
					},
				},
				Who: &whoItems,
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *AccessSyncer) loadMetastores(ctx context.Context, configMap *config.ConfigMap) ([]catalog.MetastoreInfo, []repo.Workspace, map[string][]string, error) {
	accountId, repoCredentials, err := getAndValidateParameters(configMap)
	if err != nil {
		return nil, nil, nil, err
	}

	accountClient := a.accountRepoFactory(accountId, &repoCredentials)

	metastores, err := accountClient.ListMetastores(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	if len(metastores) == 0 {
		return nil, nil, nil, nil
	}

	workspaces, err := accountClient.GetWorkspaces(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	metastoreWorkspaceMap, _, err := accountClient.GetWorkspaceMap(ctx, metastores, workspaces)
	if err != nil {
		return nil, nil, nil, err
	}

	return metastores, workspaces, metastoreWorkspaceMap, nil
}

func typeToSecurableType(t string) (catalog.SecurableType, error) {
	switch t {
	case metastoreType:
		return catalog.SecurableTypeMetastore, nil
	case catalogType:
		return catalog.SecurableTypeCatalog, nil
	case data_source.Schema:
		return catalog.SecurableTypeSchema, nil
	case data_source.Table:
		return catalog.SecurableTypeTable, nil
	case functionType:
		return catalog.SecurableTypeFunction, nil
	default:
		return "", fmt.Errorf("unknown type %q", t)
	}
}

func permissionsToDatabricksPrivileges(whatItem *sync_to_target.WhatItem) (map[data_source.DataObjectReference]set.Set[string], map[data_source.DataObjectReference]set.Set[string], error) {
	objectPermissions := make(map[data_source.DataObjectReference]set.Set[string])
	additionObjectPermissions := make(map[data_source.DataObjectReference]set.Set[string])

	for _, v := range whatItem.Permissions {
		priv := permissionToDatabricksPrivilege(v)

		addToSetInMap(objectPermissions, *whatItem.DataObject, priv)
		addToSetInMap(additionObjectPermissions, *whatItem.DataObject, priv)

		err := addUsageToUpperDataObjects(additionObjectPermissions, *whatItem.DataObject)
		if err != nil {
			return nil, nil, err
		}
	}

	return objectPermissions, additionObjectPermissions, nil
}

func permissionToDatabricksPrivilege(p string) string {
	return strings.ToUpper(strings.ReplaceAll(p, " ", "_"))
}

func addUsageToUpperDataObjects(result map[data_source.DataObjectReference]set.Set[string], object data_source.DataObjectReference) error {
	switch object.Type {
	case metastoreType, workspaceType:
		return nil
	case catalogType:
		addToSetInMap(result, object, string(catalog.PrivilegeUseCatalog))

		fullname, err := cutLastPartFullName(object.FullName)
		if err != nil {
			return err
		}

		return addUsageToUpperDataObjects(result, data_source.DataObjectReference{FullName: fullname, Type: metastoreType})
	case data_source.Schema:
		addToSetInMap(result, object, string(catalog.PrivilegeUseSchema))

		fullname, err := cutLastPartFullName(object.FullName)
		if err != nil {
			return err
		}

		return addUsageToUpperDataObjects(result, data_source.DataObjectReference{FullName: fullname, Type: catalogType})
	case data_source.Table, data_source.View, functionType:
		fullname, err := cutLastPartFullName(object.FullName)
		if err != nil {
			return err
		}

		return addUsageToUpperDataObjects(result, data_source.DataObjectReference{FullName: fullname, Type: data_source.Schema})
	default:
		return fmt.Errorf("unknown type %q", object.Type)
	}
}

func cutLastPartFullName(fullName string) (string, error) {
	i := strings.LastIndex(fullName, ".")
	if i == -1 {
		return "", fmt.Errorf("unable to find leading part of %s", fullName)
	}

	return fullName[:i], nil
}

func workspacePermissionsToDatabricksPermissions(p []string) []iam.WorkspacePermission {
	result := make([]iam.WorkspacePermission, 0, len(p))

	for _, v := range p {
		permission := iam.WorkspacePermission(v)
		result = append(result, permission)
	}

	return result
}

func raitoMaskName(name string) string {
	return strings.ToLower(fmt.Sprintf("%s%s", maskPrefix, strings.ReplaceAll(strings.ToUpper(name), " ", "_")))
}
