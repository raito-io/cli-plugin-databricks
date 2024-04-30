package databricks

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/smithy-go/ptr"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/hashicorp/go-multierror"
	"github.com/raito-io/bexpression"
	"github.com/raito-io/cli/base/access_provider"
	"github.com/raito-io/cli/base/access_provider/sync_from_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"

	"cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/databricks/masks"
	"cli-plugin-databricks/databricks/platform"
	"cli-plugin-databricks/databricks/repo"
	"cli-plugin-databricks/databricks/types"
	"cli-plugin-databricks/utils/array"
)

var _ wrappers.AccessProviderSyncer = (*AccessSyncer)(nil)

const (
	raitoPrefix = "raito_"
	idAlphabet  = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
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
	GetOwner(ctx context.Context, securableType catalog.SecurableType, fullName string) (string, error)
	workspaceRepository
}

type AccessSyncer struct {
	accountRepoFactory   func(pltfrm platform.DatabricksPlatform, accountId string, repoCredentials *repo.RepositoryCredentials) (dataAccessAccountRepository, error)
	workspaceRepoFactory func(pltfrm platform.DatabricksPlatform, host string, accountId string, repoCredentials *repo.RepositoryCredentials) (dataAccessWorkspaceRepository, error)

	privilegeCache types.PrivilegeCache

	apFeedbackObjects map[string]sync_to_target.AccessProviderSyncFeedback // Cache apFeedback objects
}

func NewAccessSyncer() *AccessSyncer {
	return &AccessSyncer{
		accountRepoFactory: func(pltfrm platform.DatabricksPlatform, accountId string, repoCredentials *repo.RepositoryCredentials) (dataAccessAccountRepository, error) {
			return repo.NewAccountRepository(pltfrm, repoCredentials, accountId)
		},
		workspaceRepoFactory: func(pltfrm platform.DatabricksPlatform, host string, accountId string, repoCredentials *repo.RepositoryCredentials) (dataAccessWorkspaceRepository, error) {
			return repo.NewWorkspaceRepository(pltfrm, host, accountId, repoCredentials)
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

	pltfrm, accountId, repoCredentials, err := getAndValidateParameters(configMap)
	if err != nil {
		return err
	}

	traverser := NewDataObjectTraverser(nil, func() (accountRepository, error) {
		return a.accountRepoFactory(pltfrm, accountId, &repoCredentials)
	}, func(metastoreWorkspaces []string) (workspaceRepository, string, error) {
		return selectWorkspaceRepo(ctx, &repoCredentials, pltfrm, accountId, metastoreWorkspaces, a.workspaceRepoFactory)
	}, createFullName)

	storedFunctions := types.NewStoredFunctions()

	metaStoreIdMap := map[string]string{}

	err = traverser.Traverse(ctx, func(ctx context.Context, securableType string, parentObject interface{}, object interface{}, metastore *string) error {
		metastoreSync := func(f func(repo dataAccessWorkspaceRepository) error) error {
			host, err2 := pltfrm.WorkspaceAddress(*metastore)
			if err2 != nil {
				return fmt.Errorf("workspace address: %w", err2)
			}

			client, err2 := a.workspaceRepoFactory(pltfrm, host, accountId, &repoCredentials)
			if err2 != nil {
				return err2
			}

			return f(client)
		}

		switch securableType {
		case constants.WorkspaceType:
			return a.syncFromTargetWorkspace(ctx, pltfrm, accessProviderHandler, accountId, &repoCredentials, object)
		case constants.MetastoreType:
			if ms, ok := object.(*catalog.MetastoreInfo); ok {
				metaStoreIdMap[ms.MetastoreId] = ms.Name
			}

			return metastoreSync(func(repo dataAccessWorkspaceRepository) error {
				return a.syncFromTargetMetastore(ctx, accessProviderHandler, repo, object)
			})
		case constants.CatalogType:
			return metastoreSync(func(repo dataAccessWorkspaceRepository) error {
				return a.syncFromTargetCatalog(ctx, accessProviderHandler, repo, object, metaStoreIdMap)
			})
		case data_source.Schema:
			return metastoreSync(func(repo dataAccessWorkspaceRepository) error {
				return a.syncFromTargetSchema(ctx, accessProviderHandler, repo, object, metaStoreIdMap)
			})
		case data_source.Table:
			return metastoreSync(func(repo dataAccessWorkspaceRepository) error {
				return a.syncFromTargetTable(ctx, &storedFunctions, accessProviderHandler, repo, object, metaStoreIdMap)
			})
		case data_source.Column:
			return a.syncFromTargetColumn(ctx, &storedFunctions, parentObject, object)
		case constants.FunctionType:
			return metastoreSync(func(repo dataAccessWorkspaceRepository) error {
				return a.syncFromTargetFunction(ctx, accessProviderHandler, repo, &storedFunctions, object, metaStoreIdMap)
			})
		}

		return fmt.Errorf("unsupported type %s", securableType)
	}, func(traverserOptions *DataObjectTraverserOptions) {
		traverserOptions.SecurableTypesToReturn = set.NewSet[string](constants.WorkspaceType, constants.MetastoreType, constants.CatalogType, data_source.Schema, data_source.Table, data_source.Column, constants.FunctionType)
	})

	if err != nil {
		return err
	}

	return nil
}

func (a *AccessSyncer) syncFromTargetWorkspace(ctx context.Context, pltfrm platform.DatabricksPlatform, accessProviderHandler wrappers.AccessProviderHandler, accountId string, repoCredentials *repo.RepositoryCredentials, object interface{}) error {
	workspace, ok := object.(*repo.Workspace)
	if !ok {
		return fmt.Errorf("unable to parse Workspace. Expected *catalog.WorkspaceInfo but got %T", object)
	}

	accountClient, err := a.accountRepoFactory(pltfrm, accountId, repoCredentials)
	if err != nil {
		return err
	}

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
			principalId = assignment.Principal.ServicePrincipalName
		} else {
			logger.Error(fmt.Sprintf("Unknown principal assignment type %+v", assignment.Principal))

			continue
		}

		do := data_source.DataObjectReference{FullName: strconv.Itoa(workspace.WorkspaceId), Type: constants.WorkspaceType}

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
						DataObject:  &data_source.DataObjectReference{FullName: strconv.Itoa(workspace.WorkspaceId), Type: constants.WorkspaceType},
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

	err = a.addPermissionIfNotSetByRaito(accessProviderHandler, metastore.Name, &data_source.DataObjectReference{FullName: metastore.Name, Type: constants.MetastoreType}, permissionsList)
	if err != nil {
		return err
	}

	return nil
}

func (a *AccessSyncer) syncFromTargetCatalog(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, workspaceClient dataAccessWorkspaceRepository, object interface{}, metastoreIdMap map[string]string) error {
	c, ok := object.(*catalog.CatalogInfo)
	if !ok {
		return fmt.Errorf("unable to parse Catalog. Expected *catalog.CatalogInfo but got %T", object)
	}

	metastoreName, ok := metastoreIdMap[c.MetastoreId]
	if !ok {
		logger.Warn(fmt.Sprintf("Unable to find metastore name for metastore id %q", c.MetastoreId))
		metastoreName = c.MetastoreId
	}

	return a.syncAccessDataObjectFromTarget(ctx, accessProviderHandler, workspaceClient, metastoreName, c.MetastoreId, c.FullName, constants.CatalogType, catalog.SecurableTypeCatalog)
}

func (a *AccessSyncer) syncFromTargetSchema(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, workspaceClient dataAccessWorkspaceRepository, object interface{}, metastoreIdMap map[string]string) error {
	schema, ok := object.(*catalog.SchemaInfo)
	if !ok {
		return fmt.Errorf("unable to parse Schema. Expected *catalog.SchemaInfo but got %T", object)
	}

	metastoreName, ok := metastoreIdMap[schema.MetastoreId]
	if !ok {
		logger.Warn(fmt.Sprintf("Unable to find metastore name for metastore id %q", schema.MetastoreId))
		metastoreName = schema.MetastoreId
	}

	return a.syncAccessDataObjectFromTarget(ctx, accessProviderHandler, workspaceClient, metastoreName, schema.MetastoreId, schema.FullName, data_source.Schema, catalog.SecurableTypeSchema)
}

func (a *AccessSyncer) syncFromTargetTable(ctx context.Context, storedFunctions *types.StoredFunctions, accessProviderHandler wrappers.AccessProviderHandler, workspaceClient dataAccessWorkspaceRepository, object interface{}, metastoreIdMap map[string]string) error {
	table, ok := object.(*catalog.TableInfo)
	if !ok {
		return fmt.Errorf("unable to parse Table. Expected *catalog.TableInfo but got %T", object)
	}

	metastoreName, ok := metastoreIdMap[table.MetastoreId]
	if !ok {
		logger.Warn(fmt.Sprintf("Unable to find metastore name for metastore id %q", table.MetastoreId))
		metastoreName = table.MetastoreId
	}

	if table.RowFilter != nil {
		functionId := createUniqueId(table.MetastoreId, table.RowFilter.FunctionName)
		storedFunctions.AddFilter(functionId, createUniqueId(table.MetastoreId, table.FullName))
	}

	return a.syncAccessDataObjectFromTarget(ctx, accessProviderHandler, workspaceClient, metastoreName, table.MetastoreId, table.FullName, data_source.Table, catalog.SecurableTypeTable)
}

func (a *AccessSyncer) syncFromTargetColumn(_ context.Context, storedFunctions *types.StoredFunctions, parent interface{}, object interface{}) error {
	column, ok := object.(*catalog.ColumnInfo)
	if !ok {
		return fmt.Errorf("unable to parse Column. Expected *catalog.ColumnInfo but got %T", object)
	}

	table, ok := parent.(*catalog.TableInfo)
	if !ok {
		return fmt.Errorf("unable to parse Table. Expected *catalog.TableInfo but got %T", parent)
	}

	if column.Mask != nil {
		functionId := createUniqueId(table.MetastoreId, column.Mask.FunctionName)
		storedFunctions.AddMask(functionId, createTableUniqueId(table.MetastoreId, table.FullName, column.Name))
	}

	return nil
}

func (a *AccessSyncer) syncFromTargetFunction(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, workspaceClient dataAccessWorkspaceRepository, storedFunction *types.StoredFunctions, object interface{}, metastoreIdMap map[string]string) error {
	function, ok := object.(*repo.FunctionInfo)
	if !ok {
		return fmt.Errorf("unable to parse Function. Expected *catalog.FunctionInfo but got %T", object)
	}

	if strings.HasPrefix(function.Name, raitoPrefix) {
		// NO need to import functions create by Raito
		return nil
	}

	functionId := createUniqueId(function.MetastoreId, function.FullName)
	if columns, found := storedFunction.Masks[functionId]; found {
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
	} else if tables, found := storedFunction.Filters[functionId]; found {
		// Currently this is not called due to a bug in by Databricks as they dont return correctly the row filter function name
		for _, table := range tables {
			err := accessProviderHandler.AddAccessProviders(&sync_from_target.AccessProvider{
				ExternalId: functionId,
				Name:       function.Name,
				ActualName: functionId,
				Policy:     function.RoutineDefinition,
				Action:     sync_from_target.Filtered,
				What: []sync_from_target.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{FullName: table, Type: data_source.Table},
					},
				},
				NotInternalizable: true,
			})

			if err != nil {
				return err
			}
		}
	} else {
		metastoreName, ok := metastoreIdMap[function.MetastoreId]
		if !ok {
			logger.Warn(fmt.Sprintf("Unable to find metastore name for metastore id %q", function.MetastoreId))
			metastoreName = function.MetastoreId
		}

		return a.syncAccessDataObjectFromTarget(ctx, accessProviderHandler, workspaceClient, metastoreName, function.MetastoreId, function.FullName, constants.FunctionType, catalog.SecurableTypeFunction)
	}

	return nil
}

func (a *AccessSyncer) syncAccessDataObjectFromTarget(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, workspaceClient dataAccessWorkspaceRepository, metastoreName, metastoreId, fullName string, doType string, securableType catalog.SecurableType) error {
	permissionsList, err := workspaceClient.GetPermissionsOnResource(ctx, securableType, fullName)
	if err != nil {
		return err
	}

	return a.addPermissionIfNotSetByRaito(accessProviderHandler, createUniqueId(metastoreName, fullName), &data_source.DataObjectReference{FullName: createUniqueId(metastoreId, fullName), Type: doType}, permissionsList)
}

func (a *AccessSyncer) SyncAccessProviderToTarget(ctx context.Context, accessProviders *sync_to_target.AccessProviderImport, accessProviderFeedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) (err error) {
	defer func() {
		if err != nil {
			logger.Error(fmt.Sprintf("SyncAccessProviderToTarget failed: %s", err.Error()))
		}
	}()

	pltfrm, accountId, repoCredentials, err := getAndValidateParameters(configMap)
	if err != nil {
		return err
	}

	accountRepo, err := a.accountRepoFactory(pltfrm, accountId, &repoCredentials)
	if err != nil {
		return fmt.Errorf("account repo: %w", err)
	}

	_, _, metastoreWorkspaceMap, err := a.loadMetastores(ctx, configMap)
	metastoreClientCache := make(map[string]dataAccessWorkspaceRepository)

	getMetastoreClient := func(metastoreId string) (dataAccessWorkspaceRepository, error) {
		if repo, ok := metastoreClientCache[metastoreId]; ok {
			return repo, nil
		}

		repo, _, werr := selectWorkspaceRepo(ctx, &repoCredentials, pltfrm, accountId, metastoreWorkspaceMap[metastoreId], a.workspaceRepoFactory)
		if werr != nil {
			return nil, werr
		}

		metastoreClientCache[metastoreId] = repo

		return repo, nil
	}

	permissionsChanges := types.NewPrivilegesChangeCollection()
	a.apFeedbackObjects = make(map[string]sync_to_target.AccessProviderSyncFeedback)

	grants := make([]*sync_to_target.AccessProvider, 0, len(accessProviders.AccessProviders))
	masksAps := make([]*sync_to_target.AccessProvider, 0, len(accessProviders.AccessProviders))
	filters := make([]*sync_to_target.AccessProvider, 0, len(accessProviders.AccessProviders))

	for i := range accessProviders.AccessProviders {
		switch accessProviders.AccessProviders[i].Action {
		case sync_to_target.Grant, sync_to_target.Purpose:
			grants = append(grants, accessProviders.AccessProviders[i])
		case sync_to_target.Mask:
			masksAps = append(masksAps, accessProviders.AccessProviders[i])
		case sync_to_target.Filtered:
			filters = append(filters, accessProviders.AccessProviders[i])
		default:
			err2 := accessProviderFeedbackHandler.AddAccessProviderFeedback(sync_to_target.AccessProviderSyncFeedback{
				AccessProvider: accessProviders.AccessProviders[i].Id,
				Errors: []string{
					"Unsupported action: " + accessProviders.AccessProviders[i].Action.String(),
				},
			})
			if err2 != nil {
				return err2
			}
		}
	}

	a.syncFiltersToTarget(ctx, filters, configMap)
	a.syncMasksToTarget(ctx, masksAps, configMap)
	a.syncGrantsToTarget(ctx, grants, &permissionsChanges)

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
		if item.Type == constants.WorkspaceType {
			a.storePrivilegesInComputePlane(ctx, item, principlePrivilegesMap, accountRepo)
		} else {
			a.storePrivilegesInDataplane(ctx, item, getMetastoreClient, principlePrivilegesMap)
		}
	}

	return nil
}

func (a *AccessSyncer) syncGrantsToTarget(ctx context.Context, grants []*sync_to_target.AccessProvider, permissionsChanges *types.PrivilegesChangeCollection) {
	for _, grant := range grants {
		feedbackElement := sync_to_target.AccessProviderSyncFeedback{
			AccessProvider: grant.Id,
		}

		feedbackElement.ActualName = grant.Id
		feedbackElement.Type = ptr.String(access_provider.AclSet)

		apErr := a.syncGrantToTarget(ctx, grant, permissionsChanges)
		if apErr != nil {
			feedbackElement.Errors = append(feedbackElement.Errors, apErr.Error())
		}

		a.apFeedbackObjects[grant.Id] = feedbackElement
	}
}

func (a *AccessSyncer) syncMasksToTarget(ctx context.Context, maskAps []*sync_to_target.AccessProvider, configMap *config.ConfigMap) {
	for _, mask := range maskAps {
		feedbackElement := sync_to_target.AccessProviderSyncFeedback{
			AccessProvider: mask.Id,
		}

		maskName, apErr := a.syncMaskToTarget(ctx, mask, configMap)

		feedbackElement.ExternalId = &maskName
		feedbackElement.ActualName = maskName

		if apErr != nil {
			feedbackElement.Errors = append(feedbackElement.Errors, apErr.Error())
		}

		a.apFeedbackObjects[mask.Id] = feedbackElement
	}
}

func (a *AccessSyncer) syncFiltersToTarget(ctx context.Context, filters []*sync_to_target.AccessProvider, configMap *config.ConfigMap) {
	filtersByDo := make(map[string][]*sync_to_target.AccessProvider)

	for i, filter := range filters {
		feedbackElement := sync_to_target.AccessProviderSyncFeedback{
			AccessProvider: filter.Id,
		}

		if len(filter.What) != 1 || filter.What[0].DataObject.Type != data_source.Table {
			feedbackElement.Errors = append(feedbackElement.Errors, "Unsupported what item(s)")
			a.apFeedbackObjects[filter.Id] = feedbackElement

			continue
		}

		do := filter.What[0].DataObject.FullName
		filtersByDo[do] = append(filtersByDo[do], filters[i])
	}

	for do, filterAps := range filtersByDo {
		actualName, externalId, err := a.syncFilterToTarget(ctx, do, filterAps, configMap)

		for _, filter := range filterAps {
			feedbackElement := sync_to_target.AccessProviderSyncFeedback{
				AccessProvider: filter.Id,
				ExternalId:     &externalId,
				ActualName:     actualName,
			}

			if err != nil {
				feedbackElement.Errors = append(feedbackElement.Errors, err.Error())
			}

			a.apFeedbackObjects[filter.Id] = feedbackElement
		}
	}
}

func (a *AccessSyncer) syncFilterToTarget(ctx context.Context, do string, aps []*sync_to_target.AccessProvider, configMap *config.ConfigMap) (filterName string, externalId string, _ error) {
	schemaNameSplit := strings.Split(do, ".")
	metastore := schemaNameSplit[0]
	catalogName := schemaNameSplit[1]
	schemaName := schemaNameSplit[2]
	tableName := schemaNameSplit[3]

	filterName = raitoPrefixName(tableName + "_filter")
	externalId = do + ".filter"

	warehouseIdMap := make(map[string]types.WarehouseDetails)

	if found, err := configMap.Unmarshal(constants.DatabricksSqlWarehouses, &warehouseIdMap); err != nil {
		return filterName, externalId, err
	} else if !found {
		return filterName, externalId, fmt.Errorf("no warehouses found in configmap")
	}

	warehouseId, ok := warehouseIdMap[metastore]
	if !ok {
		return filterName, externalId, fmt.Errorf("no warehouse found for metastore %q", metastore)
	}

	pltfrm, accountId, repoCredentials, err := getAndValidateParameters(configMap)
	if err != nil {
		return filterName, externalId, err
	}

	worspaceAddress, err := pltfrm.WorkspaceAddress(warehouseId.Workspace)
	if err != nil {
		return filterName, externalId, fmt.Errorf("workspace address: %w", err)
	}

	repository, err := a.workspaceRepoFactory(pltfrm, worspaceAddress, accountId, &repoCredentials)
	if err != nil {
		return filterName, externalId, err
	}

	sqlClient := repository.SqlWarehouseRepository(warehouseId.Warehouse)

	filterExpressionParts, filterArguments, deletedAps, err := a.parseFilterAccessProvidersForDo(ctx, aps)
	if err != nil {
		return filterName, externalId, err
	}

	if deletedAps == len(aps) {
		// All Filters are deleted for this DO
		err = a.deleteRowFilter(ctx, sqlClient, catalogName, schemaName, tableName, filterName)
		if err != nil {
			return filterName, externalId, err
		}

		return filterName, externalId, nil
	}

	err = a.createOrUpdateRowFilter(ctx, repository, sqlClient, catalogName, schemaName, tableName, filterName, filterArguments, filterExpressionParts)
	if err != nil {
		return filterName, externalId, err
	}

	return filterName, externalId, nil
}

func (a *AccessSyncer) parseFilterAccessProvidersForDo(ctx context.Context, aps []*sync_to_target.AccessProvider) ([]string, set.Set[string], int, error) {
	filterExpressionParts := make([]string, 0, len(aps))
	filterArguments := set.NewSet[string]()

	deletedAps := 0

	for _, ap := range aps {
		if ap.Delete {
			deletedAps++

			continue
		}

		whoPart, hasWho := filterWhoExpression(ap)

		if !hasWho {
			continue
		}

		var queryPart string

		if ap.PolicyRule != nil {
			var arguments []string

			queryPart, arguments = parsePolicyRuleAsFilterCriteria(*ap.PolicyRule)
			filterArguments.Add(arguments...)
		} else if ap.FilterCriteria != nil {
			var arguments set.Set[string]
			var err error

			queryPart, arguments, err = parseFilterCriteria(ctx, ap.FilterCriteria)
			if err != nil {
				return nil, nil, 0, fmt.Errorf("parse filter criteria: %w", err)
			}

			filterArguments.AddSet(arguments)
		}

		filterExpressionParts = append(filterExpressionParts, fmt.Sprintf("((%s) AND (%s))", whoPart, queryPart))
	}

	return filterExpressionParts, filterArguments, deletedAps, nil
}

func (a *AccessSyncer) createOrUpdateRowFilter(ctx context.Context, repository dataAccessWorkspaceRepository, sqlClient repo.WarehouseRepository, catalogName string, schemaName string, tableName string, filterName string, filterArguments set.Set[string], filterExpressionParts []string) error {
	tableOwner, err := repository.GetOwner(ctx, catalog.SecurableTypeTable, fmt.Sprintf("%s.%s.%s", catalogName, schemaName, tableName))
	if err != nil {
		return fmt.Errorf("get table owner: %w", err)
	}

	columnInformation, err := sqlClient.GetTableInformation(ctx, catalogName, schemaName, tableName)
	if err != nil {
		return fmt.Errorf("get table information: %w", err)
	}

	argumentsWithType := make([]string, 0, len(filterArguments))
	arguments := filterArguments.Slice()

	for _, argument := range arguments {
		columnInfo, found := columnInformation[argument]
		if !found || columnInfo == nil {
			return fmt.Errorf("column %q not found in table %q", argument, tableName)
		}

		argumentsWithType = append(argumentsWithType, fmt.Sprintf("%s %s", argument, columnInfo.Type))
	}

	var functionBody string

	if len(filterExpressionParts) > 0 {
		functionBody = strings.Join(filterExpressionParts, " OR ")
	} else {
		functionBody = "FALSE"
	}

	query := fmt.Sprintf("CREATE OR REPLACE FUNCTION %s(%s)\n RETURN %s;", filterName, strings.Join(argumentsWithType, ", "), functionBody)

	_, err = sqlClient.ExecuteStatement(ctx, catalogName, schemaName, query)
	if err != nil {
		return fmt.Errorf("create or replace function: %w", err)
	}

	err = repository.SetPermissionsOnResource(ctx, catalog.SecurableTypeFunction, fmt.Sprintf("%s.%s.%s", catalogName, schemaName, filterName), catalog.PermissionsChange{
		Principal: tableOwner,
		Add: []catalog.Privilege{
			catalog.PrivilegeExecute,
		},
	})
	if err != nil {
		return fmt.Errorf("grant table owner permission on row filter function: %w", err)
	}

	err = sqlClient.SetRowFilter(ctx, catalogName, schemaName, tableName, filterName, arguments)
	if err != nil {
		return fmt.Errorf("set row filter: %w", err)
	}

	return nil
}

func (a *AccessSyncer) deleteRowFilter(ctx context.Context, sqlClient repo.WarehouseRepository, catalogName string, schemaName string, tableName string, filterName string) error {
	err := sqlClient.DropRowFilter(ctx, catalogName, schemaName, tableName)
	if err != nil {
		return fmt.Errorf("drop row filter: %w", err)
	}

	err = sqlClient.DropFunction(ctx, catalogName, schemaName, filterName)
	if err != nil {
		return fmt.Errorf("drop function: %w", err)
	}

	return nil
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
	for group := range groups {
		switch v := group.(type) {
		case error:
			return nil, fmt.Errorf("list group item: %w", v)
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

	if item.Type == constants.MetastoreType {
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
		err = fmt.Errorf("set permissions on %s %q: %w", securableType.String(), fullname, err)
		return
	}
}

func (a *AccessSyncer) syncMaskToTarget(ctx context.Context, ap *sync_to_target.AccessProvider, configMap *config.ConfigMap) (maskName string, _ error) {
	// 0. Prepare mask update
	if ap.ExternalId != nil {
		maskName = *ap.ExternalId
	} else {
		maskName = raitoPrefixName(ap.NamingHint)
	}

	logger.Debug(fmt.Sprintf("Syncing mask %q to target", maskName))

	schemas := a.syncMasksGetSchema(ap)

	warehouseIdMap := make(map[string]types.WarehouseDetails)

	if found, err := configMap.Unmarshal(constants.DatabricksSqlWarehouses, &warehouseIdMap); err != nil {
		return maskName, err
	} else if !found {
		return maskName, fmt.Errorf("no warehouses found in configmap")
	}

	pltfrm, accountId, repoCredentials, err := getAndValidateParameters(configMap)
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
		err = a.syncMaskInSchema(ctx, pltfrm, ap, schema, warehouseIdMap, maskName, accountId, repoCredentials, dos, beneficiaries)
		if err != nil {
			return maskName, err
		}
	}

	return maskName, nil
}

func (a *AccessSyncer) syncMaskInSchema(ctx context.Context, pltfrm platform.DatabricksPlatform, ap *sync_to_target.AccessProvider, schema string, warehouseIdMap map[string]types.WarehouseDetails, maskName string, accountId string, repoCredentials repo.RepositoryCredentials, dos types.MaskDataObjectsOfSchema, beneficiaries *masks.MaskingBeneficiaries) error {
	schemaNameSplit := strings.Split(schema, ".")
	metastore := schemaNameSplit[0]
	catalogName := schemaNameSplit[1]
	schemaName := schemaNameSplit[2]

	warehouseId, ok := warehouseIdMap[metastore]
	if !ok {
		return fmt.Errorf("no warehouse found for metastore %q", metastore)
	}

	workspaceAddress, err := pltfrm.WorkspaceAddress(warehouseId.Workspace)
	if err != nil {
		return fmt.Errorf("workspace address: %w", err)
	}

	repository, err := a.workspaceRepoFactory(pltfrm, workspaceAddress, accountId, &repoCredentials)
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

func (a *AccessSyncer) addPermissionIfNotSetByRaito(accessProviderHandler wrappers.AccessProviderHandler, apNamePrefix string, do *data_source.DataObjectReference, assignments *catalog.PermissionsList) error {
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
		externalId := fmt.Sprintf("%s_%s", do.FullName, privilege.String())
		apName := fmt.Sprintf("%s_%s", apNamePrefix, privilege.String())

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
				ExternalId: externalId,
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
	pltfrm, accountId, repoCredentials, err := getAndValidateParameters(configMap)
	if err != nil {
		return nil, nil, nil, err
	}

	accountClient, err := a.accountRepoFactory(pltfrm, accountId, &repoCredentials)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("account repository factory: %w", err)
	}

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
	case constants.MetastoreType:
		return catalog.SecurableTypeMetastore, nil
	case constants.CatalogType:
		return catalog.SecurableTypeCatalog, nil
	case data_source.Schema:
		return catalog.SecurableTypeSchema, nil
	case data_source.Table:
		return catalog.SecurableTypeTable, nil
	case constants.FunctionType:
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
	case constants.MetastoreType, constants.WorkspaceType:
		return nil
	case constants.CatalogType:
		addToSetInMap(result, object, string(catalog.PrivilegeUseCatalog))

		fullname, err := cutLastPartFullName(object.FullName)
		if err != nil {
			return err
		}

		return addUsageToUpperDataObjects(result, data_source.DataObjectReference{FullName: fullname, Type: constants.MetastoreType})
	case data_source.Schema:
		addToSetInMap(result, object, string(catalog.PrivilegeUseSchema))

		fullname, err := cutLastPartFullName(object.FullName)
		if err != nil {
			return err
		}

		return addUsageToUpperDataObjects(result, data_source.DataObjectReference{FullName: fullname, Type: constants.CatalogType})
	case data_source.Table, data_source.View, constants.FunctionType:
		fullname, err := cutLastPartFullName(object.FullName)
		if err != nil {
			return err
		}

		return addUsageToUpperDataObjects(result, data_source.DataObjectReference{FullName: fullname, Type: data_source.Schema})
	default:
		return fmt.Errorf("unknown type %q", object.Type)
	}
}

func parseFilterCriteria(ctx context.Context, filterCriteria *bexpression.DataComparisonExpression) (string, set.Set[string], error) {
	filterParser := NewFilterCriteriaBuilder()

	err := filterCriteria.Accept(ctx, filterParser)
	if err != nil {
		return "", nil, err
	}

	query, args := filterParser.GetQueryAndArguments()

	return query, args, nil
}

func parsePolicyRuleAsFilterCriteria(policyRule string) (string, []string) {
	argumentRegexp := regexp.MustCompile(`\{([a-zA-Z0-9]+)\}`)

	argumentsSubMatches := argumentRegexp.FindAllStringSubmatch(policyRule, -1)
	query := argumentRegexp.ReplaceAllString(policyRule, "$1")

	arguments := make([]string, 0, len(argumentsSubMatches))
	for _, match := range argumentsSubMatches {
		arguments = append(arguments, match[1])
	}

	return query, arguments
}

func filterWhoExpression(ap *sync_to_target.AccessProvider) (string, bool) {
	whoExpressionParts := make([]string, 0, 1+len(ap.Who.Groups))

	{
		users := make([]string, 0, len(ap.Who.Users))

		for _, user := range ap.Who.Users {
			users = append(users, fmt.Sprintf("'%s'", user))
		}

		if len(users) > 0 {
			whoExpressionParts = append(whoExpressionParts, fmt.Sprintf("current_user() IN (%s)", strings.Join(users, ", ")))
		}
	}

	for _, group := range ap.Who.Groups {
		whoExpressionParts = append(whoExpressionParts, fmt.Sprintf("is_account_group_member('%s')", group))
	}

	if len(whoExpressionParts) == 0 {
		return "FALSE", false
	}

	return strings.Join(whoExpressionParts, " OR "), true
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

func raitoPrefixName(name string) string {
	return strings.ToLower(fmt.Sprintf("%s%s", raitoPrefix, strings.ReplaceAll(strings.ToUpper(name), " ", "_")))
}
