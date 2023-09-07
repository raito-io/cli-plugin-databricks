package databricks

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/smithy-go/ptr"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/raito-io/cli/base/access_provider"
	"github.com/raito-io/cli/base/access_provider/sync_from_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"

	"cli-plugin-databricks/utils/array"
)

var _ wrappers.AccessProviderSyncer = (*AccessSyncer)(nil)

//go:generate go run github.com/vektra/mockery/v2 --name=dataAccessAccountRepository
type dataAccessAccountRepository interface {
	ListMetastores(ctx context.Context) ([]catalog.MetastoreInfo, error)
	ListUsers(ctx context.Context, optFn ...func(options *databricksUsersFilter)) <-chan interface{}
	ListGroups(ctx context.Context, optFn ...func(options *databricksGroupsFilter)) <-chan interface{}
	GetWorkspaces(ctx context.Context) ([]Workspace, error)
	GetWorkspaceMap(ctx context.Context, metastores []catalog.MetastoreInfo, workspaces []Workspace) (map[string][]string, map[string]string, error)
	ListWorkspaceAssignments(ctx context.Context, workspaceId int) ([]iam.PermissionAssignment, error)
	UpdateWorkspaceAssignment(ctx context.Context, workspaceId int, principalId int64, permission []iam.WorkspacePermission) error
}

//go:generate go run github.com/vektra/mockery/v2 --name=dataAccessWorkspaceRepository
type dataAccessWorkspaceRepository interface {
	Ping(ctx context.Context) error
	ListCatalogs(ctx context.Context) ([]catalog.CatalogInfo, error)
	ListSchemas(ctx context.Context, catalogName string) ([]catalog.SchemaInfo, error)
	ListTables(ctx context.Context, catalogName string, schemaName string) ([]catalog.TableInfo, error)
	ListFunctions(ctx context.Context, catalogName string, schemaName string) ([]catalog.FunctionInfo, error)
	GetPermissionsOnResource(ctx context.Context, securableType catalog.SecurableType, fullName string) (*catalog.PermissionsList, error)
	SetPermissionsOnResource(ctx context.Context, securableType catalog.SecurableType, fullName string, changes ...catalog.PermissionsChange) error
}

type AccessSyncer struct {
	accountRepoFactory   func(accountId string, repoCredentials RepositoryCredentials) dataAccessAccountRepository
	workspaceRepoFactory func(host string, repoCredentials RepositoryCredentials) (dataAccessWorkspaceRepository, error)

	privilegeCache PrivilegeCache
}

func NewAccessSyncer() *AccessSyncer {
	return &AccessSyncer{
		accountRepoFactory: func(accountId string, repoCredentials RepositoryCredentials) dataAccessAccountRepository {
			return NewAccountRepository(repoCredentials, accountId)
		},
		workspaceRepoFactory: func(host string, repoCredentials RepositoryCredentials) (dataAccessWorkspaceRepository, error) {
			return NewWorkspaceRepository(host, repoCredentials)
		},

		privilegeCache: NewPrivilegeCache(),
	}
}

func (a *AccessSyncer) SyncAccessProvidersFromTarget(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, configMap *config.ConfigMap) (err error) {
	defer func() {
		if err != nil {
			logger.Error(fmt.Sprintf("SyncAccessProvidersFromTarget failed: %s", err.Error()))
		}
	}()

	metastores, worspaces, metastoreWorkspaceMap, err := a.loadMetastores(ctx, configMap)
	if err != nil {
		return err
	}

	for i := range worspaces {
		err = a.syncWorkspaceFromTarget(ctx, &worspaces[i], accessProviderHandler, configMap)
		if err != nil {
			return err
		}
	}

	for i := range metastores {
		if metastoreWorkspaces, ok := metastoreWorkspaceMap[metastores[i].MetastoreId]; ok {
			err = a.syncAccessProviderFromMetastore(ctx, accessProviderHandler, configMap, &metastores[i], metastoreWorkspaces)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *AccessSyncer) syncWorkspaceFromTarget(ctx context.Context, workspace *Workspace, accessProviderHandler wrappers.AccessProviderHandler, configMap *config.ConfigMap) error {
	logger.Debug(fmt.Sprintf("Sync workspace %s from target", workspace.WorkspaceName))

	accountId, repoCredentials, err := getAndValidateParameters(configMap)
	if err != nil {
		return err
	}

	accountClient := a.accountRepoFactory(accountId, repoCredentials)

	privilegesToSync := make(map[string][]string)

	assignments, err := accountClient.ListWorkspaceAssignments(ctx, workspace.WorkspaceId)
	if err != nil {
		return err
	}

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

	accountRepo := a.accountRepoFactory(accountId, repoCredentials)

	_, _, metastoreWorkspaceMap, err := a.loadMetastores(ctx, configMap)
	metastoreClientCache := make(map[string]dataAccessWorkspaceRepository)

	getMetastoreClient := func(metastoreId string) (dataAccessWorkspaceRepository, error) {
		if repo, ok := metastoreClientCache[metastoreId]; ok {
			return repo, nil
		}

		repo, werr := selectWorkspaceRepo(ctx, repoCredentials, metastoreWorkspaceMap[metastoreId], a.workspaceRepoFactory)
		if werr != nil {
			return nil, werr
		}

		metastoreClientCache[metastoreId] = *repo

		return *repo, nil
	}

	permissionsChanges := NewPrivilegesChangeCollection()

	for i := range accessProviders.AccessProviders {
		apErr := a.syncAccessProviderToTarget(ctx, accessProviders.AccessProviders[i], &permissionsChanges, accessProviderFeedbackHandler)
		if apErr != nil {
			return apErr
		}
	}

	for item, principlePrivilegesMap := range permissionsChanges.m {
		if item.Type == workspaceType {
			err2 := a.storePrivilegesInComputePlane(ctx, item, principlePrivilegesMap, accountRepo)
			if err2 != nil {
				return err2
			}
		} else {
			err2 := a.storePrivilegesInDataplane(ctx, item, getMetastoreClient, principlePrivilegesMap)
			if err2 != nil {
				return err2
			}
		}
	}

	return nil
}

func (a *AccessSyncer) storePrivilegesInComputePlane(ctx context.Context, item SecurableItemKey, principlePrivilegesMap map[string]*PrivilegesChanges, repo dataAccessAccountRepository) error {
	workspaceId, err := strconv.Atoi(item.FullName)
	if err != nil {
		return err
	}

	for principal, privilegesChanges := range principlePrivilegesMap {
		var principalId int64

		if strings.Contains(principal, "@") {
			user, err2 := a.getUserFromEmail(ctx, principal, repo)
			if err2 != nil {
				return err2
			}

			principalId, err2 = strconv.ParseInt(user.Id, 10, 64)
			if err2 != nil {
				return err2
			}
		} else {
			group, err2 := a.getGroupIdFromName(ctx, principal, repo)
			if err2 != nil {
				return err2
			}

			principalId, err2 = strconv.ParseInt(group.Id, 10, 64)
			if err2 != nil {
				return err2
			}
		}

		err2 := repo.UpdateWorkspaceAssignment(ctx, workspaceId, principalId, workspacePermissionsToDatabricksPermissions(privilegesChanges.Add.Slice()))
		if err2 != nil {
			return err2
		}
	}

	return nil
}

func (a *AccessSyncer) getUserFromEmail(ctx context.Context, email string, repo dataAccessAccountRepository) (*iam.User, error) {
	cancelCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	users := repo.ListUsers(cancelCtx, func(options *databricksUsersFilter) { options.username = &email })
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

func (a *AccessSyncer) getGroupIdFromName(ctx context.Context, groupname string, repo dataAccessAccountRepository) (*iam.Group, error) {
	cancelCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	groups := repo.ListGroups(cancelCtx, func(options *databricksGroupsFilter) { options.groupname = &groupname })
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

func (a *AccessSyncer) storePrivilegesInDataplane(ctx context.Context, item SecurableItemKey, getMetastoreClient func(metastoreId string) (dataAccessWorkspaceRepository, error), principlePrivilegesMap map[string]*PrivilegesChanges) error {
	metastore, fullname := getMetastoreAndFullnameOfUniqueId(item.FullName)

	repo, err := getMetastoreClient(metastore)
	if err != nil {
		return err
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
		return err
	}

	err = repo.SetPermissionsOnResource(ctx, securableType, fullname, changes...)
	if err != nil {
		return err
	}

	return nil
}

func (a *AccessSyncer) syncAccessProviderToTarget(_ context.Context, ap *sync_to_target.AccessProvider, changeCollection *PrivilegesChangeCollection, accessProviderFeedbackHandler wrappers.AccessProviderFeedbackHandler) error {
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
			itemKey := SecurableItemKey{
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
			itemKey := SecurableItemKey{
				Type:     do.Type,
				FullName: do.FullName,
			}

			privilegesSlice := privileges.Slice()

			if !ap.Delete {
				for _, principal := range principals {
					changeCollection.AddPrivilege(itemKey, principal, privilegesSlice...)

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
			itemKey := SecurableItemKey{
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

	return accessProviderFeedbackHandler.AddAccessProviderFeedback(ap.Id, sync_to_target.AccessSyncFeedbackInformation{
		AccessId:   ap.Id,
		ActualName: ap.Id,
	})
}

func (a *AccessSyncer) syncAccessProviderFromMetastore(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, configMap *config.ConfigMap, metastore *catalog.MetastoreInfo, workspaceDeploymentNames []string) error {
	_, repoCredentials, err := getAndValidateParameters(configMap)
	if err != nil {
		return err
	}

	logger.Debug(fmt.Sprintf("Get data access objects from metastore %s", metastore.Name))
	logger.Debug(fmt.Sprintf("Will try %d workspaces. %+v", len(workspaceDeploymentNames), workspaceDeploymentNames))

	// Select workspace
	var workspaceRepo dataAccessWorkspaceRepository

	for _, workspaceName := range workspaceDeploymentNames {
		repo, werr := a.workspaceRepoFactory(GetWorkspaceAddress(workspaceName), repoCredentials)
		if werr != nil {
			err = werr
			continue
		}

		werr = repo.Ping(ctx)
		if werr != nil {
			err = werr
			continue
		}

		workspaceRepo = repo

		logger.Debug(fmt.Sprintf("Will use workspace %q for metastore %q", workspaceName, metastore.Name))

		break
	}

	if workspaceRepo == nil {
		return fmt.Errorf("no workspace found for metastore %s: %w", metastore.Name, err)
	}

	logger.Debug(fmt.Sprintf("Load permissions on metastore %q", metastore.MetastoreId))

	permissionsList, err := workspaceRepo.GetPermissionsOnResource(ctx, catalog.SecurableTypeMetastore, metastore.MetastoreId)
	if err != nil {
		return err
	}

	logger.Debug(fmt.Sprintf("Process permission on metastore %q", metastore.Name))

	err = a.addPermissionIfNotSetByRaito(accessProviderHandler, &data_source.DataObjectReference{FullName: metastore.Name, Type: metastoreType}, permissionsList)
	if err != nil {
		return err
	}

	catalogs, err := workspaceRepo.ListCatalogs(ctx)
	if err != nil {
		return err
	}

	for i := range catalogs {
		catalogInfo := &catalogs[i]

		err = a.syncAccessProviderFromCatalog(ctx, accessProviderHandler, &catalogs[i], workspaceRepo)
		if err != nil {
			return err
		}

		schemas, schemaErr := workspaceRepo.ListSchemas(ctx, catalogInfo.Name)
		if schemaErr != nil {
			return err
		}

		for j := range schemas {
			err = a.syncAccessProviderFromSchema(ctx, accessProviderHandler, &schemas[j], workspaceRepo)
			if err != nil {
				return err
			}

			tables, tableErr := workspaceRepo.ListTables(ctx, catalogInfo.Name, schemas[j].Name)
			if tableErr != nil {
				return err
			}

			for k := range tables {
				err = a.syncAccessProviderFromTable(ctx, accessProviderHandler, &tables[k], workspaceRepo)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (a *AccessSyncer) syncAccessProviderFromCatalog(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, catalogInfo *catalog.CatalogInfo, repo dataAccessWorkspaceRepository) error {
	logger.Debug(fmt.Sprintf("Get data access objects from catalog %q", catalogInfo.Name))

	permissionsList, err := repo.GetPermissionsOnResource(ctx, catalog.SecurableTypeCatalog, catalogInfo.Name)
	if err != nil {
		return err
	}

	return a.addPermissionIfNotSetByRaito(accessProviderHandler, &data_source.DataObjectReference{FullName: createUniqueId(catalogInfo.MetastoreId, catalogInfo.Name), Type: catalogType}, permissionsList)
}

func (a *AccessSyncer) syncAccessProviderFromSchema(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, schemaInfo *catalog.SchemaInfo, repo dataAccessWorkspaceRepository) error {
	logger.Debug(fmt.Sprintf("Get data access objects from schema %q", schemaInfo.Name))

	permissionsList, err := repo.GetPermissionsOnResource(ctx, catalog.SecurableTypeSchema, schemaInfo.FullName)
	if err != nil {
		return err
	}

	return a.addPermissionIfNotSetByRaito(accessProviderHandler, &data_source.DataObjectReference{FullName: createUniqueId(schemaInfo.MetastoreId, schemaInfo.FullName), Type: data_source.Schema}, permissionsList)
}

func (a *AccessSyncer) syncAccessProviderFromTable(ctx context.Context, accessProviderHandler wrappers.AccessProviderHandler, tableInfo *catalog.TableInfo, repo dataAccessWorkspaceRepository) error {
	logger.Debug(fmt.Sprintf("Get data access objects from table %q", tableInfo.Name))

	doType, err := tableTypeToRaitoType(tableInfo.TableType)
	if err != nil {
		logger.Warn(err.Error())
		return nil
	}

	permissionsList, err := repo.GetPermissionsOnResource(ctx, catalog.SecurableTypeTable, tableInfo.FullName)
	if err != nil {
		return err
	}

	return a.addPermissionIfNotSetByRaito(accessProviderHandler, &data_source.DataObjectReference{FullName: createUniqueId(tableInfo.MetastoreId, tableInfo.FullName), Type: doType}, permissionsList)
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

func (a *AccessSyncer) loadMetastores(ctx context.Context, configMap *config.ConfigMap) ([]catalog.MetastoreInfo, []Workspace, map[string][]string, error) {
	accountId, repoCredentials, err := getAndValidateParameters(configMap)
	if err != nil {
		return nil, nil, nil, err
	}

	accountClient := a.accountRepoFactory(accountId, repoCredentials)

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
	case data_source.Table, data_source.View: //TODO also include functions
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
