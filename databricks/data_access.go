package databricks

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/aws/smithy-go/ptr"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/databricks/databricks-sdk-go/service/provisioning"
	"github.com/hashicorp/go-multierror"
	"github.com/raito-io/bexpression"
	"github.com/raito-io/cli/base/access_provider"
	"github.com/raito-io/cli/base/access_provider/sync_from_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/databricks/masks"
	"cli-plugin-databricks/databricks/platform"
	"cli-plugin-databricks/databricks/repo"
	types2 "cli-plugin-databricks/databricks/repo/types"
	"cli-plugin-databricks/databricks/types"
	"cli-plugin-databricks/databricks/utils"
	"cli-plugin-databricks/utils/array"
)

var _ wrappers.AccessProviderSyncer = (*AccessSyncer)(nil)
var TitleCaser = cases.Title(language.English)

const (
	raitoPrefix = "raito_"
)

//go:generate go run github.com/vektra/mockery/v2 --name=dataAccessAccountRepository
type dataAccessAccountRepository interface {
	ListUsers(ctx context.Context, optFn ...func(options *types2.DatabricksUsersFilter)) <-chan repo.ChannelItem[iam.User]
	ListGroups(ctx context.Context, optFn ...func(options *types2.DatabricksGroupsFilter)) <-chan repo.ChannelItem[iam.Group]
	ListServicePrincipals(ctx context.Context, optFn ...func(options *types2.DatabricksServicePrincipalFilter)) <-chan repo.ChannelItem[iam.ServicePrincipal]
	ListWorkspaceAssignments(ctx context.Context, workspaceId int64) ([]iam.PermissionAssignment, error)
	UpdateWorkspaceAssignment(ctx context.Context, workspaceId int64, principalId int64, permission []iam.WorkspacePermission) error
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
	accountRepoFactory   func(pltfrm platform.DatabricksPlatform, accountId string, repoCredentials *types2.RepositoryCredentials) (dataAccessAccountRepository, error)
	workspaceRepoFactory func(repoCredentials *types2.RepositoryCredentials) (dataAccessWorkspaceRepository, error)

	privilegeCache types.PrivilegeCache

	apFeedbackObjects map[string]sync_to_target.AccessProviderSyncFeedback // Cache apFeedback objects
}

func NewAccessSyncer() *AccessSyncer {
	return &AccessSyncer{
		accountRepoFactory: func(pltfrm platform.DatabricksPlatform, accountId string, repoCredentials *types2.RepositoryCredentials) (dataAccessAccountRepository, error) {
			return repo.NewAccountRepository(pltfrm, repoCredentials, accountId)
		},
		workspaceRepoFactory: func(repoCredentials *types2.RepositoryCredentials) (dataAccessWorkspaceRepository, error) {
			return repo.NewWorkspaceRepository(repoCredentials)
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

	pltfrm, accountId, repoCredentials, err := utils.GetAndValidateParameters(configMap)
	if err != nil {
		return err
	}

	accountRepo, err := a.accountRepoFactory(pltfrm, accountId, &repoCredentials)
	if err != nil {
		return fmt.Errorf("account repository factory: %w", err)
	}

	traverser, err := NewDataObjectTraverser(&data_source.DataSourceSyncConfig{ConfigMap: configMap}, func() (accountRepository, error) {
		return accountRepo, nil
	}, func(metastoreWorkspace *provisioning.Workspace) (workspaceRepository, error) {
		return utils.InitWorkspaceRepo(ctx, repoCredentials, pltfrm, metastoreWorkspace, a.workspaceRepoFactory)
	}, createFullName)

	if err != nil {
		return fmt.Errorf("data object traverser: %w", err)
	}

	groups, err := repo.ChannelToSet(func(ctx context.Context) <-chan repo.ChannelItem[iam.Group] {
		return accountRepo.ListGroups(ctx)
	}, func(group iam.Group) string {
		return group.DisplayName
	})
	if err != nil {
		return fmt.Errorf("list groups: %w", err)
	}

	servicePrincipals, err := repo.ChannelToSet(func(ctx context.Context) <-chan repo.ChannelItem[iam.ServicePrincipal] {
		return accountRepo.ListServicePrincipals(ctx)
	}, func(servicePrincipal iam.ServicePrincipal) string {
		return servicePrincipal.ApplicationId
	})

	apDataObjectVisitor := AccessProviderVisitor{
		syncer:                a,
		accessProviderHandler: accessProviderHandler,
		accountId:             accountId,
		repoCredentials:       repoCredentials,
		pltfrm:                pltfrm,
		storedFunctions:       types.NewStoredFunctions(),
		metaStoreIdMap:        map[string]string{},

		groups:                        groups,
		servicePrincipals:             servicePrincipals,
		includeMetastoreInExternalAps: configMap.GetBoolWithDefault(constants.DatabricksIncludeMetastoreInGrantName, false),
	}

	err = traverser.Traverse(ctx, &apDataObjectVisitor, func(traverserOptions *DataObjectTraverserOptions) {
		traverserOptions.SecurableTypesToReturn = set.NewSet[string](constants.WorkspaceType, constants.MetastoreType, constants.CatalogType, data_source.Schema, data_source.Table, data_source.Column, constants.FunctionType)
	})
	if err != nil {
		return err
	}

	return nil
}

func (a *AccessSyncer) SyncAccessProviderToTarget(ctx context.Context, accessProviders *sync_to_target.AccessProviderImport, accessProviderFeedbackHandler wrappers.AccessProviderFeedbackHandler, configMap *config.ConfigMap) (err error) {
	defer func() {
		if err != nil {
			logger.Error(fmt.Sprintf("SyncAccessProviderToTarget failed: %s", err.Error()))
		}
	}()

	pltfrm, accountId, repoCredentials, err := utils.GetAndValidateParameters(configMap)
	if err != nil {
		return err
	}

	accountRepo, err := a.accountRepoFactory(pltfrm, accountId, &repoCredentials)
	if err != nil {
		return fmt.Errorf("account repo: %w", err)
	}

	_, _, metastoreWorkspaceMap, err := a.loadMetastores(ctx, configMap)
	repoCache := NewMetastoreRepoCache(pltfrm, repoCredentials, a.workspaceRepoFactory, metastoreWorkspaceMap)

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

	a.syncFiltersToTarget(ctx, filters, configMap, &repoCache)
	a.syncMasksToTarget(ctx, masksAps, configMap, &repoCache)
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
			a.storePrivilegesInDataplane(ctx, item, &repoCache, principlePrivilegesMap)
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

func (a *AccessSyncer) syncMasksToTarget(ctx context.Context, maskAps []*sync_to_target.AccessProvider, configMap *config.ConfigMap, repoCache *MetastoreRepoCache) {
	for _, mask := range maskAps {
		feedbackElement := sync_to_target.AccessProviderSyncFeedback{
			AccessProvider: mask.Id,
		}

		maskName, apErr := a.syncMaskToTarget(ctx, mask, configMap, repoCache)

		feedbackElement.ExternalId = &maskName
		feedbackElement.ActualName = maskName

		if apErr != nil {
			feedbackElement.Errors = append(feedbackElement.Errors, apErr.Error())
		}

		a.apFeedbackObjects[mask.Id] = feedbackElement
	}
}

func (a *AccessSyncer) syncFiltersToTarget(ctx context.Context, filters []*sync_to_target.AccessProvider, configMap *config.ConfigMap, repoCache *MetastoreRepoCache) {
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
		actualName, externalId, err := a.syncFilterToTarget(ctx, do, filterAps, configMap, repoCache)

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

func (a *AccessSyncer) syncFilterToTarget(ctx context.Context, do string, aps []*sync_to_target.AccessProvider, configMap *config.ConfigMap, repoCache *MetastoreRepoCache) (filterName string, externalId string, _ error) {
	schemaNameSplit := strings.Split(do, ".")
	metastore := schemaNameSplit[0]
	catalogName := schemaNameSplit[1]
	schemaName := schemaNameSplit[2]
	tableName := schemaNameSplit[3]

	filterName = raitoPrefixName(tableName + "_filter")
	externalId = do + ".filter"

	var warehouseIdMap []types.WarehouseDetails

	if found, err := configMap.Unmarshal(constants.DatabricksSqlWarehouses, &warehouseIdMap); err != nil {
		return filterName, externalId, fmt.Errorf("unmarshal %s: %w", constants.DatabricksSqlWarehouses, err)
	} else if !found {
		return filterName, externalId, fmt.Errorf("no warehouses found in configmap")
	}

	repository, sqlClient, err := a.getSqlClient(ctx, metastore, catalogName, warehouseIdMap, repoCache)
	if err != nil {
		return filterName, externalId, fmt.Errorf("get sql client: %w", err)
	}

	if sqlClient == nil {
		return filterName, externalId, fmt.Errorf("no sql warehouse found for metastore %s", metastore)
	}

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

func (a *AccessSyncer) getSqlClient(ctx context.Context, metastore string, catalogName string, warehouseIdMap []types.WarehouseDetails, repoCache *MetastoreRepoCache) (dataAccessWorkspaceRepository, repo.WarehouseRepository, error) {
	workspacesWithWarehouses := make([]string, 0, len(warehouseIdMap))
	for _, warehouse := range warehouseIdMap {
		workspacesWithWarehouses = append(workspacesWithWarehouses, warehouse.Workspace)
	}

	repository, deploymentName := repoCache.GetCatalogRepo(ctx, metastore, catalogName, workspacesWithWarehouses...)
	if repository == nil {
		return nil, nil, fmt.Errorf("no workspace repository for metastore %q and catalog %q", metastore, catalogName)
	}

	var warehouseId string

	for _, warehouse := range warehouseIdMap {
		if warehouse.Workspace == deploymentName {
			warehouseId = warehouse.Warehouse
			break
		}
	}

	if warehouseId == "" {
		return repository, nil, nil
	}

	sqlClient := repository.SqlWarehouseRepository(warehouseId)

	return repository, sqlClient, nil
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
	workspaceId, err := strconv.ParseInt(item.FullName, 10, 64)
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

func (a *AccessSyncer) storePrivilegesInComputePlaneForPrincipal(ctx context.Context, principal string, repo dataAccessAccountRepository, workspaceId int64, privilegesChanges *types.PrivilegesChanges) {
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

	users := accountRepo.ListUsers(cancelCtx, func(options *types2.DatabricksUsersFilter) { options.Username = &email })
	for user := range users {
		if user.HasError() {
			return nil, fmt.Errorf("list user item: %w", user.Error())
		} else {
			return user.I, nil
		}
	}

	return nil, fmt.Errorf("no user found for email %q", email)
}

func (a *AccessSyncer) getGroupIdFromName(ctx context.Context, groupname string, accountRepo dataAccessAccountRepository) (*iam.Group, error) {
	cancelCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	groups := accountRepo.ListGroups(cancelCtx, func(options *types2.DatabricksGroupsFilter) { options.Groupname = &groupname })
	for group := range groups {
		if group.HasError() {
			return nil, fmt.Errorf("list group item: %w", group.Error())
		} else {
			return group.I, nil
		}
	}

	return nil, fmt.Errorf("no groupe found with name %q", groupname)
}

func (a *AccessSyncer) storePrivilegesInDataplane(ctx context.Context, item types.SecurableItemKey, repoCache *MetastoreRepoCache, principlePrivilegesMap map[string]*types.PrivilegesChanges) {
	var metastore, fullname string
	var err error

	defer func() {
		if err != nil {
			for _, privilegesChanges := range principlePrivilegesMap {
				a.handleAccessProviderError(privilegesChanges, err)
			}
		}
	}()

	var repo dataAccessWorkspaceRepository
	var workspaceDeploymentName string

	if item.Type == constants.MetastoreType {
		metastore = item.FullName
		fullname = item.FullName

		repo, workspaceDeploymentName = repoCache.GetMetastoreRepo(ctx, metastore)
	} else {
		metastore, fullname = getMetastoreAndFullnameOfUniqueId(item.FullName)

		catalogName := strings.SplitN(fullname, ".", 2)[0]
		repo, workspaceDeploymentName = repoCache.GetCatalogRepo(ctx, metastore, catalogName)
	}

	if repo == nil {
		logger.Error(fmt.Sprintf("no workspace repository for %q", item.FullName))

		return
	}

	logger.Debug(fmt.Sprintf("sync privileges for %s %q via workspace %q", item.Type, fullname, workspaceDeploymentName))

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

func (a *AccessSyncer) syncMaskToTarget(ctx context.Context, ap *sync_to_target.AccessProvider, configMap *config.ConfigMap, repoCache *MetastoreRepoCache) (maskName string, _ error) {
	// 0. Prepare mask update
	if ap.ExternalId != nil {
		maskName = *ap.ExternalId
	} else {
		maskName = raitoPrefixName(ap.NamingHint)
	}

	logger.Debug(fmt.Sprintf("Syncing mask %q to target", maskName))

	schemas := a.syncMasksGetSchema(ap)

	var warehouseIdMap []types.WarehouseDetails

	if found, err := configMap.Unmarshal(constants.DatabricksSqlWarehouses, &warehouseIdMap); err != nil {
		return maskName, err
	} else if !found {
		return maskName, fmt.Errorf("no warehouses found in configmap")
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
		err := a.syncMaskInSchema(ctx, ap, schema, warehouseIdMap, maskName, dos, beneficiaries, repoCache)
		if err != nil {
			return maskName, err
		}
	}

	return maskName, nil
}

func (a *AccessSyncer) syncMaskInSchema(ctx context.Context, ap *sync_to_target.AccessProvider, schema string, warehouseIdMap []types.WarehouseDetails, maskName string, dos types.MaskDataObjectsOfSchema, beneficiaries *masks.MaskingBeneficiaries, repoCache *MetastoreRepoCache) error {
	schemaNameSplit := strings.Split(schema, ".")
	metastore := schemaNameSplit[0]
	catalogName := schemaNameSplit[1]
	schemaName := schemaNameSplit[2]

	_, sqlClient, err := a.getSqlClient(ctx, metastore, catalogName, warehouseIdMap, repoCache)
	if err != nil {
		return fmt.Errorf("get sql client: %w", err)
	}

	if sqlClient == nil {
		return fmt.Errorf("no sql warehouse found for metastore %s", metastore)
	}

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
	tableInformationMap := map[string]map[string]*types2.ColumnInformation{}

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

func (a *AccessSyncer) loadMetastores(ctx context.Context, configMap *config.ConfigMap) ([]catalog.MetastoreInfo, []provisioning.Workspace, map[string][]*provisioning.Workspace, error) {
	pltfrm, accountId, repoCredentials, err := utils.GetAndValidateParameters(configMap)
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

		utils.AddToSetInMap(objectPermissions, *whatItem.DataObject, priv)
		utils.AddToSetInMap(additionObjectPermissions, *whatItem.DataObject, priv)

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
		utils.AddToSetInMap(result, object, string(catalog.PrivilegeUseCatalog))

		fullname, err := cutLastPartFullName(object.FullName)
		if err != nil {
			return err
		}

		return addUsageToUpperDataObjects(result, data_source.DataObjectReference{FullName: fullname, Type: constants.MetastoreType})
	case data_source.Schema:
		utils.AddToSetInMap(result, object, string(catalog.PrivilegeUseSchema))

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

var _ DataObjectVisitor = (*AccessProviderVisitor)(nil)

type AccessProviderVisitor struct {
	syncer                *AccessSyncer
	accessProviderHandler wrappers.AccessProviderHandler

	groups            set.Set[string]
	servicePrincipals set.Set[string]

	repoCredentials               types2.RepositoryCredentials
	accountId                     string
	pltfrm                        platform.DatabricksPlatform
	storedFunctions               types.StoredFunctions
	metaStoreIdMap                map[string]string
	includeMetastoreInExternalAps bool
}

func (a *AccessProviderVisitor) VisitWorkspace(ctx context.Context, workspace *provisioning.Workspace) error {
	accountClient, err := a.syncer.accountRepoFactory(a.pltfrm, a.accountId, &a.repoCredentials)
	if err != nil {
		return fmt.Errorf("create account repository: %w", err)
	}

	assignments, err := accountClient.ListWorkspaceAssignments(ctx, workspace.WorkspaceId)
	if err != nil {
		return fmt.Errorf("list workspace assignments: %w", err)
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

		do := data_source.DataObjectReference{FullName: strconv.FormatInt(workspace.WorkspaceId, 10), Type: constants.WorkspaceType}

		for _, permission := range assignment.Permissions {
			p := string(permission)
			if !a.syncer.privilegeCache.ContainsPrivilege(do, principalId, p) {
				privilegesToSync[p] = append(privilegesToSync[p], principalId)
			}
		}
	}

	for privilege, principleList := range privilegesToSync {
		apExternalId := fmt.Sprintf("%s_%s", workspace.WorkspaceName, privilege)
		apName := fmt.Sprintf("%s %s - %s", TitleCaser.String(constants.WorkspaceType), workspace.WorkspaceName, privilege)

		whoItems := sync_from_target.WhoItem{}

		for _, principal := range principleList {
			// We assume that a group doesn't contain an @ character
			if strings.Contains(principal, "@") {
				whoItems.Users = append(whoItems.Users, principal)
			} else {
				whoItems.Groups = append(whoItems.Groups, principal)
			}
		}

		err2 := a.accessProviderHandler.AddAccessProviders(
			&sync_from_target.AccessProvider{
				ExternalId: apExternalId,
				Action:     sync_from_target.Grant,
				Name:       apName,
				NamingHint: apName,
				ActualName: apName,
				Type:       ptr.String(access_provider.AclSet),
				What: []sync_from_target.WhatItem{
					{
						DataObject:  &data_source.DataObjectReference{FullName: strconv.FormatInt(workspace.WorkspaceId, 10), Type: constants.WorkspaceType},
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

func (a *AccessProviderVisitor) VisitMetastore(ctx context.Context, metastore *catalog.MetastoreInfo, workspaces []*provisioning.Workspace) error {
	a.metaStoreIdMap[metastore.MetastoreId] = metastore.Name

	if len(workspaces) == 0 {
		return fmt.Errorf("no workspace found for metastore %s", metastore.MetastoreId)
	}

	var workspaceClient dataAccessWorkspaceRepository
	var err error

	for _, workspace := range workspaces {
		var werr error

		workspaceClient, werr = a.getWorkspaceRepository(workspace)
		if werr != nil {
			err = multierror.Append(err, werr)

			continue
		}

		err = nil

		break
	}

	if err != nil {
		return fmt.Errorf("unable to get workspace repository: %w", err)
	}

	logger.Debug(fmt.Sprintf("Load permissions on metastore %q", metastore.MetastoreId))

	permissionsList, err := workspaceClient.GetPermissionsOnResource(ctx, catalog.SecurableTypeMetastore, metastore.MetastoreId)
	if err != nil {
		return err
	}

	logger.Debug(fmt.Sprintf("Process permission on metastore %q", metastore.Name))

	err = a.addPermissionIfNotSetByRaito(fmt.Sprintf("%s %s", TitleCaser.String(constants.MetastoreType), metastore.Name), &data_source.DataObjectReference{FullName: metastore.Name, Type: constants.MetastoreType}, permissionsList)
	if err != nil {
		return err
	}

	return nil
}

func (a *AccessProviderVisitor) VisitCatalog(ctx context.Context, c *catalog.CatalogInfo, _ *catalog.MetastoreInfo, workspace *provisioning.Workspace) error {
	workspaceClient, err := a.getWorkspaceRepository(workspace)
	if err != nil {
		return fmt.Errorf("unable to get workspace repository: %w", err)
	}

	metastoreName, ok := a.metaStoreIdMap[c.MetastoreId]
	if !ok {
		logger.Warn(fmt.Sprintf("Unable to find metastore name for metastore id %q", c.MetastoreId))
		metastoreName = c.MetastoreId
	}

	return a.syncAccessProviderObjectFromTarget(ctx, workspaceClient, metastoreName, c.MetastoreId, c.FullName, constants.CatalogType, catalog.SecurableTypeCatalog)
}

func (a *AccessProviderVisitor) VisitSchema(ctx context.Context, schema *catalog.SchemaInfo, _ *catalog.CatalogInfo, workspace *provisioning.Workspace) error {
	workspaceClient, err := a.getWorkspaceRepository(workspace)
	if err != nil {
		return fmt.Errorf("unable to get workspace repository: %w", err)
	}

	metastoreName, ok := a.metaStoreIdMap[schema.MetastoreId]
	if !ok {
		logger.Warn(fmt.Sprintf("Unable to find metastore name for metastore id %q", schema.MetastoreId))
		metastoreName = schema.MetastoreId
	}

	return a.syncAccessProviderObjectFromTarget(ctx, workspaceClient, metastoreName, schema.MetastoreId, schema.FullName, data_source.Schema, catalog.SecurableTypeSchema)
}

func (a *AccessProviderVisitor) VisitTable(ctx context.Context, table *catalog.TableInfo, parent *catalog.SchemaInfo, workspace *provisioning.Workspace) error {
	workspaceClient, err := a.getWorkspaceRepository(workspace)
	if err != nil {
		return fmt.Errorf("unable to get workspace repository: %w", err)
	}

	metastoreName, ok := a.metaStoreIdMap[table.MetastoreId]
	if !ok {
		logger.Warn(fmt.Sprintf("Unable to find metastore name for metastore id %q", table.MetastoreId))
		metastoreName = table.MetastoreId
	}

	if table.RowFilter != nil {
		functionId := createUniqueId(table.MetastoreId, table.RowFilter.FunctionName)
		a.storedFunctions.AddFilter(functionId, createUniqueId(table.MetastoreId, table.FullName))
	}

	return a.syncAccessProviderObjectFromTarget(ctx, workspaceClient, metastoreName, table.MetastoreId, table.FullName, data_source.Table, catalog.SecurableTypeTable)
}

func (a *AccessProviderVisitor) VisitColumn(_ context.Context, column *catalog.ColumnInfo, table *catalog.TableInfo, _ *provisioning.Workspace) error {
	if column.Mask != nil {
		functionId := createUniqueId(table.MetastoreId, column.Mask.FunctionName)
		a.storedFunctions.AddMask(functionId, createTableUniqueId(table.MetastoreId, table.FullName, column.Name))
	}

	return nil
}

func (a *AccessProviderVisitor) VisitFunction(ctx context.Context, function *catalog.FunctionInfo, parent *catalog.SchemaInfo, workspace *provisioning.Workspace) error {
	if strings.HasPrefix(function.Name, raitoPrefix) {
		// NO need to import functions create by Raito
		return nil
	}

	functionId := createUniqueId(function.MetastoreId, function.FullName)
	if columns, found := a.storedFunctions.Masks[functionId]; found {
		what := make([]sync_from_target.WhatItem, 0, len(columns))

		for _, column := range columns {
			what = append(what, sync_from_target.WhatItem{
				DataObject: &data_source.DataObjectReference{FullName: column, Type: data_source.Column},
			})
		}

		return a.accessProviderHandler.AddAccessProviders(&sync_from_target.AccessProvider{
			ExternalId:        functionId,
			Name:              function.Name,
			ActualName:        functionId,
			Policy:            function.RoutineDefinition,
			Action:            sync_from_target.Mask,
			What:              what,
			NotInternalizable: true,
			Incomplete:        ptr.Bool(true),
		})
	} else if tables, found := a.storedFunctions.Filters[functionId]; found {
		// Currently this is not called due to a bug in by Databricks as they dont return correctly the row filter function name
		for _, table := range tables {
			err := a.accessProviderHandler.AddAccessProviders(&sync_from_target.AccessProvider{
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
		workspaceClient, err := a.getWorkspaceRepository(workspace)
		if err != nil {
			return fmt.Errorf("unable to get workspace repository: %w", err)
		}

		metastoreName, ok := a.metaStoreIdMap[function.MetastoreId]
		if !ok {
			logger.Warn(fmt.Sprintf("Unable to find metastore name for metastore id %q", function.MetastoreId))
			metastoreName = function.MetastoreId
		}

		return a.syncAccessProviderObjectFromTarget(ctx, workspaceClient, metastoreName, function.MetastoreId, function.FullName, constants.FunctionType, catalog.SecurableTypeFunction)
	}

	return nil
}

func (a *AccessProviderVisitor) getWorkspaceRepository(workspace *provisioning.Workspace) (dataAccessWorkspaceRepository, error) {
	if workspace == nil {
		return nil, errors.New("workspace not found")
	}

	credentials, err2 := utils.InitializeWorkspaceRepoCredentials(a.repoCredentials, a.pltfrm, workspace)
	if err2 != nil {
		return nil, fmt.Errorf("workspace address: %w", err2)
	}

	client, err2 := a.syncer.workspaceRepoFactory(credentials)
	if err2 != nil {
		return nil, err2
	}

	return client, nil
}

func (a *AccessProviderVisitor) syncAccessProviderObjectFromTarget(ctx context.Context, workspaceClient dataAccessWorkspaceRepository, metastoreName, metastoreId, fullName string, doType string, securableType catalog.SecurableType) error {
	permissionsList, err := workspaceClient.GetPermissionsOnResource(ctx, securableType, fullName)
	if err != nil {
		return err
	}

	return a.addPermissionIfNotSetByRaito(createAccessProviderNamePrefix(metastoreName, fullName, doType, a.includeMetastoreInExternalAps), &data_source.DataObjectReference{FullName: createUniqueId(metastoreId, fullName), Type: doType}, permissionsList)
}

func (a *AccessProviderVisitor) addPermissionIfNotSetByRaito(apNamePrefix string, do *data_source.DataObjectReference, assignments *catalog.PermissionsList) error {
	if assignments == nil {
		return nil
	}

	privilegeToPrincipleMap := make(map[catalog.Privilege][]string)

	for _, assignment := range assignments.PrivilegeAssignments {
		for _, privilege := range assignment.Privileges {
			logger.Debug(fmt.Sprintf("Check if privilege was assigned by Raito: {%s, %s}, %s, %v", do.FullName, do.Type, assignment.Principal, privilege))

			if a.syncer.privilegeCache.ContainsPrivilege(*do, assignment.Principal, string(privilege)) {
				logger.Debug(fmt.Sprintf("Privilege was assigned by Raito and will be ignored: %v, %s, %v", *do, assignment.Principal, privilege))
				continue
			}

			privilegeToPrincipleMap[privilege] = append(privilegeToPrincipleMap[privilege], assignment.Principal)
		}
	}

	for privilege, principleList := range privilegeToPrincipleMap {
		humanReadablePrivilege := strings.ToUpper(strings.ReplaceAll(privilege.String(), "_", " "))

		externalId := fmt.Sprintf("%s_%s", do.FullName, privilege.String())
		apName := fmt.Sprintf("%s - %s", apNamePrefix, humanReadablePrivilege)

		whoItems := sync_from_target.WhoItem{}

		// The principal can be a user email address, a group name or a service principal ID (https://docs.databricks.com/api/workspace/grants/get#privilege_assignments)
		for _, principal := range principleList {
			if a.servicePrincipals.Contains(principal) {
				whoItems.Users = append(whoItems.Users, principal)
			} else if a.groups.Contains(principal) {
				whoItems.Groups = append(whoItems.Groups, principal)
			} else if strings.Contains(principal, "@") {
				whoItems.Users = append(whoItems.Users, principal)
			} else {
				logger.Warn(fmt.Sprintf("Unable to find to validate if %q is users, group or service principal", principal))
			}
		}

		err := a.accessProviderHandler.AddAccessProviders(
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
						Permissions: []string{humanReadablePrivilege},
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

func createAccessProviderNamePrefix(metastoreId string, fullName string, doType string, includeMetastore bool) string {
	objectName := fullName
	if includeMetastore {
		objectName = fmt.Sprintf("%s.%s", metastoreId, fullName)
	}

	return fmt.Sprintf("%s %s", TitleCaser.String(doType), objectName)
}

type metastoreRepoCacheItem struct {
	dataAccessWorkspaceRepository
	workspaceDeploymentName string
}

type MetastoreRepoCache struct {
	pltfrm                platform.DatabricksPlatform
	repoCredentials       types2.RepositoryCredentials
	repoFn                func(*types2.RepositoryCredentials) (dataAccessWorkspaceRepository, error)
	metastoreWorkspaceMap map[string][]*provisioning.Workspace

	metastoreCatalogRepoCache map[string]map[string][]metastoreRepoCacheItem
	metastoreRepoCache        map[string][]metastoreRepoCacheItem
}

func NewMetastoreRepoCache(pltfrm platform.DatabricksPlatform, repoCredentials types2.RepositoryCredentials, repoFn func(*types2.RepositoryCredentials) (dataAccessWorkspaceRepository, error), metastoreWorkspaceMap map[string][]*provisioning.Workspace) MetastoreRepoCache {
	return MetastoreRepoCache{
		pltfrm:                    pltfrm,
		repoCredentials:           repoCredentials,
		metastoreWorkspaceMap:     metastoreWorkspaceMap,
		repoFn:                    repoFn,
		metastoreCatalogRepoCache: make(map[string]map[string][]metastoreRepoCacheItem),
		metastoreRepoCache:        make(map[string][]metastoreRepoCacheItem),
	}
}

func (t *MetastoreRepoCache) GetMetastoreRepo(ctx context.Context, metastoreId string) (dataAccessWorkspaceRepository, string) {
	possibleRepos, found := t.metastoreRepoCache[metastoreId]
	if !found {
		t.loadMetastore(ctx, metastoreId)

		if possibleRepos, found = t.metastoreRepoCache[metastoreId]; !found {
			return nil, ""
		}
	}

	for _, r := range possibleRepos {
		if err := r.Ping(ctx); err == nil {
			return r, r.workspaceDeploymentName
		}
	}

	return nil, ""
}

func (t *MetastoreRepoCache) GetCatalogRepo(ctx context.Context, metastoreId string, catalogId string, preferredWorkspaces ...string) (dataAccessWorkspaceRepository, string) {
	possibleRepos, found := t.metastoreCatalogRepoCache[metastoreId]
	if !found {
		t.loadMetastore(ctx, metastoreId)

		if possibleRepos, found = t.metastoreCatalogRepoCache[metastoreId]; !found {
			return nil, ""
		}
	}

	r, found := possibleRepos[catalogId]
	if !found {
		return nil, ""
	}

	preferredWorkspacesSet := set.NewSet(preferredWorkspaces...)

	possiblePreferedRepos := make([]metastoreRepoCacheItem, 0, len(preferredWorkspaces))
	possibleUnpreferedRepos := make([]metastoreRepoCacheItem, 0, len(r))

	for _, possibleRepo := range r {
		if preferredWorkspacesSet.Contains(possibleRepo.workspaceDeploymentName) {
			possiblePreferedRepos = append(possiblePreferedRepos, possibleRepo)
		} else {
			possibleUnpreferedRepos = append(possibleUnpreferedRepos, possibleRepo)
		}
	}

	r = slices.Concat(possiblePreferedRepos, possibleUnpreferedRepos)

	for _, possibleRepo := range r {
		if err := possibleRepo.Ping(ctx); err == nil {
			return possibleRepo, possibleRepo.workspaceDeploymentName
		}
	}

	return nil, ""
}

func (t *MetastoreRepoCache) loadMetastore(ctx context.Context, metastoreId string) {
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, metastoreWorkspace := range t.metastoreWorkspaceMap[metastoreId] {
		r, werr := utils.InitWorkspaceRepo(ctx, t.repoCredentials, t.pltfrm, metastoreWorkspace, t.repoFn)
		if werr != nil {
			continue
		}

		werr = r.Ping(ctx)
		if werr != nil {
			continue
		}

		item := metastoreRepoCacheItem{
			dataAccessWorkspaceRepository: r,
			workspaceDeploymentName:       metastoreWorkspace.DeploymentName,
		}

		t.metastoreRepoCache[metastoreId] = append(t.metastoreRepoCache[metastoreId], item)
		t.metastoreCatalogRepoCache[metastoreId] = make(map[string][]metastoreRepoCacheItem)

		catalogChannel := r.ListCatalogs(cancelCtx)

		for c := range catalogChannel {
			if c.Err != nil {
				continue
			}

			t.metastoreCatalogRepoCache[metastoreId][c.I.Name] = append(t.metastoreCatalogRepoCache[metastoreId][c.I.Name], item)
		}
	}
}
