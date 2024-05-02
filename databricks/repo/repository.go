package repo

import (
	"context"
	"fmt"
	"time"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/databricks/databricks-sdk-go/service/provisioning"
	"github.com/databricks/databricks-sdk-go/service/sql"

	"cli-plugin-databricks/databricks/platform"
)

const (
	queryHistoryLimit = 10000
)

type AccountRepository struct {
	dbClient *databricks.AccountClient

	accountId string
}

func NewAccountRepository(pltfrm platform.DatabricksPlatform, credentials *RepositoryCredentials, accountId string) (*AccountRepository, error) {
	accountHost, err := pltfrm.Host()
	if err != nil {
		return nil, fmt.Errorf("get host for platform %s: %w", pltfrm, err)
	}

	config := credentials.DatabricksConfig()
	config.Host = accountHost
	config.AccountID = accountId

	dbClient, err := databricks.NewAccountClient(config)
	if err != nil {
		return nil, fmt.Errorf("create account client: %w", err)
	}

	return &AccountRepository{
		dbClient:  dbClient,
		accountId: accountId,
	}, nil
}

func (r *AccountRepository) ListMetastores(ctx context.Context) ([]catalog.MetastoreInfo, error) {
	return r.dbClient.Metastores.ListAll(ctx)
}

func (r *AccountRepository) GetWorkspacesForMetastore(ctx context.Context, metastoreId string) (*catalog.ListAccountMetastoreAssignmentsResponse, error) {
	return r.dbClient.MetastoreAssignments.ListByMetastoreId(ctx, metastoreId)
}

func (r *AccountRepository) GetWorkspaceMap(ctx context.Context, metastores []catalog.MetastoreInfo, workspaces []provisioning.Workspace) (map[string][]string, map[string]string, error) {
	workspacesMap := make(map[int64]string)

	for wi := range workspaces {
		workspace := &workspaces[wi]

		if workspace.WorkspaceStatus != "RUNNING" {
			logger.Debug(fmt.Sprintf("Workspace %s is not running. Will ignore workspace", workspace.WorkspaceName))
		}

		logger.Debug(fmt.Sprintf("Found running workspace %s with deployment name %s", workspace.WorkspaceName, workspace.DeploymentName))
		workspacesMap[workspace.WorkspaceId] = workspace.DeploymentName
	}

	metastoreToWorkspaceMap := make(map[string][]string)
	workspaceToMetastoreMap := make(map[string]string)

	for i := range metastores {
		metastore := &metastores[i]

		metastoreWorkspaces, err := r.GetWorkspacesForMetastore(ctx, metastore.MetastoreId)
		if err != nil {
			return nil, nil, err
		}

		logger.Debug(fmt.Sprintf("Found %d possible workspaces for metastore %q", len(metastoreWorkspaces.WorkspaceIds), metastore.Name))

		for _, workspaceId := range metastoreWorkspaces.WorkspaceIds {
			if workspaceDeploymentName, ok := workspacesMap[workspaceId]; ok {
				logger.Debug(fmt.Sprintf("Found workspace deployment %q for metastore %q", workspaceDeploymentName, metastore.Name))
				metastoreToWorkspaceMap[metastore.MetastoreId] = append(metastoreToWorkspaceMap[metastore.MetastoreId], workspaceDeploymentName)
				workspaceToMetastoreMap[workspaceDeploymentName] = metastore.MetastoreId
			}
		}

		if len(metastoreToWorkspaceMap[metastore.MetastoreId]) == 0 {
			logger.Warn(fmt.Sprintf("No running workspace found for metastore %s", metastore.Name))
		} else {
			logger.Debug(fmt.Sprintf("Found %d active workspaces for metastore %q", len(metastoreToWorkspaceMap[metastore.MetastoreId]), metastore.Name))
		}
	}

	logger.Debug(fmt.Sprintf("Metastore map: %+v", metastoreToWorkspaceMap))

	return metastoreToWorkspaceMap, workspaceToMetastoreMap, nil
}

func (r *AccountRepository) GetWorkspaces(ctx context.Context) ([]provisioning.Workspace, error) {
	return r.dbClient.Workspaces.List(ctx)
}

func (r *AccountRepository) ListUsers(ctx context.Context, optFn ...func(options *DatabricksUsersFilter)) <-chan interface{} { //nolint:dupl
	options := DatabricksUsersFilter{}
	for _, fn := range optFn {
		fn(&options)
	}

	outputChannel := make(chan interface{})

	go func() {
		defer close(outputChannel)

		send := func(item interface{}) bool {
			select {
			case <-ctx.Done():
				return false
			case outputChannel <- item:
				return true
			}
		}

		var filter string

		if options.Username != nil {
			filter = fmt.Sprintf("userName eq %s", *options.Username)
		}

		it := r.dbClient.Users.List(ctx, iam.ListAccountUsersRequest{
			Filter: filter,
		})

		for it.HasNext(ctx) {
			user, err := it.Next(ctx)
			if err != nil {
				send(err)
				return
			}

			if !send(user) {
				return
			}
		}
	}()

	return outputChannel
}

func (r *AccountRepository) ListServicePrincipals(ctx context.Context, optFn ...func(options *DatabricksServicePrincipalFilter)) <-chan interface{} { //nolint:dupl
	options := DatabricksServicePrincipalFilter{}
	for _, fn := range optFn {
		fn(&options)
	}

	outputChannel := make(chan interface{})

	go func() {
		defer close(outputChannel)

		send := func(item interface{}) bool {
			select {
			case <-ctx.Done():
				return false
			case outputChannel <- item:
				return true
			}
		}

		var filter string

		if options.ServicePrincipalName != nil {
			filter = fmt.Sprintf("displayName eq %s", *options.ServicePrincipalName)
		}

		it := r.dbClient.ServicePrincipals.List(ctx, iam.ListAccountServicePrincipalsRequest{
			Filter: filter,
		})

		for it.HasNext(ctx) {
			servicePrincipal, err := it.Next(ctx)
			if err != nil {
				send(err)
				return
			}

			if !send(servicePrincipal) {
				return
			}
		}
	}()

	return outputChannel
}

func (r *AccountRepository) ListGroups(ctx context.Context, optFn ...func(options *DatabricksGroupsFilter)) <-chan interface{} { //nolint:dupl
	options := DatabricksGroupsFilter{}
	for _, fn := range optFn {
		fn(&options)
	}

	outputChannel := make(chan interface{})

	go func() {
		defer close(outputChannel)

		send := func(item interface{}) bool {
			select {
			case <-ctx.Done():
				return false
			case outputChannel <- item:
				return true
			}
		}

		var filter string

		if options.Groupname != nil {
			filter = fmt.Sprintf("displayName eq %s", *options.Groupname)
		}

		it := r.dbClient.Groups.List(ctx, iam.ListAccountGroupsRequest{
			Filter: filter,
		})

		for it.HasNext(ctx) {
			group, err := it.Next(ctx)
			if err != nil {
				send(err)
				return
			}

			if !send(group) {
				return
			}
		}
	}()

	return outputChannel
}

func (r *AccountRepository) ListWorkspaceAssignments(ctx context.Context, workspaceId int64) ([]iam.PermissionAssignment, error) {
	return r.dbClient.WorkspaceAssignment.ListAll(ctx, iam.ListWorkspaceAssignmentRequest{WorkspaceId: workspaceId})
}

func (r *AccountRepository) UpdateWorkspaceAssignment(ctx context.Context, workspaceId int64, principalId int64, permission []iam.WorkspacePermission) error {
	_, err := r.dbClient.WorkspaceAssignment.Update(ctx, iam.UpdateWorkspaceAssignments{
		Permissions: permission,
		PrincipalId: principalId,
		WorkspaceId: workspaceId,
	})

	return err
}

type WorkspaceRepository struct {
	client *databricks.WorkspaceClient
}

func NewWorkspaceRepository(host string, credentials *RepositoryCredentials) (*WorkspaceRepository, error) {
	config := credentials.DatabricksConfig()
	config.Host = host

	client, err := databricks.NewWorkspaceClient(config)
	if err != nil {
		return nil, err
	}

	return &WorkspaceRepository{
		client: client,
	}, nil
}

func (r *WorkspaceRepository) ListCatalogs(ctx context.Context) ([]catalog.CatalogInfo, error) {
	response, err := r.client.Catalogs.ListAll(ctx, catalog.ListCatalogsRequest{
		IncludeBrowse: true,
	})
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (r *WorkspaceRepository) ListSchemas(ctx context.Context, catalogName string) ([]catalog.SchemaInfo, error) {
	response, err := r.client.Schemas.ListAll(ctx, catalog.ListSchemasRequest{
		CatalogName: catalogName,
	})
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (r *WorkspaceRepository) ListTables(ctx context.Context, catalogName string, schemaName string) ([]catalog.TableInfo, error) {
	response, err := r.client.Tables.ListAll(ctx, catalog.ListTablesRequest{
		CatalogName: catalogName,
		SchemaName:  schemaName,
	})
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (r *WorkspaceRepository) GetTable(ctx context.Context, catalogName string, schemaName string, tableName string) (*catalog.TableInfo, error) {
	response, err := r.client.Tables.Get(ctx, catalog.GetTableRequest{
		FullName: fmt.Sprintf("%s.%s.%s", catalogName, schemaName, tableName),
	})
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (r *WorkspaceRepository) ListFunctions(ctx context.Context, catalogName string, schemaName string) ([]catalog.FunctionInfo, error) {
	return r.client.Functions.ListAll(ctx, catalog.ListFunctionsRequest{
		CatalogName: catalogName,
		SchemaName:  schemaName,
	})
}

func (r *WorkspaceRepository) GetPermissionsOnResource(ctx context.Context, securableType catalog.SecurableType, fullName string) (*catalog.PermissionsList, error) {
	response, err := r.client.Grants.Get(ctx, catalog.GetGrantRequest{
		SecurableType: securableType,
		FullName:      fullName,
	})
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (r *WorkspaceRepository) SetPermissionsOnResource(ctx context.Context, securableType catalog.SecurableType, fullName string, changes ...catalog.PermissionsChange) error {
	_, err := r.client.Grants.Update(ctx, catalog.UpdatePermissions{
		SecurableType: securableType,
		FullName:      fullName,
		Changes:       changes,
	})
	if err != nil {
		return err
	}

	return nil
}

func (r *WorkspaceRepository) GetOwner(ctx context.Context, securableType catalog.SecurableType, fullName string) (string, error) {
	switch securableType { //nolint:exhaustive
	case catalog.SecurableTypeCatalog:
		result, err := r.client.Catalogs.Get(ctx, catalog.GetCatalogRequest{
			Name: fullName,
		})
		if err != nil {
			return "", fmt.Errorf("get catalog %s: %w", fullName, err)
		}

		return result.Owner, nil
	case catalog.SecurableTypeSchema:
		result, err := r.client.Schemas.Get(ctx, catalog.GetSchemaRequest{
			FullName: fullName,
		})
		if err != nil {
			return "", fmt.Errorf("get schema %s: %w", fullName, err)
		}

		return result.Owner, nil
	case catalog.SecurableTypeTable:
		result, err := r.client.Tables.Get(ctx, catalog.GetTableRequest{
			FullName: fullName,
		})
		if err != nil {
			return "", fmt.Errorf("get table %s: %w", fullName, err)
		}

		return result.Owner, nil
	case catalog.SecurableTypeFunction:
		result, err := r.client.Functions.Get(ctx, catalog.GetFunctionRequest{
			Name: fullName,
		})
		if err != nil {
			return "", fmt.Errorf("get function %s: %w", fullName, err)
		}

		return result.Owner, nil
	}

	return "", fmt.Errorf("unsupported securable type: %s", securableType)
}

func (r *WorkspaceRepository) QueryHistory(ctx context.Context, startTime *time.Time, f func(context.Context, *sql.QueryInfo) error) error {
	request := sql.ListQueryHistoryRequest{
		IncludeMetrics: true,
	}

	if startTime != nil {
		request.FilterBy = &sql.QueryFilter{QueryStartTimeRange: &sql.TimeRange{StartTimeMs: int(startTime.UnixMilli())}}
	}

	iterator := r.client.QueryHistory.List(ctx, request)

	i := 0

	for iterator.HasNext(ctx) && i < queryHistoryLimit {
		it, err := iterator.Next(ctx)
		if err != nil {
			return err
		}

		err = f(ctx, &it)
		if err != nil {
			return err
		}

		i += 1
	}

	return nil
}

func (r *WorkspaceRepository) SqlWarehouseRepository(warehouseId string) WarehouseRepository {
	return NewSqlWarehouseRepository(r.client, warehouseId)
}

func (r *WorkspaceRepository) Ping(ctx context.Context) error {
	_, err := r.client.CurrentUser.Me(ctx)
	if err != nil {
		return err
	}

	return nil
}
