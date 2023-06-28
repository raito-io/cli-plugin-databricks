package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/imroc/req/v3"
)

type AccountRepository struct {
	// currently not possible to use the databricks SDK as there is a bug in the List metatsores call
	client *req.Client

	accountId string
}

func NewAccountRepository(user string, password string, accountId string) *AccountRepository {
	return &AccountRepository{
		client:    req.NewClient().SetCommonBasicAuth(user, password).SetBaseURL("https://accounts.cloud.databricks.com/"),
		accountId: accountId,
	}
}

func (r *AccountRepository) ListMetastores(ctx context.Context) ([]catalog.MetastoreInfo, error) {
	var result catalog.ListMetastoresResponse
	response, err := r.client.R().
		SetContext(ctx).
		SetHeader("X-Databricks-Account-Console-API-Version", "2.0").
		SetSuccessResult(&result).
		SetPathParam("accountId", r.accountId).
		Get("/api/2.0/accounts/{accountId}/metastores")

	if err != nil {
		return nil, err
	}

	if response.IsErrorState() {
		return nil, response.Err
	}

	return result.Metastores, nil
}

func (r *AccountRepository) GetWorkspacesForMetastore(ctx context.Context, metastoreId string) (*MetastoreAssignment, error) {
	var result MetastoreAssignment
	response, err := r.client.R().
		SetContext(ctx).
		SetHeader("X-Databricks-Account-Console-API-Version", "2.0").
		SetSuccessResult(&result).
		SetPathParams(map[string]string{"account_id": r.accountId, "metastore_id": metastoreId}).
		EnableDump().
		Get("/api/2.0/accounts/{account_id}/metastores/{metastore_id}/workspaces")

	if err != nil {
		return nil, err
	}

	if response.IsErrorState() {
		return nil, response.Err
	}

	return &result, nil
}

func (r *AccountRepository) GetWorkspaceMap(ctx context.Context, metastores []catalog.MetastoreInfo, workspaces []Workspace) (map[string][]string, error) {
	workspacesMap := make(map[int]string)

	for _, workspace := range workspaces {
		if workspace.WorkspaceStatus != "RUNNING" {
			logger.Debug(fmt.Sprintf("Workspace %s is not running. Will ignore workspace", workspace.WorkspaceName))
		}

		logger.Debug(fmt.Sprintf("Found running workspace %s with deployment name %s", workspace.WorkspaceName, workspace.DeploymentName))
		workspacesMap[workspace.WorkspaceId] = workspace.DeploymentName
	}

	result := make(map[string][]string)

	for i := range metastores {
		metastore := &metastores[i]

		metastoreWorkspaces, err := r.GetWorkspacesForMetastore(ctx, metastore.MetastoreId)
		if err != nil {
			return nil, err
		}

		logger.Debug(fmt.Sprintf("Found %d possible workspaces for metastore %q", len(metastoreWorkspaces.WorkspaceIds), metastore.Name))

		for _, workspaceId := range metastoreWorkspaces.WorkspaceIds {
			if workspaceDeploymentName, ok := workspacesMap[workspaceId]; ok {
				logger.Debug(fmt.Sprintf("Found workspace deployment %q for metastore %q", workspaceDeploymentName, metastore.Name))
				result[metastore.MetastoreId] = append(result[metastore.MetastoreId], workspaceDeploymentName)
			}
		}

		if len(result[metastore.MetastoreId]) == 0 {
			logger.Warn(fmt.Sprintf("No running workspace found for metastore %s", metastore.Name))
		} else {
			logger.Debug(fmt.Sprintf("Found %d active workspaces for metastore %q", len(result[metastore.MetastoreId]), metastore.Name))
		}
	}

	logger.Debug(fmt.Sprintf("Metastore map: %+v", result))

	return result, nil
}

func (r *AccountRepository) GetWorkspaces(ctx context.Context) ([]Workspace, error) {
	var result []Workspace
	response, err := r.client.R().
		SetContext(ctx).
		SetHeader("X-Databricks-Account-Console-API-Version", "2.0").
		SetSuccessResult(&result).
		SetPathParam("accountId", r.accountId).
		Get("/api/2.0/accounts/{accountId}/workspaces")

	if err != nil {
		return nil, err
	}

	if response.IsErrorState() {
		return nil, response.Err
	}

	return result, nil
}

func (r *AccountRepository) ListUsers(ctx context.Context, optFn ...func(options *databricksUsersFilter)) <-chan interface{} { //nolint:dupl
	options := databricksUsersFilter{}
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

		startIndex := "1"

		queryParams := make(map[string]string)

		if options.username != nil {
			queryParams["filter"] = fmt.Sprintf("userName eq %s", *options.username)
		}

		for {
			queryParams["startIndex"] = startIndex

			var result iam.ListUsersResponse
			response, err := r.client.R().
				SetContext(ctx).
				SetHeader("X-Databricks-Account-Console-API-Version", "2.0").
				SetSuccessResult(&result).SetPathParam("account_id", r.accountId).
				SetQueryParams(queryParams).
				Get("/api/2.0/accounts/{account_id}/scim/v2/Users")

			if err != nil {
				send(err)
				return
			}

			if response.IsErrorState() {
				send(response.Err)
				return
			}

			for i := range result.Resources {
				if !send(result.Resources[i]) {
					return
				}
			}

			lastItemIndex := result.StartIndex + result.ItemsPerPage

			if result.TotalResults > lastItemIndex-1 {
				startIndex = strconv.FormatInt(lastItemIndex, 10)
			} else {
				return
			}
		}
	}()

	return outputChannel
}

func (r *AccountRepository) ListGroups(ctx context.Context, optFn ...func(options *databricksGroupsFilter)) <-chan interface{} { //nolint:dupl
	options := databricksGroupsFilter{}
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

		startIndex := "1"

		queryParams := make(map[string]string)

		if options.groupname != nil {
			queryParams["filter"] = fmt.Sprintf("displayName eq %s", *options.groupname)
		}

		for {
			queryParams["startIndex"] = startIndex

			var result iam.ListGroupsResponse
			response, err := r.client.R().
				SetContext(ctx).
				SetHeader("X-Databricks-Account-Console-API-Version", "2.0").
				SetSuccessResult(&result).SetPathParam("account_id", r.accountId).
				SetQueryParams(queryParams).
				Get("/api/2.0/accounts/{account_id}/scim/v2/Groups")

			if err != nil {
				send(err)
				return
			}

			if response.IsErrorState() {
				send(response.Err)
				return
			}

			for i := range result.Resources {
				if !send(result.Resources[i]) {
					return
				}
			}

			lastItemIndex := result.StartIndex + result.ItemsPerPage

			if result.TotalResults > lastItemIndex-1 {
				startIndex = strconv.FormatInt(lastItemIndex, 10)
			} else {
				return
			}
		}
	}()

	return outputChannel
}

func (r *AccountRepository) ListWorkspaceAssignments(ctx context.Context, workspaceId int) ([]iam.PermissionAssignment, error) {
	var result iam.PermissionAssignments

	response, err := r.client.R().
		SetContext(ctx).
		SetHeader("X-Databricks-Account-Console-API-Version", "2.0").
		SetSuccessResult(&result).
		SetPathParams(map[string]string{"account_id": r.accountId, "workspace_id": strconv.Itoa(workspaceId)}).
		Get("/api/2.0/accounts/{account_id}/workspaces/{workspace_id}/permissionassignments")

	if err != nil {
		return nil, err
	}

	if response.IsErrorState() {
		return nil, response.Err
	}

	return result.PermissionAssignments, nil
}

func (r *AccountRepository) UpdateWorkspaceAssignment(ctx context.Context, workspaceId int, principalId int64, permission []iam.WorkspacePermission) error {
	permissions := struct {
		Permissions []iam.WorkspacePermission `json:"permissions"`
	}{
		Permissions: permission,
	}

	jsonBytes, err := json.Marshal(permissions)
	if err != nil {
		return err
	}

	response, err := r.client.R().SetContext(ctx).SetHeader("X-Databricks-Account-Console-API-Version", "2.0").
		SetPathParams(map[string]string{"account_id": r.accountId, "workspace_id": strconv.Itoa(workspaceId), "principal_id": strconv.FormatInt(principalId, 10)}).
		SetBody(jsonBytes).Put("/api/2.0/accounts/{account_id}/workspaces/{workspace_id}/permissionassignments/principals/{principal_id}")

	if err != nil {
		return err
	}

	if response.IsErrorState() {
		return response.Err
	}

	return nil
}

type WorkspaceRepository struct {
	client *databricks.WorkspaceClient
}

func NewWorkspaceRepository(host string, user string, password string) (*WorkspaceRepository, error) {
	client, err := databricks.NewWorkspaceClient(&databricks.Config{
		Username: user,
		Password: password,
		Host:     host,
	})
	if err != nil {
		return nil, err
	}

	return &WorkspaceRepository{
		client: client,
	}, nil
}

func (r *WorkspaceRepository) ListCatalogs(ctx context.Context) ([]catalog.CatalogInfo, error) {
	response, err := r.client.Catalogs.ListAll(ctx)
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

func (r *WorkspaceRepository) ListFunctions(ctx context.Context, catalogName string, schemaName string) ([]catalog.FunctionInfo, error) {
	response, err := r.client.Functions.ListAll(ctx, catalog.ListFunctionsRequest{
		CatalogName: catalogName,
		SchemaName:  schemaName,
	})
	if err != nil {
		return nil, err
	}

	return response, nil
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

func (r *WorkspaceRepository) Ping(ctx context.Context) error {
	_, err := r.client.CurrentUser.Me(ctx)
	if err != nil {
		return err
	}

	return nil
}
