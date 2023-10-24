package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/imroc/req/v3"
)

const databricksAccountConsoleApiVersionKeyStr = "X-Databricks-Account-Console-API-Version"

const accountHost = "https://accounts.cloud.databricks.com/"

type repositoryRequestFactory interface {
	NewRequest(ctx context.Context) (*req.Request, error)
}

func getDefaultClient(host string) *req.Client {
	return req.NewClient().SetBaseURL(host).SetCommonHeader("user-agent", "Raito")
}

type BasicAuthAccountRepositoryRequestFactory struct {
	client *req.Client
}

func NewBasicAuthAccountRepositoryRequestFactory(host, user, password string) *BasicAuthAccountRepositoryRequestFactory {
	logger.Debug("Creating basic auth request factory")

	return &BasicAuthAccountRepositoryRequestFactory{
		client: getDefaultClient(host).SetCommonBasicAuth(user, password),
	}
}

func (f *BasicAuthAccountRepositoryRequestFactory) NewRequest(ctx context.Context) (*req.Request, error) {
	return f.client.R().SetContext(ctx), nil
}

type OAuthRepositoryRequestFactory struct {
	client           *req.Client
	tokenRenewClient *req.Client

	clientId     string
	clientSecret string
	accountId    string

	token       string
	tokenExpiry time.Time
}

func NewOAuthAccountRepositoryRequestFactory(host string, clientId string, clientSecret string, accountId string) *OAuthRepositoryRequestFactory {
	logger.Debug("Creating oauth request factory")

	return &OAuthRepositoryRequestFactory{
		client:           getDefaultClient(host),
		tokenRenewClient: getDefaultClient(accountHost),
		clientId:         clientId,
		clientSecret:     clientSecret,
		accountId:        accountId,
	}
}

func (f *OAuthRepositoryRequestFactory) NewRequest(ctx context.Context) (*req.Request, error) {
	if f.tokenExpiry.Before(time.Now()) {
		if err := f.RefreshToken(ctx); err != nil {
			return nil, err
		}
	}

	return f.client.R().SetBearerAuthToken(f.token).SetContext(ctx), nil
}

func (f *OAuthRepositoryRequestFactory) RefreshToken(ctx context.Context) error {
	logger.Debug("Refresh databricks oauth token")

	accessTokenResponse := struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		Scope       string `json:"scope"`
		TokenType   string `json:"token_type"`
	}{}

	response, err := f.tokenRenewClient.R().SetContext(ctx).SetFormData(map[string]string{"grant_type": "client_credentials", "scope": "all-apis"}).SetPathParams(map[string]string{"account_id": f.accountId}).
		SetBasicAuth(f.clientId, f.clientSecret).Post("/oidc/accounts/{account_id}/v1/token")

	if err != nil {
		return err
	}

	err = response.UnmarshalJson(&accessTokenResponse)
	if err != nil {
		return err
	}

	f.token = accessTokenResponse.AccessToken
	f.tokenExpiry = time.Now().Add(time.Duration(accessTokenResponse.ExpiresIn-5) * time.Second)

	return nil
}

type AccountRepository struct {
	// currently not possible to use the databricks SDK as there is a bug in the List metatsores call
	clientFactory repositoryRequestFactory

	accountId string
}

func NewAccountRepository(credentials RepositoryCredentials, accountId string) *AccountRepository {
	var factory repositoryRequestFactory

	if credentials.ClientId != "" && credentials.ClientSecret != "" {
		factory = NewOAuthAccountRepositoryRequestFactory(accountHost, credentials.ClientId, credentials.ClientSecret, accountId)
	} else {
		factory = NewBasicAuthAccountRepositoryRequestFactory(accountHost, credentials.Username, credentials.Password)
	}

	return &AccountRepository{
		clientFactory: factory,
		accountId:     accountId,
	}
}

func (r *AccountRepository) ListMetastores(ctx context.Context) ([]catalog.MetastoreInfo, error) {
	var result catalog.ListMetastoresResponse

	request, err := r.clientFactory.NewRequest(ctx)
	if err != nil {
		return nil, err
	}

	response, err := request.
		SetHeader(databricksAccountConsoleApiVersionKeyStr, "2.0").
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

	request, err := r.clientFactory.NewRequest(ctx)
	if err != nil {
		return nil, err
	}

	response, err := request.
		SetHeader(databricksAccountConsoleApiVersionKeyStr, "2.0").
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

func (r *AccountRepository) GetWorkspaceMap(ctx context.Context, metastores []catalog.MetastoreInfo, workspaces []Workspace) (map[string][]string, map[string]string, error) {
	workspacesMap := make(map[int]string)

	for _, workspace := range workspaces {
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

func (r *AccountRepository) GetWorkspaces(ctx context.Context) ([]Workspace, error) {
	var result []Workspace

	request, err := r.clientFactory.NewRequest(ctx)
	if err != nil {
		return nil, err
	}

	response, err := request.
		SetHeader(databricksAccountConsoleApiVersionKeyStr, "2.0").
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

			request, err := r.clientFactory.NewRequest(ctx)
			if err != nil {
				send(err)
				return
			}

			response, err := request.
				SetHeader(databricksAccountConsoleApiVersionKeyStr, "2.0").
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

			request, err := r.clientFactory.NewRequest(ctx)
			if err != nil {
				send(err)
				return
			}

			response, err := request.
				SetHeader(databricksAccountConsoleApiVersionKeyStr, "2.0").
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

	request, err := r.clientFactory.NewRequest(ctx)
	if err != nil {
		return nil, err
	}

	response, err := request.
		SetHeader(databricksAccountConsoleApiVersionKeyStr, "2.0").
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

	request, err := r.clientFactory.NewRequest(ctx)
	if err != nil {
		return err
	}

	response, err := request.SetHeader(databricksAccountConsoleApiVersionKeyStr, "2.0").
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

	// SDK workaround
	restClient repositoryRequestFactory
}

func NewWorkspaceRepository(host string, accountId string, credentials RepositoryCredentials) (*WorkspaceRepository, error) {
	client, err := databricks.NewWorkspaceClient(&databricks.Config{
		Username:     credentials.Username,
		Password:     credentials.Password,
		ClientID:     credentials.ClientId,
		ClientSecret: credentials.ClientSecret,
		Host:         host,
	})
	if err != nil {
		return nil, err
	}

	var restClient repositoryRequestFactory
	if credentials.ClientId != "" {
		restClient = NewOAuthAccountRepositoryRequestFactory(host, credentials.ClientId, credentials.ClientSecret, accountId)
	} else {
		restClient = NewBasicAuthAccountRepositoryRequestFactory(host, credentials.Username, credentials.Password)
	}

	return &WorkspaceRepository{
		client:     client,
		restClient: restClient,
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

// FunctionInfo Temporarily defined until bug in SDK is fixed
type FunctionInfo struct {
	// Name of parent catalog.
	CatalogName string `json:"catalog_name,omitempty"`
	// User-provided free-form text description.
	Comment string `json:"comment,omitempty"`
	// Time at which this function was created, in epoch milliseconds.
	CreatedAt int64 `json:"created_at,omitempty"`
	// Username of function creator.
	CreatedBy string `json:"created_by,omitempty"`
	// Scalar function return data type.
	DataType catalog.ColumnTypeName `json:"data_type,omitempty"`
	// External function language.
	ExternalLanguage string `json:"external_language,omitempty"`
	// External function name.
	ExternalName string `json:"external_name,omitempty"`
	// Pretty printed function data type.
	FullDataType string `json:"full_data_type,omitempty"`
	// Full name of function, in form of
	// __catalog_name__.__schema_name__.__function__name__
	FullName string `json:"full_name,omitempty"`
	// Id of Function, relative to parent schema.
	FunctionId string `json:"function_id,omitempty"`
	// The array of __FunctionParameterInfo__ definitions of the function's
	// parameters.
	InputParams []catalog.FunctionParameterInfo `json:"input_params,omitempty"`
	// Whether the function is deterministic.
	IsDeterministic bool `json:"is_deterministic,omitempty"`
	// Function null call.
	IsNullCall bool `json:"is_null_call,omitempty"`
	// Unique identifier of parent metastore.
	MetastoreId string `json:"metastore_id,omitempty"`
	// Name of function, relative to parent schema.
	Name string `json:"name,omitempty"`
	// Username of current owner of function.
	Owner string `json:"owner,omitempty"`
	// Function parameter style. **S** is the value for SQL.
	ParameterStyle catalog.FunctionInfoParameterStyle `json:"parameter_style,omitempty"`
	// A map of key-value properties attached to the securable.
	// INVALID Properties map[string]string `json:"properties,omitempty"`
	// Table function return parameters.
	ReturnParams []catalog.FunctionParameterInfo `json:"return_params,omitempty"`
	// Function language. When **EXTERNAL** is used, the language of the routine
	// function should be specified in the __external_language__ field, and the
	// __return_params__ of the function cannot be used (as **TABLE** return
	// type is not supported), and the __sql_data_access__ field must be
	// **NO_SQL**.
	RoutineBody catalog.FunctionInfoRoutineBody `json:"routine_body,omitempty"`
	// Function body.
	RoutineDefinition string `json:"routine_definition,omitempty"`
	// Function dependencies.
	RoutineDependencies []catalog.Dependency `json:"routine_dependencies,omitempty"`
	// Name of parent schema relative to its parent catalog.
	SchemaName string `json:"schema_name,omitempty"`
	// Function security type.
	SecurityType catalog.FunctionInfoSecurityType `json:"security_type,omitempty"`
	// Specific name of the function; Reserved for future use.
	SpecificName string `json:"specific_name,omitempty"`
	// Function SQL data access.
	SqlDataAccess catalog.FunctionInfoSqlDataAccess `json:"sql_data_access,omitempty"`
	// List of schemes whose objects can be referenced without qualification.
	SqlPath string `json:"sql_path,omitempty"`
	// Time at which this function was created, in epoch milliseconds.
	UpdatedAt int64 `json:"updated_at,omitempty"`
	// Username of user who last modified function.
	UpdatedBy string `json:"updated_by,omitempty"`

	ForceSendFields []string `json:"-"`
}

type FunctionsInfo struct {
	Functions []FunctionInfo `json:"functions,omitempty"`
}

func (r *WorkspaceRepository) ListFunctions(ctx context.Context, catalogName string, schemaName string) ([]FunctionInfo, error) {
	request, err := r.restClient.NewRequest(ctx)
	if err != nil {
		return nil, err
	}

	result := FunctionsInfo{}

	response, err := request.SetHeader(databricksAccountConsoleApiVersionKeyStr, "2.1").SetSuccessResult(&result).SetQueryParams(map[string]string{"catalog_name": catalogName, "schema_name": schemaName}).Get("/api/2.1/unity-catalog/functions")
	if err != nil {
		return nil, err
	}

	if response.IsErrorState() {
		return nil, fmt.Errorf("list functions failed status: %d", response.StatusCode)
	}

	return result.Functions, nil
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

func (r *WorkspaceRepository) QueryHistory(ctx context.Context, startTime *time.Time) ([]sql.QueryInfo, error) {
	request := sql.ListQueryHistoryRequest{}

	if startTime != nil {
		request.FilterBy = &sql.QueryFilter{QueryStartTimeRange: &sql.TimeRange{StartTimeMs: int(startTime.UnixMilli())}}
	}

	queryInfo, err := r.client.QueryHistory.ListAll(ctx, sql.ListQueryHistoryRequest{
		FilterBy: &sql.QueryFilter{
			QueryStartTimeRange: &sql.TimeRange{StartTimeMs: int(startTime.UnixMilli())},
		},
		IncludeMetrics: true,
	})

	if err != nil {
		return nil, err
	}

	return queryInfo, nil
}

func (r *WorkspaceRepository) Ping(ctx context.Context) error {
	_, err := r.client.CurrentUser.Me(ctx)
	if err != nil {
		return err
	}

	return nil
}
