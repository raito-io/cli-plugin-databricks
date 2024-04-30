package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/hashicorp/go-hclog"
	"github.com/imroc/req/v3"

	databricks2 "cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/databricks/platform"
)

const (
	databricksAccountConsoleApiVersionKeyStr = "X-Databricks-Account-Console-API-Version"
	queryHistoryLimit                        = 10000
)

type repositoryRequestFactory interface {
	NewRequest(ctx context.Context) (*req.Request, error)
}

func getDefaultClient(host string, verbosity string) *req.Client {
	client := req.NewClient().SetBaseURL(host).SetCommonHeader("user-agent", "Raito").SetCommonContentType("application/json")

	writer := logger.StandardWriter(&hclog.StandardLoggerOptions{
		InferLevels:              false,
		InferLevelsWithTimestamp: false,
		ForceLevel:               hclog.Debug,
	})

	switch verbosity {
	case databricks2.RestCallVerbosityBody:
		client.EnableDump(&req.DumpOptions{
			Output:         writer,
			RequestHeader:  false,
			RequestBody:    true,
			ResponseHeader: false,
			ResponseBody:   true,
			Async:          false,
		})
	case databricks2.RestCallVerbosityFull:
		client.EnableDump(&req.DumpOptions{
			Output:         writer,
			RequestHeader:  true,
			RequestBody:    true,
			ResponseHeader: true,
			ResponseBody:   true,
			Async:          false,
		})
	}

	return client
}

type BasicAuthAccountRepositoryRequestFactory struct {
	client *req.Client
}

func NewBasicAuthAccountRepositoryRequestFactory(host, user, password string) *BasicAuthAccountRepositoryRequestFactory {
	logger.Debug("Creating basic auth request factory")

	verbosity := os.Getenv(databricks2.DatabricksRestCallVerbosityEnvvar)

	return &BasicAuthAccountRepositoryRequestFactory{
		client: getDefaultClient(host, verbosity).SetCommonBasicAuth(user, password),
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

func NewOAuthAccountRepositoryRequestFactory(accountHost string, host string, clientId string, clientSecret string, accountId string) *OAuthRepositoryRequestFactory {
	logger.Debug("Creating oauth request factory")

	verbosity := os.Getenv(databricks2.DatabricksRestCallVerbosityEnvvar)

	return &OAuthRepositoryRequestFactory{
		client:           getDefaultClient(host, verbosity),
		tokenRenewClient: getDefaultClient(accountHost, "off"),
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

func NewAccountRepository(pltfrm platform.DatabricksPlatform, credentials *RepositoryCredentials, accountId string) (*AccountRepository, error) {
	var factory repositoryRequestFactory

	accountHost, err := pltfrm.Host()
	if err != nil {
		return nil, fmt.Errorf("get host for platform %s: %w", pltfrm, err)
	}

	if credentials.ClientId != "" && credentials.ClientSecret != "" {
		factory = NewOAuthAccountRepositoryRequestFactory(accountHost, accountHost, credentials.ClientId, credentials.ClientSecret, accountId)
	} else {
		factory = NewBasicAuthAccountRepositoryRequestFactory(accountHost, credentials.Username, credentials.Password)
	}

	return &AccountRepository{
		clientFactory: factory,
		accountId:     accountId,
	}, nil
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

		startIndex := "1"

		queryParams := make(map[string]string)

		if options.Username != nil {
			queryParams["filter"] = fmt.Sprintf("userName eq %s", *options.Username)
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

		startIndex := "1"

		queryParams := make(map[string]string)

		if options.ServicePrincipalName != nil {
			queryParams["filter"] = fmt.Sprintf("displayName eq %s", *options.ServicePrincipalName)
		}

		for {
			queryParams["startIndex"] = startIndex

			var result iam.ListServicePrincipalResponse

			request, err := r.clientFactory.NewRequest(ctx)
			if err != nil {
				send(err)
				return
			}

			response, err := request.SetHeader(databricksAccountConsoleApiVersionKeyStr, "2.0").
				SetSuccessResult(&result).
				SetPathParam("account_id", r.accountId).
				SetQueryParams(queryParams).
				Get("/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals")

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

func (r *AccountRepository) ListGroups(ctx context.Context, optFn ...func(options *DatabricksGroupsFilter)) <-chan interface{} {
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

		startIndex := "1"

		queryParams := make(map[string]string)

		if options.Groupname != nil {
			queryParams["filter"] = fmt.Sprintf("displayName eq %s", *options.Groupname)
		}

		for {
			queryParams["startIndex"] = startIndex

			var result iam.ListGroupsResponse

			request, err := r.clientFactory.NewRequest(ctx)
			if err != nil {
				send(fmt.Errorf("new request: %w", err))
				return
			}

			response, err := request.
				SetHeader(databricksAccountConsoleApiVersionKeyStr, "2.0").
				SetSuccessResult(&result).SetPathParam("account_id", r.accountId).
				SetQueryParams(queryParams).
				Get("/api/2.0/accounts/{account_id}/scim/v2/Groups")

			if err != nil {
				send(fmt.Errorf("get request: %w", err))
				return
			}

			if response.IsErrorState() {
				send(fmt.Errorf("error state %d: %w", response.StatusCode, response.Err))

				logger.Debug(fmt.Sprintf("body of error state %q", response.String()))

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

func NewWorkspaceRepository(pltfrm platform.DatabricksPlatform, host string, accountId string, credentials *RepositoryCredentials) (*WorkspaceRepository, error) {
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
		accountHost, err2 := pltfrm.Host()
		if err2 != nil {
			return nil, fmt.Errorf("get host for platform %s: %w", pltfrm, err2)
		}

		restClient = NewOAuthAccountRepositoryRequestFactory(accountHost, host, credentials.ClientId, credentials.ClientSecret, accountId)
	} else {
		restClient = NewBasicAuthAccountRepositoryRequestFactory(host, credentials.Username, credentials.Password)
	}

	return &WorkspaceRepository{
		client:     client,
		restClient: restClient,
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
