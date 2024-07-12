package constants

const (
	DatabricksAccountId = "databricks-account-id"

	// Native authentication
	DatabricksUser         = "databricks-user"
	DatabricksPassword     = "databricks-password"
	DatabricksClientId     = "databricks-client-id"
	DatabricksClientSecret = "databricks-client-secret"
	DatabricksToken        = "databricks-token"

	// Azure authentication
	DatabricksAzureUseMSI       = "databricks-azure-use-msi"
	DatabricksAzureClientId     = "databricks-azure-client-id"
	DatabricksAzureClientSecret = "databricks-azure-client-secret"
	DatabricksAzureTenantID     = "databricks-azure-tenant-id"
	DatabricksAzureEnvironment  = "databricks-azure-environment"

	DatabricksGoogleCredentials    = "databricks-google-credentials" //nolint:gosec
	DatabricksGoogleServiceAccount = "databricks-google-service-account"

	DatabricksSqlWarehouses = "databricks-sql-warehouses"
	DatabricksPlatform      = "databricks-platform"

	DatabricksDataUsageWindow = "databricks-data-usage-window"

	WorkspaceType = "workspace"
	MetastoreType = "metastore"
	CatalogType   = "catalog"
	FunctionType  = "function"

	TagSource = "Databricks"
)
