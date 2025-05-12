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

	DatabricksExcludeWorkspaces = "databricks-exclude-workspaces"
	DatabricksIncludeWorkspaces = "databricks-include-workspaces"
	DatabricksExcludeMetastores = "databricks-exclude-metastores"
	DatabricksIncludeMetastores = "databricks-include-metastores"
	DatabricksExcludeCatalogs   = "databricks-exclude-catalogs"
	DatabricksIncludeCatalogs   = "databricks-include-catalogs"
	DatabricksExcludeSchemas    = "databricks-exclude-schemas"
	DatabricksIncludeSchemas    = "databricks-include-schemas"
	DatabricksExcludeTables     = "databricks-exclude-tables"
	DatabricksIncludeTables     = "databricks-include-tables"

	DatabricksIncludeMetastoreInGrantName = "databricks-include-metastore-in-grant-name"

	WorkspaceType        = "workspace"
	MetastoreType        = "metastore"
	CatalogType          = "catalog"
	FunctionType         = "function"
	MaterializedViewType = "materializedview"

	TagSource = "Databricks"
)
