package constants

const (
	DatabricksAccountId               = "databricks-account-id"
	DatabricksUser                    = "databricks-user"
	DatabricksPassword                = "databricks-password"
	DatabricksClientId                = "databricks-client-id"
	DatabricksClientSecret            = "databricks-client-secret"
	DatabricksSqlWarehouses           = "databricks-sql-warehouses"
	DatabricksPlatform                = "databricks-platform"
	DatabricksRestCallVerbosityEnvvar = "DATABRICKS_REST_CALL_VERBOSITY"

	DatabricksDataUsageWindow = "databricks-data-usage-window"

	WorkspaceType = "workspace"
	MetastoreType = "metastore"
	CatalogType   = "catalog"
	FunctionType  = "function"

	RestCallVerbosityNone = "off"
	RestCallVerbosityBody = "body"
	RestCallVerbosityFull = "full"
)
