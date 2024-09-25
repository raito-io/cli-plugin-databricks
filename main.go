package main

import (
	"fmt"

	"github.com/hashicorp/go-hclog"
	"github.com/raito-io/cli/base"
	"github.com/raito-io/cli/base/access_provider"
	"github.com/raito-io/cli/base/info"
	"github.com/raito-io/cli/base/util/plugin"
	"github.com/raito-io/cli/base/wrappers"

	"cli-plugin-databricks/databricks"
	"cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/version"
)

var logger hclog.Logger

func main() {
	logger = base.Logger()
	logger.SetLevel(hclog.Debug)

	err := base.RegisterPlugins(
		wrappers.DataSourceSync(databricks.NewDataSourceSyncer()),
		wrappers.IdentityStoreSync(databricks.NewIdentityStoreSyncer()),
		wrappers.DataAccessSync(databricks.NewAccessSyncer(), access_provider.WithSupportPartialSync()),
		wrappers.DataUsageSync(databricks.NewDataUsageSyncer()),
		&info.InfoImpl{
			Info: &plugin.PluginInfo{
				Name:    "Databricks",
				Version: plugin.ParseVersion(version.Version),
				Parameters: []*plugin.ParameterInfo{
					{Name: constants.DatabricksAccountId, Description: "The Databricks account to connect to.", Mandatory: true},
					{Name: constants.DatabricksPlatform, Description: "The Databricks platform to connect to (AWS/GCP/Azure).", Mandatory: true},

					// Native authentication
					{Name: constants.DatabricksClientId, Description: "The (oauth) client ID to use when authenticating against the Databricks account.", Mandatory: false},
					{Name: constants.DatabricksClientSecret, Description: "The (oauth) client Secret to use when authentic against the Databricks account.", Mandatory: false},
					{Name: constants.DatabricksUser, Description: "The username to authenticate against the Databricks account.", Mandatory: false},
					{Name: constants.DatabricksPassword, Description: "The password to authenticate against the Databricks account.", Mandatory: false},
					{Name: constants.DatabricksToken, Description: "The Databricks personal access token (PAT) (AWS, Azure, and GCP) or Azure Active Directory (Azure AD) token (Azure).", Mandatory: false},

					// Azure authentication
					{Name: constants.DatabricksAzureUseMSI, Description: "true to use Azure Managed Service Identity passwordless authentication flow for service principals. Requires AzureResourceID to be set.", Mandatory: false},
					{Name: constants.DatabricksAzureClientId, Description: "The Azure AD service principal's client secret.", Mandatory: false},
					{Name: constants.DatabricksAzureClientSecret, Description: "The Azure AD service principal's application ID.", Mandatory: false},
					{Name: constants.DatabricksAzureTenantID, Description: "The Azure AD service principal's tenant ID.", Mandatory: false},
					{Name: constants.DatabricksAzureEnvironment, Description: "The Azure environment type (such as Public, UsGov, China, and Germany) for a specific set of API endpoints. Defaults to PUBLIC.", Mandatory: false},

					// GCP authentication
					{Name: constants.DatabricksGoogleCredentials, Description: "GCP Service Account Credentials JSON or the location of these credentials on the local filesystem.", Mandatory: false},
					{Name: constants.DatabricksGoogleServiceAccount, Description: "The Google Cloud Platform (GCP) service account e-mail used for impersonation in the Default Application Credentials Flow that does not require a password.", Mandatory: false},

					{Name: constants.DatabricksDataUsageWindow, Description: "The maximum number of days of usage data to retrieve. Default is 90. Maximum is 90 days.", Mandatory: false},

					// Data Object selection
					{Name: constants.DatabricksExcludeWorkspaces, Description: "Optional comma-separated list of workspaces to exclude. If specified, only these buckets will not be handled. Wildcards (*) can be used. Excludes have preference over includes.", Mandatory: false},
					{Name: constants.DatabricksIncludeWorkspaces, Description: "Optional comma-separated list of workspaces to include. If specified, only these buckets will be handled. Wildcards (*) can be used.", Mandatory: false},
					{Name: constants.DatabricksExcludeMetastores, Description: "Optional comma-separated list of metastores to exclude. If specified, only these metastores will not be handled. Wildcards (*) can be used. Excludes have preference over includes.", Mandatory: false},
					{Name: constants.DatabricksIncludeMetastores, Description: "Optional comma-separated list of metastores to include. If specified, only these metastores will be handled. Wildcards (*) can be used.", Mandatory: false},
					{Name: constants.DatabricksExcludeCatalogs, Description: "Optional comma-separated list of catalogs to exclude. If specified, only these catalogs will not be handled. Wildcards (*) can be used. Excludes have preference over includes.", Mandatory: false},
					{Name: constants.DatabricksIncludeCatalogs, Description: "Optional comma-separated list of catalogs to include. If specified, only these catalogs will be handled. Wildcards (*) can be used.", Mandatory: false},
					{Name: constants.DatabricksExcludeSchemas, Description: "Optional comma-separated list of schemas to exclude. If specified, only these schemas will not be handled. Wildcards (*) can be used. Excludes have preference over includes.", Mandatory: false},
					{Name: constants.DatabricksIncludeSchemas, Description: "Optional comma-separated list of schemas to include. If specified, only these schemas will be handled. Wildcards (*) can be used.", Mandatory: false},
					{Name: constants.DatabricksExcludeTables, Description: "Optional comma-separated list of tables to exclude. If specified, only these tables will not be handled. Wildcards (*) can be used. Excludes have preference over includes.", Mandatory: false},
					{Name: constants.DatabricksIncludeTables, Description: "Optional comma-separated list of tables to include. If specified, only these tables will be handled. Wildcards (*) can be used.", Mandatory: false},
				},
			},
		},
	)

	if err != nil {
		logger.Error(fmt.Sprintf("error while registering plugins: %s", err.Error()))
	}
}
