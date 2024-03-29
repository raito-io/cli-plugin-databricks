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
					{Name: databricks.DatabricksAccountId, Description: "The Databricks account to connect to.", Mandatory: true},
					{Name: databricks.DatabricksClientId, Description: "The (oauth) client ID to use when authenticating against the Databricks account.", Mandatory: false},
					{Name: databricks.DatabricksClientSecret, Description: "The (oauth)  client Secret to use when authentic against the Databricks account.", Mandatory: false},
					{Name: databricks.DatabricksUser, Description: "The username to authenticate against the Databricks account.", Mandatory: false},
					{Name: databricks.DatabricksPassword, Description: "The password to authenticate against the Databricks account.", Mandatory: false},
					{Name: databricks.DatabricksDataUsageWindow, Description: "The maximum number of days of usage data to retrieve. Default is 90. Maximum is 90 days.", Mandatory: false},
					{Name: databricks.DatabricksPlatform, Description: "The Databricks platform to connect to (AWS/GCP/Azure).", Mandatory: true},
				},
			},
		},
	)

	if err != nil {
		logger.Error(fmt.Sprintf("error while registering plugins: %s", err.Error()))
	}
}
