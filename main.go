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
)

var version = "0.0.0"

var logger hclog.Logger

func main() {
	logger = base.Logger()
	logger.SetLevel(hclog.Debug)

	err := base.RegisterPlugins(
		wrappers.DataSourceSync(databricks.NewDataSourceSyncer()),
		wrappers.IdentityStoreSync(databricks.NewIdentityStoreSyncer()),
		wrappers.DataAccessSync(databricks.NewAccessSyncer(), access_provider.WithAccessProviderExportWhoList(access_provider.AccessProviderExportWhoList_ACCESSPROVIDER_EXPORT_WHO_LIST_NATIVE_GROUPS_INHERITED, access_provider.AccessProviderExportWhoList_ACCESSPROVIDER_EXPORT_WHO_LIST_USERS_INHERITED_NATIVE_GROUPS_EXCLUDED)),
		wrappers.DataUsageSync(databricks.NewDataUsageSyncer()),
		&info.InfoImpl{
			Info: &plugin.PluginInfo{
				Name:    "Databricks",
				Version: plugin.ParseVersion(version),
				Parameters: []*plugin.ParameterInfo{
					{Name: databricks.DatabricksAccountId, Description: "The Databricks account to connect to.", Mandatory: true},
					{Name: databricks.DatabricksUser, Description: "The username to authenticate against the Databricks account.", Mandatory: true},
					{Name: databricks.DatabricksPassword, Description: "The password to authenticate against the Databricks account.", Mandatory: true},
					{Name: databricks.DatabricksDataUsageWindow, Description: "The maximum number of days of usage data to retrieve. Default is 14. Maximum is 90 days.", Mandatory: false},
				},
			},
		},
	)

	if err != nil {
		logger.Error(fmt.Sprintf("error while registering plugins: %s", err.Error()))
	}
}
