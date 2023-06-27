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
		&info.InfoImpl{
			Info: &plugin.PluginInfo{
				Name:    "Databricks",
				Version: plugin.ParseVersion(version),
				Parameters: []*plugin.ParameterInfo{
					{Name: databricks.DatabricksAccountId, Description: "The Databricks account to connect to.", Mandatory: true},
					{Name: databricks.DatabricksUser, Description: "The username to authenticate against the Databricks account.", Mandatory: true},
					{Name: databricks.DatabricksPassword, Description: "The password to authenticate against the Databricks account.", Mandatory: true},
				},
			},
		},
	)

	if err != nil {
		logger.Error(fmt.Sprintf("error while registering plugins: %s", err.Error()))
	}

	//accountRepo := databricks.NewAccountRepository("ruben@raito.io", "@Ds7r7QyT8OFtiYj", "55b37de5-81ce-4a41-93a9-2c776060c1fc")
	//result, err := accountRepo.GetWorkspaces(context.Background())
	//if err != nil {
	//	logger.Error(fmt.Sprintf("error while getting workspaces: %s", err.Error()))
	//	return
	//}
	//
	//fmt.Printf("%+v\n", result)
}
