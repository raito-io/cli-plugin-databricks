package repo

import (
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/config"
	config2 "github.com/raito-io/cli/base/util/config"

	"cli-plugin-databricks/databricks/constants"
)

type RepositoryCredentials struct {
	Username     string
	Password     string
	ClientId     string
	ClientSecret string
	Token        string

	AzureResourceId   string
	AzureUseMSI       bool
	AzureClientId     string
	AzureClientSecret string
	AzureTenantId     string
	AzureEnvironment  string

	GoogleCredentials    string
	GoogleServiceAccount string

	Host string
}

func (r *RepositoryCredentials) DatabricksConfig() *databricks.Config {
	return &databricks.Config{
		Credentials:          &config.DefaultCredentials{},
		Username:             r.Username,
		Password:             r.Password,
		Token:                r.Token,
		AzureResourceID:      r.AzureResourceId,
		AzureUseMSI:          r.AzureUseMSI,
		AzureClientSecret:    r.AzureClientSecret,
		AzureClientID:        r.AzureClientId,
		AzureTenantID:        r.AzureTenantId,
		AzureEnvironment:     r.AzureEnvironment,
		ClientID:             r.ClientId,
		ClientSecret:         r.ClientSecret,
		GoogleCredentials:    r.GoogleCredentials,
		GoogleServiceAccount: r.GoogleServiceAccount,
		Host:                 r.Host,
	}
}

func GenerateConfig(configParams *config2.ConfigMap) RepositoryCredentials {
	username := configParams.GetString(constants.DatabricksUser)
	password := configParams.GetString(constants.DatabricksPassword)
	clientId := configParams.GetString(constants.DatabricksClientId)
	clientSecret := configParams.GetString(constants.DatabricksClientSecret)
	token := configParams.GetString(constants.DatabricksToken)

	azureUseMSI := configParams.GetBool(constants.DatabricksAzureUseMSI)
	azureClientId := configParams.GetString(constants.DatabricksAzureClientId)
	azureClientSecret := configParams.GetString(constants.DatabricksAzureClientSecret)
	azureTenantId := configParams.GetString(constants.DatabricksAzureTenantID)
	azureEnvironment := configParams.GetString(constants.DatabricksAzureEnvironment)

	googleCredentials := configParams.GetString(constants.DatabricksGoogleCredentials)
	googleServiceAccount := configParams.GetString(constants.DatabricksGoogleServiceAccount)

	return RepositoryCredentials{
		Username:             username,
		Password:             password,
		ClientId:             clientId,
		ClientSecret:         clientSecret,
		Token:                token,
		AzureUseMSI:          azureUseMSI,
		AzureClientId:        azureClientId,
		AzureClientSecret:    azureClientSecret,
		AzureTenantId:        azureTenantId,
		AzureEnvironment:     azureEnvironment,
		GoogleCredentials:    googleCredentials,
		GoogleServiceAccount: googleServiceAccount,
	}
}

type DatabricksUsersFilter struct {
	Username *string
}

type DatabricksServicePrincipalFilter struct {
	ServicePrincipalName *string
}

type DatabricksGroupsFilter struct {
	Groupname *string
}

type ColumnInformation struct {
	Name string
	Type string
	Mask *string
}
