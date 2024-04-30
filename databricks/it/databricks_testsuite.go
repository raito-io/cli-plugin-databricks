//go:build integration

package it

import (
	"os"
	"sync"

	"github.com/raito-io/cli/base/util/config"
	"github.com/stretchr/testify/suite"

	"cli-plugin-databricks/databricks"
)

var (
	dbAccountId        string
	dbUsername         string
	dbPassword         string
	dbClientID         string
	dbClientSecret     string
	dbSqlWarehouses    string
	dbPlatform         string
	dbTestingMetastore string
	lock               = &sync.Mutex{}
)

func readDatabaseConfig() *config.ConfigMap {
	lock.Lock()
	defer lock.Unlock()

	if dbAccountId == "" {
		dbAccountId = os.Getenv("DB_ACCOUNT_ID")
		dbUsername = os.Getenv("DB_USERNAME")
		dbPassword = os.Getenv("DB_PASSWORD")
		dbClientID = os.Getenv("DB_CLIENT_ID")
		dbClientSecret = os.Getenv("DB_CLIENT_SECRET")
		dbSqlWarehouses = os.Getenv("DB_SQL_WAREHOUSES")
		dbPlatform = os.Getenv("DB_PLATFORM")
	}

	return &config.ConfigMap{
		Parameters: map[string]string{
			databricks.DatabricksAccountId:     dbAccountId,
			databricks.DatabricksUser:          dbUsername,
			databricks.DatabricksPassword:      dbPassword,
			databricks.DatabricksClientId:      dbClientID,
			databricks.DatabricksClientSecret:  dbClientSecret,
			databricks.DatabricksSqlWarehouses: dbSqlWarehouses,
			databricks.DatabricksPlatform:      dbPlatform,
		},
	}
}

type DatabricksTestSuite struct {
	suite.Suite
}

func (s *DatabricksTestSuite) GetConfig() *config.ConfigMap {
	return readDatabaseConfig()
}

func (s *DatabricksTestSuite) GetTestingMetastore() string {
	lock.Lock()
	defer lock.Unlock()

	if dbTestingMetastore == "" {
		dbTestingMetastore = os.Getenv("DB_TESTING_METASTORE")
	}

	return dbTestingMetastore
}
