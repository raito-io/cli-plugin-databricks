//go:build integration

package it

import (
	"context"
	"os"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"cli-plugin-databricks/databricks"
	"cli-plugin-databricks/databricks/it"
	platform2 "cli-plugin-databricks/databricks/platform"
	"cli-plugin-databricks/databricks/repo"
)

type SqlWarehouseRepositoryTestSuite struct {
	it.DatabricksTestSuite
	repo repo.WarehouseRepository
}

func TestSqlWarehouseRepositoryTestSuite(t *testing.T) {
	testSuite := SqlWarehouseRepositoryTestSuite{}

	config := testSuite.GetConfig()

	pltfrm, err := platform2.DatabricksPlatformString(config.GetString(databricks.DatabricksPlatform))
	require.NoError(t, err)

	host, err := pltfrm.WorkspaceAddress(os.Getenv("DB_TESTING_DEPLOYMENT"))
	require.NoError(t, err)

	credentials := repo.RepositoryCredentials{
		Username:     config.GetString(databricks.DatabricksUser),
		Password:     config.GetString(databricks.DatabricksPassword),
		ClientId:     config.GetString(databricks.DatabricksClientId),
		ClientSecret: config.GetString(databricks.DatabricksClientSecret),
	}

	repository, err := repo.NewWorkspaceRepository(pltfrm, host, config.GetString(databricks.DatabricksAccountId), &credentials)
	require.NoError(t, err)
	require.NoError(t, repository.Ping(context.Background()))

	testSuite.repo = repository.SqlWarehouseRepository(os.Getenv("DB_SQL_WAREHOUSE_ID"))

	suite.Run(t, &testSuite)
}

func (s *SqlWarehouseRepositoryTestSuite) TestSqlWarehouseRepository_ExecuteStatement() {
	response, err := s.repo.ExecuteStatement(context.Background(), s.GetTestingMetastore(), "default", "SELECT 1")

	require.NoError(s.T(), err)
	require.NotNil(s.T(), response)

	assert.Equal(s.T(), sql.StatementStateSucceeded, response.Status.State)
}

func (s *SqlWarehouseRepositoryTestSuite) TestSqlWarehouseRepository_GetTableInformation() {
	tableInformation, err := s.repo.GetTableInformation(context.Background(), "raito_testing", "humanresources", "employee")

	require.NoError(s.T(), err)
	assert.Equal(s.T(), map[string]*repo.ColumnInformation{
		"BirthDate": {
			Name: "BirthDate",
			Type: "date",
		},
		"BusinessEntityID": {
			Name: "BusinessEntityID",
			Type: "decimal(38,0)",
		},
		"CurrentFlag": {
			Name: "CurrentFlag",
			Type: "string",
		},
		"Gender": {
			Name: "Gender",
			Type: "string",
		},
		"HireDate": {
			Name: "HireDate",
			Type: "date",
		},
		"JobTitle": {
			Name: "JobTitle",
			Type: "string",
		},
		"LoginID": {
			Name: "LoginID",
			Type: "decimal(38,0)",
		},
		"MaritalStatus": {
			Name: "MaritalStatus",
			Type: "string",
		},
		"ModifiedDate": {
			Name: "ModifiedDate",
			Type: "timestamp_ntz",
		},
		"NationalIDNumber": {
			Name: "NationalIDNumber",
			Type: "string",
		},
		"OrganizationLevel": {
			Name: "OrganizationLevel",
			Type: "string",
		},
		"OrganizationNode": {
			Name: "OrganizationNode",
			Type: "string",
		},
		"ROWGUID": {
			Name: "ROWGUID",
			Type: "decimal(38,0)",
		},
		"SalariedFlag": {
			Name: "SalariedFlag",
			Type: "string",
		},
		"SickLeaveHours": {
			Name: "SickLeaveHours",
			Type: "decimal(38,0)",
		},
		"VacationHours": {
			Name: "VacationHours",
			Type: "decimal(38,0)",
		},
	}, tableInformation)
}
