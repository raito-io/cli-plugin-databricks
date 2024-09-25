//go:build integration

package repo

import (
	"context"
	"os"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/databricks/it"
	platform2 "cli-plugin-databricks/databricks/platform"
	"cli-plugin-databricks/databricks/repo/types"
	"cli-plugin-databricks/databricks/utils"
)

const _catalog = "raito_testing"

type SqlWarehouseRepositoryTestSuite struct {
	it.DatabricksTestSuite
	repo WarehouseRepository
}

func TestSqlWarehouseRepositoryTestSuite(t *testing.T) {
	testSuite := SqlWarehouseRepositoryTestSuite{}

	config := testSuite.GetConfig()

	pltfrm, err := platform2.DatabricksPlatformString(config.GetString(constants.DatabricksPlatform))
	require.NoError(t, err)

	credentials := types.RepositoryCredentials{
		Username:     config.GetString(constants.DatabricksUser),
		Password:     config.GetString(constants.DatabricksPassword),
		ClientId:     config.GetString(constants.DatabricksClientId),
		ClientSecret: config.GetString(constants.DatabricksClientSecret),
	}

	accountRepo, err := NewAccountRepository(pltfrm, &credentials, config.GetString(constants.DatabricksAccountId))
	require.NoError(t, err)

	workspace, err := accountRepo.GetWorkspaceByName(context.Background(), os.Getenv("DB_TESTING_WORKSPACE"))
	require.NoError(t, err)

	repoCredentials, err := utils.InitializeWorkspaceRepoCredentials(credentials, pltfrm, workspace)
	require.NoError(t, err)

	repository, err := NewWorkspaceRepository(repoCredentials)
	require.NoError(t, err)
	require.NoError(t, repository.Ping(context.Background()))

	me, err := repository.Me(context.Background())
	require.NoError(t, err)

	err = repository.SetPermissionsOnResource(context.Background(), catalog.SecurableTypeCatalog, _catalog, catalog.PermissionsChange{
		Add:             []catalog.Privilege{catalog.PrivilegeCreateFunction, catalog.PrivilegeSelect, catalog.PrivilegeUseCatalog, catalog.PrivilegeUseSchema, catalog.PrivilegeModify},
		Principal:       me.UserName,
		Remove:          nil,
		ForceSendFields: nil,
	})
	require.NoError(t, err)

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
	assert.Equal(s.T(), map[string]*types.ColumnInformation{
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

func (s *SqlWarehouseRepositoryTestSuite) TestSqlWarehouseRepository_Mask() {
	ctx := context.Background()
	schema := "humanresources"

	_, err := s.repo.ExecuteStatement(ctx, _catalog, schema, "CREATE FUNCTION IF NOT EXISTS mask_ssn(ssn STRING) RETURN CASE WHEN is_member('HUMAN_RESOURCES') THEN ssn ELSE '***-**-****' END;")
	require.NoError(s.T(), err)

	defer func() {
		err = s.repo.DropFunction(ctx, _catalog, schema, "mask_ssn")
		assert.NoError(s.T(), err)
	}()

	err = s.repo.SetMask(ctx, _catalog, schema, "employee", "NationalIDNumber", "mask_ssn")
	require.NoError(s.T(), err)

	defer func() {
		err = s.repo.DropMask(ctx, _catalog, schema, "employee", "NationalIDNumber")
		assert.NoError(s.T(), err)
	}()
}

func (s *SqlWarehouseRepositoryTestSuite) TestSqlWarehouseRepository_Filter() {
	ctx := context.Background()
	catalog := "raito_testing"
	schema := "person"

	_, err := s.repo.ExecuteStatement(ctx, catalog, schema, "CREATE FUNCTION IF NOT EXISTS filter_state(id decimal(38,0)) RETURN IF(IS_ACCOUNT_GROUP_MEMBER('admin'), true, id > 10)")
	require.NoError(s.T(), err)

	defer func() {
		err = s.repo.DropFunction(ctx, catalog, schema, "filter_state")
		assert.NoError(s.T(), err)
	}()

	err = s.repo.SetRowFilter(ctx, catalog, schema, "stateprovince", "filter_state", []string{"StateProvinceID"})
	require.NoError(s.T(), err)

	defer func() {
		err = s.repo.DropRowFilter(ctx, catalog, schema, "stateprovince")
		assert.NoError(s.T(), err)
	}()
}

func (s *SqlWarehouseRepositoryTestSuite) TestSqlWarehouseRepository_GetTags() {
	ctx := context.Background()
	catalog := "raito_testing"

	err := s.repo.GetTags(ctx, catalog, func(ctx context.Context, fullName string, key string, value string) error {
		s.Fail("Should not be called") // Currently we cannot set tags via TF. So we are not expecting any tags but we test if the queries ran successfully

		return nil
	})

	require.NoError(s.T(), err)
}
