//go:build integration

package repo

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/smithy-go/ptr"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/provisioning"
	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/maps"

	"cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/databricks/it"
	platform2 "cli-plugin-databricks/databricks/platform"
	"cli-plugin-databricks/databricks/repo/types"
	"cli-plugin-databricks/databricks/utils"
	"cli-plugin-databricks/utils/array"
)

type AccountRepositoryTestSuite struct {
	it.DatabricksTestSuite
	repo *AccountRepository
}

func TestAccountRepositoryTestSuite(t *testing.T) {
	testSuite := AccountRepositoryTestSuite{}
	config := testSuite.GetConfig()

	platform, err := platform2.DatabricksPlatformString(config.GetString(constants.DatabricksPlatform))
	if err != nil {
		t.Fatalf("failed to parse platform: %s", err.Error())
	}

	credentials := types.RepositoryCredentials{
		Username:     config.GetString(constants.DatabricksUser),
		Password:     config.GetString(constants.DatabricksPassword),
		ClientId:     config.GetString(constants.DatabricksClientId),
		ClientSecret: config.GetString(constants.DatabricksClientSecret),
	}

	testSuite.repo, err = NewAccountRepository(platform, &credentials, config.GetString(constants.DatabricksAccountId))
	if err != nil {
		t.Fatalf("failed to create account repository: %s", err.Error())
	}

	suite.Run(t, &testSuite)
}

func (s *AccountRepositoryTestSuite) TestAccountRepository_ListMetastores() {
	metastores, err := s.repo.ListMetastores(context.Background())
	require.NoError(s.T(), err)

	metastoreIds := array.Map(metastores, func(m *catalog.MetastoreInfo) string { return m.MetastoreId })

	s.Contains(metastoreIds, s.GetTestingMetastore())
}

func (s *AccountRepositoryTestSuite) TestAccountRepository_GetWorkspacesForMetastore() {
	workspaces, err := s.repo.GetWorkspacesForMetastore(context.Background(), s.GetTestingMetastore())
	require.NoError(s.T(), err)

	s.NotEmpty(workspaces.WorkspaceIds)
}

func (s *AccountRepositoryTestSuite) TestAccountRepository_GetWorkspaceMap() {
	// Given
	metastores, err := s.repo.ListMetastores(context.Background())
	require.NoError(s.T(), err)

	var metastore *catalog.MetastoreInfo
	for _, m := range metastores {
		if m.MetastoreId == s.GetTestingMetastore() {
			metastore = &m
			break
		}
	}

	require.NotNil(s.T(), metastore)

	metastoreAssignment, err := s.repo.GetWorkspacesForMetastore(context.Background(), metastore.MetastoreId)
	require.NoError(s.T(), err)

	workspaces, err := s.repo.GetWorkspaces(context.Background())
	require.NoError(s.T(), err)

	var workspace *provisioning.Workspace
	for _, w := range workspaces {
		for _, a := range metastoreAssignment.WorkspaceIds {
			if a == w.WorkspaceId {
				workspace = &w
				break
			}
		}
	}

	require.NotNil(s.T(), workspace)

	// When
	metatoreToWorkspaceMap, workspaceToMetastoreMap, err := s.repo.GetWorkspaceMap(context.Background(), []catalog.MetastoreInfo{*metastore}, []provisioning.Workspace{*workspace})
	require.NoError(s.T(), err)

	// Then
	require.Contains(s.T(), maps.Keys(metatoreToWorkspaceMap), metastore.MetastoreId)
	require.NotEmpty(s.T(), metatoreToWorkspaceMap[metastore.MetastoreId])

	for _, w := range metatoreToWorkspaceMap[metastore.MetastoreId] {
		s.Equal(metastore.MetastoreId, workspaceToMetastoreMap[w.DeploymentName])
	}
}

func (s *AccountRepositoryTestSuite) TestAccountRepository_GetWorkspaces() {
	workspaces, err := s.repo.GetWorkspaces(context.Background())
	require.NoError(s.T(), err)

	s.NotEmpty(workspaces)
}

func (s *AccountRepositoryTestSuite) TestAccountRepository_ListUsers() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	userChannel := s.repo.ListUsers(ctx)

	var users []string

	for channelItem := range userChannel {
		if channelItem.HasError() {
			s.Fail(channelItem.Error().Error())

			return
		} else {
			users = append(users, channelItem.Item().DisplayName)
		}
	}

	s.NotEmpty(users)
}

func (s *AccountRepositoryTestSuite) TestAccountRepository_ListServicePrincipals() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	servicePrincipalChannel := s.repo.ListServicePrincipals(ctx)

	var servicePrincipals []string

	for channelItem := range servicePrincipalChannel {
		if channelItem.HasError() {
			s.Fail(channelItem.Error().Error())
		} else {
			servicePrincipals = append(servicePrincipals, channelItem.Item().DisplayName)
		}
	}
	s.Contains(servicePrincipals, "RaitoSync")
}

func (s *AccountRepositoryTestSuite) TestAccountRepository_ListGroups() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	groupChannel := s.repo.ListGroups(ctx)

	var groups []string

	for channelItem := range groupChannel {
		if channelItem.HasError() {
			s.Fail(channelItem.Error().Error())
		} else {
			groups = append(groups, channelItem.Item().DisplayName)
		}
	}

	s.Contains(groups, "HUMAN_RESOURCES")
	s.Contains(groups, "SALES")
	s.Contains(groups, "MARKETING")
	s.Contains(groups, "SALES_ANALYSIS")
	s.Contains(groups, "FINANCE")
}

func (s *AccountRepositoryTestSuite) TestAccountRepository_ListWorkspaceAssignments() {
	// Given
	metastores, err := s.repo.ListMetastores(context.Background())
	require.NoError(s.T(), err)

	var metastore *catalog.MetastoreInfo
	for _, m := range metastores {
		if m.MetastoreId == s.GetTestingMetastore() {
			metastore = &m
			break
		}
	}

	metastoreAssignment, err := s.repo.GetWorkspacesForMetastore(context.Background(), metastore.MetastoreId)
	require.NoError(s.T(), err)

	workspaces, err := s.repo.GetWorkspaces(context.Background())
	require.NoError(s.T(), err)

	var workspace *provisioning.Workspace
	for _, w := range workspaces {
		for _, a := range metastoreAssignment.WorkspaceIds {
			if a == w.WorkspaceId {
				workspace = &w
				break
			}
		}
	}

	require.NotNil(s.T(), workspace)

	metatoreToWorkspaceMap, _, err := s.repo.GetWorkspaceMap(context.Background(), []catalog.MetastoreInfo{*metastore}, []provisioning.Workspace{*workspace})
	require.NoError(s.T(), err)
	require.NotEmpty(s.T(), metatoreToWorkspaceMap[metastore.MetastoreId])
	workspaceInMetastore := metatoreToWorkspaceMap[metastore.MetastoreId][0]

	var workspaceId int64
	for _, w := range workspaces {
		if w.DeploymentName == workspaceInMetastore.DeploymentName {
			workspaceId = w.WorkspaceId
			break
		}
	}

	// When
	workspaceAssignments, err := s.repo.ListWorkspaceAssignments(context.Background(), workspaceId)
	require.NoError(s.T(), err)

	// Then
	s.NotEmpty(workspaceAssignments)
}

func (s *AccountRepositoryTestSuite) TestAccountRepository_UpdateWorkspaceAssignment() {
	s.T().Skip("Skip tests for know")
}

type WorkspaceRepositoryTestSuite struct {
	it.DatabricksTestSuite
	repo *WorkspaceRepository
}

func TestWorkspaceRepositoryTestSuite(t *testing.T) {
	testSuite := WorkspaceRepositoryTestSuite{}
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

	repository, err := NewWorkspaceRepository(repoCredentials, workspace.WorkspaceId)
	require.NoError(t, err)
	require.NoError(t, repository.Ping(context.Background()))

	testSuite.repo = repository

	suite.Run(t, &testSuite)
}

func (s *WorkspaceRepositoryTestSuite) TestWorkspaceRepository_ListCatalogs() {
	var catalogs []catalog.CatalogInfo

	catalogChannel := s.repo.ListCatalogs(context.Background())

	for catalogItem := range catalogChannel {
		if catalogItem.HasError() {
			s.Fail(catalogItem.Error().Error())
		} else {
			catalogs = append(catalogs, catalogItem.Item())
		}
	}

	catalogNames := make([]string, 0, len(catalogs))
	for _, c := range catalogs {
		catalogNames = append(catalogNames, c.Name)
	}

	assert.Contains(s.T(), catalogNames, "raito_testing")
}

func (s *WorkspaceRepositoryTestSuite) TestWorkspaceRepository_ListSchemas() {
	var schemas []catalog.SchemaInfo

	schemaChannel := s.repo.ListSchemas(context.Background(), "raito_testing")

	for schema := range schemaChannel {
		if schema.HasError() {
			s.Fail(schema.Error().Error())
		} else {
			schemas = append(schemas, schema.Item())
		}
	}

	schemaNames := make([]string, 0, len(schemas))
	for _, s := range schemas {
		schemaNames = append(schemaNames, s.Name)
	}

	assert.Contains(s.T(), schemaNames, "humanresources")
	assert.Contains(s.T(), schemaNames, "person")
}

func (s *WorkspaceRepositoryTestSuite) TestWorkspaceRepository_ListTables() {
	var tables []catalog.TableInfo

	tableChannel := s.repo.ListTables(context.Background(), "raito_testing", "humanresources")

	for table := range tableChannel {
		if table.HasError() {
			s.Fail(table.Error().Error())
		} else {
			tables = append(tables, table.Item())
		}
	}

	tableNames := make([]string, 0, len(tables))
	for _, t := range tables {
		tableNames = append(tableNames, t.Name)
	}

	assert.ElementsMatch(s.T(), tableNames, []string{"department", "department_view", "employee", "employeedepartmenthistory", "jobcandidate", "shift"})
}

func (s *WorkspaceRepositoryTestSuite) TestWorkspaceRepository_GetTable() {
	table, err := s.repo.GetTable(context.Background(), "raito_testing", "humanresources", "employee")
	require.NoError(s.T(), err)

	assert.Equal(s.T(), "employee", table.Name)
	assert.Equal(s.T(), s.GetTestingMetastore(), table.MetastoreId)
	assert.Equal(s.T(), "raito_testing", table.CatalogName)
	assert.Equal(s.T(), "raito_testing.humanresources.employee", table.FullName)

	columnNames := make([]string, 0, len(table.Columns))
	for _, c := range table.Columns {
		columnNames = append(columnNames, c.Name)
	}

	assert.ElementsMatch(s.T(), columnNames, []string{"BirthDate", "BusinessEntityID", "CurrentFlag", "Gender", "HireDate",
		"JobTitle", "LoginID", "MaritalStatus", "ModifiedDate", "NationalIDNumber", "OrganizationLevel", "OrganizationNode", "ROWGUID",
		"SalariedFlag", "SickLeaveHours", "VacationHours",
	})
}

func (s *WorkspaceRepositoryTestSuite) TestWorkspaceRepository_ListFunctions() {
	var functions []catalog.FunctionInfo

	functionChannel := s.repo.ListFunctions(context.Background(), "raito_testing", "humanresources")

	for function := range functionChannel {
		if function.HasError() {
			s.Fail(function.Error().Error())
		} else {
			functions = append(functions, function.Item())
		}
	}

	functionNames := make([]string, 0, len(functions))
	for _, f := range functions {
		functionNames = append(functionNames, f.Name)
	}

	assert.Empty(s.T(), functionNames) // Currently TF cannot create functions
}

func (s *WorkspaceRepositoryTestSuite) TestWorkspaceRepository_GetPermissionsOnResource() {
	permissions, err := s.repo.GetPermissionsOnResource(context.Background(), catalog.SecurableTypeTable, "raito_testing.humanresources.employee")
	require.NoError(s.T(), err)

	assert.Contains(s.T(), permissions.PrivilegeAssignments, catalog.PrivilegeAssignment{
		Principal:       "HUMAN_RESOURCES_TF_2",
		Privileges:      []catalog.Privilege{catalog.PrivilegeModify, catalog.PrivilegeSelect},
		ForceSendFields: []string{"Principal"},
	})
}

func (s *WorkspaceRepositoryTestSuite) TestWorkspaceRepository_SetPermissionsOnResource() {
	err := s.repo.SetPermissionsOnResource(context.Background(), catalog.SecurableTypeTable, "raito_testing.humanresources.employee",
		catalog.PermissionsChange{
			Add:             []catalog.Privilege{catalog.PrivilegeSelect},
			Principal:       "SALES",
			Remove:          nil,
			ForceSendFields: nil,
		})
	require.NoError(s.T(), err)

	permissions, err := s.repo.GetPermissionsOnResource(context.Background(), catalog.SecurableTypeTable, "raito_testing.humanresources.employee")
	require.NoError(s.T(), err)

	assert.Contains(s.T(), permissions.PrivilegeAssignments, catalog.PrivilegeAssignment{
		Principal:       "SALES",
		Privileges:      []catalog.Privilege{catalog.PrivilegeSelect},
		ForceSendFields: []string{"Principal"},
	})

	err = s.repo.SetPermissionsOnResource(context.Background(), catalog.SecurableTypeTable, "raito_testing.humanresources.employee",
		catalog.PermissionsChange{
			Add:             nil,
			Principal:       "SALES",
			Remove:          []catalog.Privilege{catalog.PrivilegeSelect},
			ForceSendFields: nil,
		})
	require.NoError(s.T(), err)

	permissions, err = s.repo.GetPermissionsOnResource(context.Background(), catalog.SecurableTypeTable, "raito_testing.humanresources.employee")
	require.NoError(s.T(), err)

	assert.NotContains(s.T(), permissions.PrivilegeAssignments, catalog.PrivilegeAssignment{
		Principal:       "SALES",
		Privileges:      []catalog.Privilege{catalog.PrivilegeSelect},
		ForceSendFields: []string{"Principal"},
	})
}

func (s *WorkspaceRepositoryTestSuite) TestWorkspaceRepository_GetOwner() {
	tests := []struct {
		name          string
		securableType catalog.SecurableType
		fullName      string
		expectedOwner *string
	}{
		{
			name:          "catalog",
			securableType: catalog.SecurableTypeCatalog,
			fullName:      "raito_testing",
			expectedOwner: ptr.String("master_catalog_owner"),
		},
		{
			name:          "schema",
			securableType: catalog.SecurableTypeSchema,
			fullName:      "raito_testing.humanresources",
			expectedOwner: nil,
		},
		{
			name:          "table",
			securableType: catalog.SecurableTypeTable,
			fullName:      "raito_testing.humanresources.employee",
			expectedOwner: nil,
		},
		// TODO function
	}

	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			owner, err := s.repo.GetOwner(context.Background(), tt.securableType, tt.fullName)
			require.NoError(t, err)

			if tt.expectedOwner != nil {
				assert.Equal(t, *tt.expectedOwner, owner)
			}
		})
	}
}

func (s *WorkspaceRepositoryTestSuite) TestWorkspaceRepository_QueryHistory() {
	hasQueryLogs := false

	querLogHandler := func(ctx context.Context, queryLog *sql.QueryInfo) error {
		hasQueryLogs = true
		return nil
	}

	err := s.repo.QueryHistory(context.Background(), nil, querLogHandler)
	require.NoError(s.T(), err)

	assert.True(s.T(), hasQueryLogs)
}
