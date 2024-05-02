package databricks

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/provisioning"
	ds "github.com/raito-io/cli/base/data_source"

	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/databricks/platform"
	"cli-plugin-databricks/databricks/repo"
)

func TestDataSourceSyncer_SyncDataSource(t *testing.T) {
	// Given
	deployment := "test-deployment"
	workspace := "test-workspace"
	dsSyncer, accountMock, workspaceMocks := createDataSourceSyncer(t, deployment)

	dataSourceHandlerMock := mocks.NewSimpleDataSourceObjectHandler(t, 1)
	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			constants.DatabricksAccountId: "AccountId",
			constants.DatabricksUser:      "User",
			constants.DatabricksPassword:  "Password",
			constants.DatabricksPlatform:  "AWS",
		},
	}

	accountMock.EXPECT().ListMetastores(mock.Anything).Return([]catalog.MetastoreInfo{
		{
			Name:        "metastore-1",
			MetastoreId: "metastore-Id1",
		},
	}, nil).Once()

	accountMock.EXPECT().GetWorkspaces(mock.Anything).Return([]provisioning.Workspace{
		{
			WorkspaceId:     42,
			DeploymentName:  deployment,
			WorkspaceName:   workspace,
			WorkspaceStatus: "RUNNING",
		},
	}, nil).Once()

	accountMock.EXPECT().GetWorkspaceMap(mock.Anything, []catalog.MetastoreInfo{
		{
			Name:        "metastore-1",
			MetastoreId: "metastore-Id1",
		},
	}, []provisioning.Workspace{
		{
			WorkspaceId:     42,
			DeploymentName:  deployment,
			WorkspaceName:   workspace,
			WorkspaceStatus: "RUNNING",
		},
	}).Return(map[string][]string{"metastore-Id1": {deployment}}, nil, nil).Twice()

	workspaceMocks[deployment].EXPECT().Ping(mock.Anything).Return(nil).Twice()
	workspaceMocks[deployment].EXPECT().ListCatalogs(mock.Anything).Return([]catalog.CatalogInfo{
		{
			Name:        "catalog-1",
			MetastoreId: "metastore-Id1",
			Comment:     "comment on catalog-1",
		},
	}, nil).Once()
	workspaceMocks[deployment].EXPECT().ListSchemas(mock.Anything, "catalog-1").Return([]catalog.SchemaInfo{
		{
			Name:        "schema-1",
			MetastoreId: "metastore-Id1",
			CatalogName: "catalog-1",
			Comment:     "comment on schema-1",
			FullName:    "catalog-1.schema-1",
		},
	}, nil).Once()
	workspaceMocks[deployment].EXPECT().ListTables(mock.Anything, "catalog-1", "schema-1").Return([]catalog.TableInfo{
		{
			Name:        "table-1",
			MetastoreId: "metastore-Id1",
			Comment:     "comment on table-1",
			FullName:    "catalog-1.schema-1.table-1",
			TableType:   catalog.TableTypeManaged,
			Columns: []catalog.ColumnInfo{
				{
					Name:    "column-1",
					Comment: "comment on column-1",
				},
			},
		},
	}, nil)
	workspaceMocks[deployment].EXPECT().ListFunctions(mock.Anything, "catalog-1", "schema-1").Return([]catalog.FunctionInfo{
		{
			Name:        "function-1",
			MetastoreId: "metastore-Id1",
			Comment:     "comment on function-1",
			FullName:    "catalog-1.schema-1.function-1",
			CatalogName: "catalog-1",
		},
	}, nil)

	// When
	err := dsSyncer.SyncDataSource(context.Background(), dataSourceHandlerMock, &ds.DataSourceSyncConfig{ConfigMap: configMap})

	// Then
	require.NoError(t, err)

	assert.Equal(t, "AccountId", dataSourceHandlerMock.DataSourceName)
	assert.Equal(t, "AccountId", dataSourceHandlerMock.DataSourceFullName)
	require.Len(t, dataSourceHandlerMock.DataObjects, 7)

}

func TestDataSourceSyncer_SyncDataSource_Partial(t *testing.T) {
	// Given
	deployment := "test-deployment"
	workspace := "test-workspace"
	dsSyncer, accountMock, workspaceMocks := createDataSourceSyncer(t, deployment)

	dataSourceHandlerMock := mocks.NewSimpleDataSourceObjectHandler(t, 1)
	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			constants.DatabricksAccountId: "AccountId",
			constants.DatabricksUser:      "User",
			constants.DatabricksPassword:  "Password",
			constants.DatabricksPlatform:  "AWS",
		},
	}

	accountMock.EXPECT().ListMetastores(mock.Anything).Return([]catalog.MetastoreInfo{
		{
			Name:        "metastore-1",
			MetastoreId: "metastore-Id1",
		},
	}, nil).Once()

	accountMock.EXPECT().GetWorkspaces(mock.Anything).Return([]provisioning.Workspace{
		{
			WorkspaceId:     42,
			DeploymentName:  deployment,
			WorkspaceName:   workspace,
			WorkspaceStatus: "RUNNING",
		},
	}, nil).Once()

	accountMock.EXPECT().GetWorkspaceMap(mock.Anything, []catalog.MetastoreInfo{
		{
			Name:        "metastore-1",
			MetastoreId: "metastore-Id1",
		},
	}, []provisioning.Workspace{
		{
			WorkspaceId:     42,
			DeploymentName:  deployment,
			WorkspaceName:   workspace,
			WorkspaceStatus: "RUNNING",
		},
	}).Return(map[string][]string{"metastore-Id1": {deployment}}, nil, nil).Twice()

	workspaceMocks[deployment].EXPECT().Ping(mock.Anything).Return(nil).Twice()
	workspaceMocks[deployment].EXPECT().ListCatalogs(mock.Anything).Return([]catalog.CatalogInfo{
		{
			Name:        "catalog-1",
			MetastoreId: "metastore-Id1",
			Comment:     "comment on catalog-1",
		},
		{
			Name:        "catalog-2",
			MetastoreId: "metastore-Id2",
			Comment:     "comment on catalog-2",
		},
	}, nil).Once()
	workspaceMocks[deployment].EXPECT().ListSchemas(mock.Anything, "catalog-1").Return([]catalog.SchemaInfo{
		{
			Name:        "schema-1",
			MetastoreId: "metastore-Id1",
			CatalogName: "catalog-1",
			Comment:     "comment on schema-1",
			FullName:    "catalog-1.schema-1",
		},
		{
			Name:        "schema-2",
			MetastoreId: "metastore-Id2",
			CatalogName: "catalog-1",
			Comment:     "comment on schema-2",
			FullName:    "catalog-1.schema-2",
		},
	}, nil).Once()
	workspaceMocks[deployment].EXPECT().ListTables(mock.Anything, "catalog-1", "schema-1").Return([]catalog.TableInfo{
		{
			Name:        "table-1",
			MetastoreId: "metastore-Id1",
			Comment:     "comment on table-1",
			FullName:    "catalog-1.schema-1.table-1",
			TableType:   catalog.TableTypeManaged,
			Columns: []catalog.ColumnInfo{
				{
					Name:    "column-1",
					Comment: "comment on column-1",
				},
			},
		},
	}, nil)
	workspaceMocks[deployment].EXPECT().ListFunctions(mock.Anything, "catalog-1", "schema-1").Return([]catalog.FunctionInfo{
		{
			Name:        "function-1",
			MetastoreId: "metastore-Id1",
			Comment:     "comment on function-1",
			FullName:    "catalog-1.schema-1.function-1",
			CatalogName: "catalog-1",
		},
	}, nil)

	// When
	err := dsSyncer.SyncDataSource(context.Background(), dataSourceHandlerMock, &ds.DataSourceSyncConfig{ConfigMap: configMap, DataObjectParent: "metastore-Id1.catalog-1.schema-1", DataObjectExcludes: []string{"function-1"}})

	// Then
	require.NoError(t, err)

	assert.Equal(t, "AccountId", dataSourceHandlerMock.DataSourceName)
	assert.Equal(t, "AccountId", dataSourceHandlerMock.DataSourceFullName)
	require.Len(t, dataSourceHandlerMock.DataObjects, 2)
	assert.Equal(t, "metastore-Id1.catalog-1.schema-1.table-1", dataSourceHandlerMock.DataObjects[0].FullName)
	assert.Equal(t, "metastore-Id1.catalog-1.schema-1.table-1.column-1", dataSourceHandlerMock.DataObjects[1].FullName)

}

func createDataSourceSyncer(t *testing.T, deployments ...string) (*DataSourceSyncer, *mockAccountRepository, map[string]*mockDataSourceWorkspaceRepository) {
	t.Helper()

	mockAccountRepo := newMockAccountRepository(t)
	workspaceMockRepos := make(map[string]*mockDataSourceWorkspaceRepository)
	for _, deployment := range deployments {
		workspaceMockRepos[deployment] = newMockDataSourceWorkspaceRepository(t)
	}

	return &DataSourceSyncer{
		accountRepoFactory: func(pltfrm platform.DatabricksPlatform, accountId string, repoCredentials *repo.RepositoryCredentials) (accountRepository, error) {
			return mockAccountRepo, nil
		},
		workspaceRepoFactory: func(host string, repoCredentials *repo.RepositoryCredentials) (dataSourceWorkspaceRepository, error) {
			deploymentRegex := regexp.MustCompile("https://([a-zA-Z0-9_-]*).cloud.databricks.com")

			deployment := deploymentRegex.ReplaceAllString(host, "${1}")

			if workspaceMock, ok := workspaceMockRepos[deployment]; ok {
				return workspaceMock, nil
			}

			return nil, errors.New("no workspace repository")
		},
	}, mockAccountRepo, workspaceMockRepos
}
