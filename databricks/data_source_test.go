package databricks

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDataSourceSyncer_SyncDataSource(t *testing.T) {
	// Given
	deployment := "test-deployment"
	workspace := "test-workspace"
	dsSyncer, accountMock, workspaceMocks := createDataSourceSyncer(t, deployment)

	dataSourceHandlerMock := mocks.NewSimpleDataSourceObjectHandler(t, 1)
	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			DatabricksAccountId: "AccountId",
			DatabricksUser:      "User",
			DatabricksPassword:  "Password",
		},
	}

	accountMock.EXPECT().ListMetastores(mock.Anything).Return([]catalog.MetastoreInfo{
		{
			Name:        "metastore-1",
			MetastoreId: "metastore-Id1",
		},
	}, nil).Once()

	accountMock.EXPECT().GetWorkspaces(mock.Anything).Return([]Workspace{
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
	}, []Workspace{
		{
			WorkspaceId:     42,
			DeploymentName:  deployment,
			WorkspaceName:   workspace,
			WorkspaceStatus: "RUNNING",
		},
	}).Return(map[string][]string{"metastore-Id1": {deployment}}, nil, nil).Once()

	workspaceMocks[deployment].EXPECT().Ping(mock.Anything).Return(nil).Once()
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

	// When
	err := dsSyncer.SyncDataSource(context.Background(), dataSourceHandlerMock, configMap)

	// Then
	require.NoError(t, err)

	assert.Equal(t, "AccountId", dataSourceHandlerMock.DataSourceName)
	assert.Equal(t, "AccountId", dataSourceHandlerMock.DataSourceFullName)
	require.Len(t, dataSourceHandlerMock.DataObjects, 6)

}

func createDataSourceSyncer(t *testing.T, deployments ...string) (*DataSourceSyncer, *mockDataSourceAccountRepository, map[string]*mockDataSourceWorkspaceRepository) {
	t.Helper()

	mockAccountRepo := newMockDataSourceAccountRepository(t)
	workspaceMockRepos := make(map[string]*mockDataSourceWorkspaceRepository)
	for _, deployment := range deployments {
		workspaceMockRepos[deployment] = newMockDataSourceWorkspaceRepository(t)
	}

	return &DataSourceSyncer{
		accountRepoFactory: func(accountId string, repoCredentials RepositoryCredentials) dataSourceAccountRepository {
			return mockAccountRepo
		},
		workspaceRepoFactory: func(host string, repoCredentials RepositoryCredentials) (dataSourceWorkspaceRepository, error) {
			deploymentRegex := regexp.MustCompile("https://([a-zA-Z0-9_-]*).cloud.databricks.com")

			deployment := deploymentRegex.ReplaceAllString(host, "${1}")

			if workspaceMock, ok := workspaceMockRepos[deployment]; ok {
				return workspaceMock, nil
			}

			return nil, errors.New("no workspace repository")
		},
	}, mockAccountRepo, workspaceMockRepos
}
