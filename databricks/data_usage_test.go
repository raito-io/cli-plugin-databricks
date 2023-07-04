package databricks

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/raito-io/cli/base/access_provider/sync_from_target"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/data_usage"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDataUsageSyncer_SyncDataUsage(t *testing.T) {
	deployment := "deployment1"
	metastoreId := "metastoreId1"
	duSyncer, accountRepo, workspaceRepoMap := createDataUsageSyncer(t, deployment)

	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			DatabricksAccountId: "AccountId",
			DatabricksUser:      "User",
			DatabricksPassword:  "Password",
		},
	}

	fileCreatorMock := mocks.NewSimpleDataUsageStatementHandler(t)

	metastores := []catalog.MetastoreInfo{
		{
			Name:        "metastore1",
			MetastoreId: "metastoreId1",
		},
	}

	workspaces := []Workspace{
		{
			WorkspaceId:     42,
			DeploymentName:  deployment,
			WorkspaceName:   "workspace1",
			WorkspaceStatus: "RUNNING",
		},
	}

	accountRepo.EXPECT().ListMetastores(mock.Anything).Return(metastores, nil).Once()

	accountRepo.EXPECT().GetWorkspaces(mock.Anything).Return(workspaces, nil).Once()

	accountRepo.EXPECT().GetWorkspaceMap(mock.Anything, metastores, workspaces).Return(nil, map[string]string{deployment: metastoreId}, nil).Once()

	workspaceRepoMap[deployment].EXPECT().ListCatalogs(mock.Anything).Return([]catalog.CatalogInfo{
		{
			Name:        "catalog1",
			MetastoreId: metastoreId,
		},
	}, nil).Once()

	workspaceRepoMap[deployment].EXPECT().ListSchemas(mock.Anything, "catalog1").Return([]catalog.SchemaInfo{
		{
			Name:        "schema1",
			FullName:    "catalog1.schema1",
			MetastoreId: metastoreId,
			CatalogName: "catalog1",
		},
	}, nil).Once()

	workspaceRepoMap[deployment].EXPECT().ListTables(mock.Anything, "catalog1", "schema1").Return([]catalog.TableInfo{
		{
			Name:        "table1",
			FullName:    "catalog1.schema1.table1",
			MetastoreId: metastoreId,
			CatalogName: "catalog1",
			SchemaName:  "schema1",
		},
	}, nil).Once()

	startTime := time.Now().Add(time.Hour)
	endTime := time.Now()

	workspaceRepoMap[deployment].EXPECT().QueryHistory(mock.Anything, mock.Anything).Return([]sql.QueryInfo{
		{
			QueryText: "SELECT * FROM `catalog1`.`schema1`.`table1`",
			Metrics: &sql.QueryMetrics{
				RowsProducedCount: 1,
				ReadBytes:         2,
			},
			QueryStartTimeMs: int(startTime.UnixMilli()),
			QueryEndTimeMs:   int(endTime.UnixMilli()),
			Status:           sql.QueryStatusFinished,
			UserName:         "ruben@raito.io",
			QueryId:          "queryId1",
			StatementType:    sql.QueryStatementTypeSelect,
		},
	}, nil).Once()

	// When
	err := duSyncer.SyncDataUsage(context.Background(), fileCreatorMock, configMap)

	require.NoError(t, err)

	assert.Len(t, fileCreatorMock.Statements, 1)
	assert.ElementsMatch(t, fileCreatorMock.Statements, []data_usage.Statement{
		{
			ExternalId: "queryId1",
			Status:     "FINISHED",
			Rows:       1,
			Bytes:      2,
			EndTime:    endTime.Unix(),
			StartTime:  startTime.Unix(),
			User:       "ruben@raito.io",
			Success:    true,
			AccessedDataObjects: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.table1",
						Type:     data_source.Table,
					},
					Permissions: []string{"SELECT"},
				},
			},
		},
	})
}

func createDataUsageSyncer(t *testing.T, deployments ...string) (*DataUsageSyncer, *mockDataUsageAccountRepository, map[string]*mockDataUsageWorkspaceRepository) {
	t.Helper()

	mockAccountRepo := newMockDataUsageAccountRepository(t)
	workspaceMockRepos := make(map[string]*mockDataUsageWorkspaceRepository)
	for _, deployment := range deployments {
		workspaceMockRepos[deployment] = newMockDataUsageWorkspaceRepository(t)
	}

	return &DataUsageSyncer{
		accountRepoFactory: func(user string, password string, accountId string) dataUsageAccountRepository {
			return mockAccountRepo
		},
		workspaceRepoFactory: func(host string, user string, password string) (dataUsageWorkspaceRepository, error) {
			deploymentRegex := regexp.MustCompile("https://([a-zA-Z0-9_-]*).cloud.databricks.com")

			deployment := deploymentRegex.ReplaceAllString(host, "${1}")

			if workspaceMock, ok := workspaceMockRepos[deployment]; ok {
				return workspaceMock, nil
			}

			return nil, errors.New("no workspace repository")
		},
	}, mockAccountRepo, workspaceMockRepos
}
