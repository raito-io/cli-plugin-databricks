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

	"cli-plugin-databricks/utils/array"
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

func TestDataUsageSyncer_syncWorkspace(t *testing.T) {
	deployment := "deployment1"
	metastoreId := "metastoreId1"
	duSyncer, _, workspaceRepoMap := createDataUsageSyncer(t, deployment)

	fileCreatorMock := mocks.NewSimpleDataUsageStatementHandler(t)
	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			DatabricksAccountId: "AccountId",
			DatabricksUser:      "User",
			DatabricksPassword:  "Password",
		},
	}

	workspaceRepoMap[deployment].EXPECT().ListCatalogs(mock.Anything).Return([]catalog.CatalogInfo{
		{
			Name:        "catalog1",
			MetastoreId: metastoreId,
		},
		{
			Name:        "catalog2",
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

	workspaceRepoMap[deployment].EXPECT().ListSchemas(mock.Anything, "catalog2").Return([]catalog.SchemaInfo{
		{
			Name:        "schema1",
			FullName:    "catalog2.schema1",
			MetastoreId: metastoreId,
			CatalogName: "catalog2",
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
		{
			Name:        "table2",
			FullName:    "catalog1.schema1.table2",
			MetastoreId: metastoreId,
			CatalogName: "catalog1",
			SchemaName:  "schema1",
		},
	}, nil).Once()

	workspaceRepoMap[deployment].EXPECT().ListTables(mock.Anything, "catalog2", "schema1").Return([]catalog.TableInfo{
		{
			Name:        "table1",
			FullName:    "catalog2.schema1.table1",
			MetastoreId: metastoreId,
			CatalogName: "catalog2",
			SchemaName:  "schema1",
		},
	}, nil).Once()

	startTime := time.Now().Add(time.Hour)
	endTime := time.Now()

	queryHistory := []sql.QueryInfo{
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
		{
			QueryText: "INSERT INTO `catalog1`.`schema1`.`table1` (`id`,`name`,`description`,`created`) VALUES (?,?,?,?)",
			Metrics: &sql.QueryMetrics{
				RowsProducedCount: 1,
				ReadBytes:         2,
			},
			QueryStartTimeMs: int(startTime.UnixMilli()),
			QueryEndTimeMs:   int(endTime.UnixMilli()),
			Status:           sql.QueryStatusFinished,
			UserName:         "ruben@raito.io",
			QueryId:          "queryId2",
			StatementType:    sql.QueryStatementTypeInsert,
		},
		{
			QueryText: "USE CATALOG `catalog2`",
			Metrics: &sql.QueryMetrics{
				RowsProducedCount: 3,
				ReadBytes:         4,
			},
			QueryStartTimeMs: int(startTime.UnixMilli()),
			QueryEndTimeMs:   int(endTime.UnixMilli()),
			Status:           sql.QueryStatusFinished,
			UserName:         "ruben@raito.io",
			QueryId:          "queryId3",
			StatementType:    sql.QueryStatementTypeUse,
		},
		{
			QueryText: "MERGE INTO `schema1`.`table1` USING `catalog1`.schema1`.table1 ON merge_condition",
			Metrics: &sql.QueryMetrics{
				RowsProducedCount: 5,
				ReadBytes:         6,
			},
			QueryStartTimeMs: int(startTime.UnixMilli()),
			QueryEndTimeMs:   int(endTime.UnixMilli()),
			Status:           sql.QueryStatusFinished,
			UserName:         "ruben@raito.io",
			QueryId:          "queryId4",
			StatementType:    sql.QueryStatementTypeMerge,
		},
		{
			QueryText: "UPDATE `schema1`.`table1` SET `description` = 'blablabla'",
			Metrics: &sql.QueryMetrics{
				RowsProducedCount: 5,
				ReadBytes:         6,
			},
			QueryStartTimeMs: int(startTime.UnixMilli()),
			QueryEndTimeMs:   int(endTime.UnixMilli()),
			Status:           sql.QueryStatusFinished,
			UserName:         "ruben@raito.io",
			QueryId:          "queryId5",
			StatementType:    sql.QueryStatementTypeUpdate,
		},
		{
			QueryText: "DELETE FROM `schema1`.`table1`",
			Metrics: &sql.QueryMetrics{
				RowsProducedCount: 20,
				ReadBytes:         21,
			},
			QueryStartTimeMs: int(startTime.UnixMilli()),
			QueryEndTimeMs:   int(endTime.UnixMilli()),
			Status:           sql.QueryStatusFinished,
			UserName:         "ruben@raito.io",
			QueryId:          "queryId6",
			StatementType:    sql.QueryStatementTypeDelete,
		},
	}

	reverse(queryHistory)

	workspaceRepoMap[deployment].EXPECT().QueryHistory(mock.Anything, mock.Anything).Return(queryHistory, nil).Once()

	// When
	err := duSyncer.syncWorkspace(context.Background(), &Workspace{WorkspaceId: 42, DeploymentName: deployment, WorkspaceName: "workspaceName", WorkspaceStatus: "RUNNING"}, &catalog.MetastoreInfo{Name: "Metastore1", MetastoreId: metastoreId}, fileCreatorMock, configMap)

	// Then
	require.NoError(t, err)

	assert.Len(t, fileCreatorMock.Statements, 6)
	assert.ElementsMatch(t, fileCreatorMock.Statements, []data_usage.Statement{
		{
			ExternalId: "queryId1",
			AccessedDataObjects: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.table1",
						Type:     data_source.Table,
					},
					Permissions: []string{"SELECT"},
				},
			},
			User:      "ruben@raito.io",
			Success:   true,
			Status:    "FINISHED",
			StartTime: startTime.Unix(),
			EndTime:   endTime.Unix(),
			Bytes:     2,
			Rows:      1,
		},
		{
			ExternalId: "queryId2",
			AccessedDataObjects: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.table1",
						Type:     data_source.Table,
					},
					Permissions: []string{"INSERT"},
				},
			},
			User:      "ruben@raito.io",
			Success:   true,
			Status:    "FINISHED",
			StartTime: startTime.Unix(),
			EndTime:   endTime.Unix(),
			Bytes:     2,
			Rows:      1,
		},
		{
			ExternalId:          "queryId3",
			AccessedDataObjects: nil,
			User:                "ruben@raito.io",
			Success:             true,
			Status:              "FINISHED",
			StartTime:           startTime.Unix(),
			EndTime:             endTime.Unix(),
			Query:               "USE CATALOG `catalog2`",
		},
		{
			ExternalId: "queryId4",
			AccessedDataObjects: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog2.schema1.table1",
						Type:     data_source.Table,
					},
					Permissions: []string{"MERGE"},
				},
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.table1",
						Type:     data_source.Table,
					},
					Permissions: []string{"SELECT"},
				},
			},
			User:      "ruben@raito.io",
			Success:   true,
			Status:    "FINISHED",
			StartTime: startTime.Unix(),
			EndTime:   endTime.Unix(),
			Bytes:     6,
			Rows:      5,
		},
		{
			ExternalId: "queryId5",
			AccessedDataObjects: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog2.schema1.table1",
						Type:     data_source.Table,
					},
					Permissions: []string{"UPDATE"},
				},
			},
			User:      "ruben@raito.io",
			Success:   true,
			Status:    "FINISHED",
			StartTime: startTime.Unix(),
			EndTime:   endTime.Unix(),
			Bytes:     6,
			Rows:      5,
		},
		{
			ExternalId: "queryId6",
			AccessedDataObjects: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog2.schema1.table1",
						Type:     data_source.Table,
					},
					Permissions: []string{"DELETE"},
				},
			},
			User:      "ruben@raito.io",
			Success:   true,
			Status:    "FINISHED",
			StartTime: startTime.Unix(),
			EndTime:   endTime.Unix(),
			Bytes:     21,
			Rows:      20,
		},
	})
}

func TestDataUsageSyncer_SelectStatement(t *testing.T) {
	tests := []struct {
		query          string
		expectedTables []string
	}{
		{
			query:          "SELECT * FROM events TIMESTAMP AS OF '2018-10-18T22:15:12.013Z';",
			expectedTables: []string{"metastoreId1.catalog1.schema1.events"},
		},
		{
			query:          "SELECT * FROM `catalog1`.`schema1`.`table1`, `catalog1`.`schema1`.`table2`",
			expectedTables: []string{"metastoreId1.catalog1.schema1.table1", "metastoreId1.catalog1.schema1.table2"},
		},
		{
			query:          "SELECT * FROM table1, `table2`",
			expectedTables: []string{"metastoreId1.catalog1.schema1.table1", "metastoreId1.catalog1.schema1.table2"},
		},
		{
			query:          "SELECT * FROM VALUES(1, 2) AS t1(c1, c2), VALUES(3, 4) AS t2(c3, c4);",
			expectedTables: []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			duSyncer := DataUsageSyncer{}
			queryInfo := sql.QueryInfo{
				QueryId:       "queryId1",
				StatementType: sql.QueryStatementTypeSelect,
				Status:        "FINISHED",
				UserName:      "ruben@raito.io",
				QueryText:     test.query,
				Metrics: &sql.QueryMetrics{
					RowsProducedCount: 1,
					ReadBytes:         2,
				},
			}
			tableInfo := map[string][]catalog.TableInfo{
				"events": {{Name: "events", FullName: "catalog1.schema1.events"}},
				"table1": {{Name: "table1", FullName: "catalog1.schema1.table1"}},
				"table2": {{Name: "table2", FullName: "catalog1.schema1.table2"}},
			}
			userLastUsage := map[string]*UserDefaults{"ruben@raito.io": {CatalogName: "catalog1", SchemaName: "schema1"}}
			metastore := catalog.MetastoreInfo{Name: "metastore1", MetastoreId: "metastoreId1"}

			// When
			whatItems, bytes, rows := duSyncer.selectStatement(&queryInfo, queryInfo.QueryText, tableInfo, userLastUsage, &metastore)

			// Then
			assert.Equal(t, rows, 1)
			assert.Equal(t, bytes, 2)
			assert.ElementsMatch(t, array.Map(whatItems, func(i *sync_from_target.WhatItem) string { return i.DataObject.FullName }), test.expectedTables)
		})
	}
}

func TestDataUsageSyncer_UpdateStatement(t *testing.T) {
	tests := []struct {
		query             string
		expectedWhatItems []sync_from_target.WhatItem
	}{
		{
			query: "UPDATE events SET eventType = 'click' WHERE eventType = 'clk';",
			expectedWhatItems: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.events",
						Type:     data_source.Table,
					},
					Permissions: []string{"UPDATE"},
				},
			},
		},
		{
			query: "UPDATE all_events SET session_time = 0, ignored = true WHERE session_time < (SELECT min(session_time) FROM good_events)",
			expectedWhatItems: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.all_events",
						Type:     data_source.Table,
					},
					Permissions: []string{"UPDATE"},
				},
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.good_events",
						Type:     data_source.Table,
					},
					Permissions: []string{"SELECT"},
				},
			},
		},
		{
			query: "UPDATE orders AS t1 SET order_status = 'returned' WHERE EXISTS (SELECT oid FROM returned_orders WHERE t1.oid = oid)",
			expectedWhatItems: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.orders",
						Type:     data_source.Table,
					},
					Permissions: []string{"UPDATE"},
				},
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.returned_orders",
						Type:     data_source.Table,
					},
					Permissions: []string{"SELECT"},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			duSyncer := DataUsageSyncer{}
			queryInfo := sql.QueryInfo{
				QueryId:       "queryId1",
				StatementType: sql.QueryStatementTypeUpdate,
				Status:        "FINISHED",
				UserName:      "ruben@raito.io",
				QueryText:     test.query,
				Metrics: &sql.QueryMetrics{
					RowsProducedCount: 1,
					ReadBytes:         2,
				},
			}
			tableInfo := map[string][]catalog.TableInfo{
				"events":          {{Name: "events", FullName: "catalog1.schema1.events"}},
				"all_events":      {{Name: "all_events", FullName: "catalog1.schema1.all_events"}},
				"good_events":     {{Name: "good_events", FullName: "catalog1.schema1.good_events"}},
				"orders":          {{Name: "good_events", FullName: "catalog1.schema1.orders"}},
				"returned_orders": {{Name: "good_events", FullName: "catalog1.schema1.returned_orders"}},
			}
			userLastUsage := map[string]*UserDefaults{"ruben@raito.io": {CatalogName: "catalog1", SchemaName: "schema1"}}
			metastore := catalog.MetastoreInfo{Name: "metastore1", MetastoreId: "metastoreId1"}

			// When
			whatItems, bytes, rows := duSyncer.updateStatement(&queryInfo, queryInfo.QueryText, tableInfo, userLastUsage, &metastore)

			// Then
			assert.Equal(t, rows, 1)
			assert.Equal(t, bytes, 2)
			assert.ElementsMatch(t, whatItems, test.expectedWhatItems)
		})
	}
}

func TestDataUsageSyncer_MergeStatement(t *testing.T) {
	tests := []struct {
		query             string
		expectedWhatItems []sync_from_target.WhatItem
	}{
		{
			query: "MERGE INTO target USING source ON target.key = source.key WHEN MATCHED THEN DELETE",
			expectedWhatItems: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.target",
						Type:     data_source.Table,
					},
					Permissions: []string{"MERGE"},
				},
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.source",
						Type:     data_source.Table,
					},
					Permissions: []string{"SELECT"},
				},
			},
		},
		{
			query: "MERGE INTO target USING source ON target.key = source.key WHEN MATCHED AND target.updated_at < source.updated_at THEN UPDATE SET *",
			expectedWhatItems: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.target",
						Type:     data_source.Table,
					},
					Permissions: []string{"MERGE"},
				},
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.source",
						Type:     data_source.Table,
					},
					Permissions: []string{"SELECT"},
				},
			},
		},
		{
			query: "MERGE INTO target USING source ON target.key = source.key WHEN NOT MATCHED BY SOURCE THEN DELETE",
			expectedWhatItems: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.target",
						Type:     data_source.Table,
					},
					Permissions: []string{"MERGE"},
				},
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.source",
						Type:     data_source.Table,
					},
					Permissions: []string{"SELECT"},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			duSyncer := DataUsageSyncer{}
			queryInfo := sql.QueryInfo{
				QueryId:       "queryId1",
				StatementType: sql.QueryStatementTypeMerge,
				Status:        "FINISHED",
				UserName:      "ruben@raito.io",
				QueryText:     test.query,
				Metrics: &sql.QueryMetrics{
					RowsProducedCount: 1,
					ReadBytes:         2,
				},
			}
			tableInfo := map[string][]catalog.TableInfo{
				"target": {{Name: "events", FullName: "catalog1.schema1.target"}},
				"source": {{Name: "all_events", FullName: "catalog1.schema1.source"}},
			}
			userLastUsage := map[string]*UserDefaults{"ruben@raito.io": {CatalogName: "catalog1", SchemaName: "schema1"}}
			metastore := catalog.MetastoreInfo{Name: "metastore1", MetastoreId: "metastoreId1"}

			// When
			whatItems, bytes, rows := duSyncer.mergeStatement(&queryInfo, queryInfo.QueryText, tableInfo, userLastUsage, &metastore)

			// Then
			assert.Equal(t, rows, 1)
			assert.Equal(t, bytes, 2)
			assert.ElementsMatch(t, whatItems, test.expectedWhatItems)
		})
	}
}

func TestDataUsageSyncer_InsertStatement(t *testing.T) {
	tests := []struct {
		query             string
		expectedWhatItems []sync_from_target.WhatItem
	}{
		{
			query: "INSERT INTO students VALUES ('Amy Smith', '123 Park Ave, San Jose', 111111);",
			expectedWhatItems: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.students",
						Type:     data_source.Table,
					},
					Permissions: []string{"INSERT"},
				},
			},
		},
		{
			query: "INSERT INTO students(name, student_id) VALUES('Grayson Miller', 222222);",
			expectedWhatItems: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.students",
						Type:     data_source.Table,
					},
					Permissions: []string{"INSERT"},
				},
			},
		},
		{
			query: "INSERT INTO students VALUES('Youna Kim', DEFAULT, 333333);",
			expectedWhatItems: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.students",
						Type:     data_source.Table,
					},
					Permissions: []string{"INSERT"},
				},
			},
		},
		{
			query: "INSERT INTO students VALUES ('Bob Brown', '456 Taylor St, Cupertino', 444444), ('Cathy Johnson', '789 Race Ave, Palo Alto', 555555);",
			expectedWhatItems: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.students",
						Type:     data_source.Table,
					},
					Permissions: []string{"INSERT"},
				},
			},
		},
		{
			query: "INSERT INTO students PARTITION (student_id = 444444)    SELECT name, address FROM persons WHERE name = \"Dora Williams\";",
			expectedWhatItems: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.students",
						Type:     data_source.Table,
					},
					Permissions: []string{"INSERT"},
				},
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.persons",
						Type:     data_source.Table,
					},
					Permissions: []string{"SELECT"},
				},
			},
		},
		{
			query: "INSERT INTO students TABLE visiting_students;",
			expectedWhatItems: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.students",
						Type:     data_source.Table,
					},
					Permissions: []string{"INSERT"},
				},
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.visiting_students",
						Type:     data_source.Table,
					},
					Permissions: []string{"SELECT"},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			duSyncer := DataUsageSyncer{}
			queryInfo := sql.QueryInfo{
				QueryId:       "queryId1",
				StatementType: sql.QueryStatementTypeInsert,
				Status:        "FINISHED",
				UserName:      "ruben@raito.io",
				QueryText:     test.query,
				Metrics: &sql.QueryMetrics{
					RowsProducedCount: 1,
					ReadBytes:         2,
				},
			}
			tableInfo := map[string][]catalog.TableInfo{
				"students":          {{Name: "students", FullName: "catalog1.schema1.students"}},
				"persons":           {{Name: "persons", FullName: "catalog1.schema1.persons"}},
				"visiting_students": {{Name: "visiting_students", FullName: "catalog1.schema1.visiting_students"}},
			}
			userLastUsage := map[string]*UserDefaults{"ruben@raito.io": {CatalogName: "catalog1", SchemaName: "schema1"}}
			metastore := catalog.MetastoreInfo{Name: "metastore1", MetastoreId: "metastoreId1"}

			// When
			whatItems, bytes, rows := duSyncer.insertStatement(&queryInfo, queryInfo.QueryText, tableInfo, userLastUsage, &metastore)

			// Then
			assert.Equal(t, rows, 1)
			assert.Equal(t, bytes, 2)
			assert.ElementsMatch(t, whatItems, test.expectedWhatItems)
		})
	}
}

func TestDataUsageSyncer_DeleteStatement(t *testing.T) {
	tests := []struct {
		query             string
		expectedWhatItems []sync_from_target.WhatItem
	}{
		{
			query: "DELETE FROM events WHERE date < '2017-01-01';",
			expectedWhatItems: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.events",
						Type:     data_source.Table,
					},
					Permissions: []string{"DELETE"},
				},
			},
		},
		{
			query: "DELETE FROM all_events  WHERE session_time < (SELECT min(session_time) FROM good_events)",
			expectedWhatItems: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.all_events",
						Type:     data_source.Table,
					},
					Permissions: []string{"DELETE"},
				},
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.good_events",
						Type:     data_source.Table,
					},
					Permissions: []string{"SELECT"},
				},
			},
		},
		{
			query: "DELETE FROM events   WHERE category NOT IN (SELECT category FROM events2 WHERE date > '2001-01-01');",
			expectedWhatItems: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.events",
						Type:     data_source.Table,
					},
					Permissions: []string{"DELETE"},
				},
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastoreId1.catalog1.schema1.events2",
						Type:     data_source.Table,
					},
					Permissions: []string{"SELECT"},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			duSyncer := DataUsageSyncer{}
			queryInfo := sql.QueryInfo{
				QueryId:       "queryId1",
				StatementType: sql.QueryStatementTypeDelete,
				Status:        "FINISHED",
				UserName:      "ruben@raito.io",
				QueryText:     test.query,
				Metrics: &sql.QueryMetrics{
					RowsProducedCount: 1,
					ReadBytes:         2,
				},
			}
			tableInfo := map[string][]catalog.TableInfo{
				"events":      {{Name: "events", FullName: "catalog1.schema1.events"}},
				"events2":     {{Name: "events2", FullName: "catalog1.schema1.events2"}},
				"all_events":  {{Name: "visiting_students", FullName: "catalog1.schema1.all_events"}},
				"good_events": {{Name: "good_events", FullName: "catalog1.schema1.good_events"}},
			}
			userLastUsage := map[string]*UserDefaults{"ruben@raito.io": {CatalogName: "catalog1", SchemaName: "schema1"}}
			metastore := catalog.MetastoreInfo{Name: "metastore1", MetastoreId: "metastoreId1"}

			// When
			whatItems, bytes, rows := duSyncer.deleteStatement(&queryInfo, queryInfo.QueryText, tableInfo, userLastUsage, &metastore)

			// Then
			assert.Equal(t, rows, 1)
			assert.Equal(t, bytes, 2)
			assert.ElementsMatch(t, whatItems, test.expectedWhatItems)
		})
	}

}

func createDataUsageSyncer(t *testing.T, deployments ...string) (*DataUsageSyncer, *mockDataUsageAccountRepository, map[string]*mockDataUsageWorkspaceRepository) {
	t.Helper()

	mockAccountRepo := newMockDataUsageAccountRepository(t)
	workspaceMockRepos := make(map[string]*mockDataUsageWorkspaceRepository)
	for _, deployment := range deployments {
		workspaceMockRepos[deployment] = newMockDataUsageWorkspaceRepository(t)
	}

	return &DataUsageSyncer{
		accountRepoFactory: func(accountId string, repoCredentials RepositoryCredentials) dataUsageAccountRepository {
			return mockAccountRepo
		},
		workspaceRepoFactory: func(host string, repoCredentials RepositoryCredentials) (dataUsageWorkspaceRepository, error) {
			deploymentRegex := regexp.MustCompile("https://([a-zA-Z0-9_-]*).cloud.databricks.com")

			deployment := deploymentRegex.ReplaceAllString(host, "${1}")

			if workspaceMock, ok := workspaceMockRepos[deployment]; ok {
				return workspaceMock, nil
			}

			return nil, errors.New("no workspace repository")
		},
	}, mockAccountRepo, workspaceMockRepos
}

func TestCleanUpQueryText(t *testing.T) {
	query := `SELECT * FROM 	events   WHERE
                            date < '2017-01-01'; --This is a comment
		-- this is also a comment
			SELECT column1 FROM table1 WHERE column1 = ';' LIMIT 500;
 /*
  	This is all comment
  	Blablabla
  */
  	UPDATE table1 
  	SET column1 = 'Blablabla' 
  	WHERE column1 = 'Blab'`

	result := cleanUpQueryText(query)

	expected := `SELECT * FROM events WHERE date < '2017-01-01';
SELECT column1 FROM table1 WHERE column1 = ';' LIMIT 500;
UPDATE table1 SET column1 = 'Blablabla' WHERE column1 = 'Blab'`

	assert.Equal(t, result, expected)
}