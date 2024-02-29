package databricks

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/raito-io/bexpression"
	"github.com/raito-io/bexpression/datacomparison"
	"github.com/raito-io/cli/base/access_provider"
	"github.com/raito-io/cli/base/access_provider/sync_from_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"cli-plugin-databricks/databricks/platform"
	"cli-plugin-databricks/databricks/repo"
	"cli-plugin-databricks/databricks/types"
	"cli-plugin-databricks/utils/array"
)

func TestAccessSyncer_SyncAccessProvidersFromTarget(t *testing.T) {
	// Given
	deployment := "test-deployment"
	workspace := "test-workspace"
	accessSyncer, mockAccountRepo, mockWorkspaceRepoMap := createAccessSyncer(t, deployment)

	accessProviderHandlerMock := mocks.NewSimpleAccessProviderHandler(t, 1)
	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			DatabricksAccountId: "AccountId",
			DatabricksUser:      "User",
			DatabricksPassword:  "Password",
			DatabricksPlatform:  "AWS",
		},
	}

	accessSyncer.privilegeCache.AddPrivilege(data_source.DataObjectReference{FullName: "metastore-id1.catalog-1.schema-1", Type: data_source.Schema}, "principal1", "SELECT")

	metastore1 := catalog.MetastoreInfo{
		Name:        "metastore1",
		MetastoreId: "metastore-id1",
	}

	workspaceObject := repo.Workspace{
		WorkspaceId:     42,
		DeploymentName:  deployment,
		WorkspaceName:   workspace,
		WorkspaceStatus: "RUNNING",
	}

	mockAccountRepo.EXPECT().ListMetastores(mock.Anything).Return([]catalog.MetastoreInfo{metastore1}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaces(mock.Anything).Return([]repo.Workspace{workspaceObject}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaceMap(mock.Anything, []catalog.MetastoreInfo{metastore1}, []repo.Workspace{workspaceObject}).Return(map[string][]string{metastore1.MetastoreId: {deployment}}, nil, nil).Twice()
	mockAccountRepo.EXPECT().ListWorkspaceAssignments(mock.Anything, workspaceObject.WorkspaceId).Return([]iam.PermissionAssignment{
		{
			Principal: &iam.PrincipalOutput{
				UserName:    "ruben@raito.io",
				DisplayName: "Ruben",
				PrincipalId: 314,
			},
			Permissions: []iam.WorkspacePermission{"USER"},
		},
		{
			Principal: &iam.PrincipalOutput{
				UserName:    "dieter@raito.io",
				DisplayName: "Dieter",
				PrincipalId: 1526,
			},
			Permissions: []iam.WorkspacePermission{"ADMIN"},
		},
	}, nil).Once()

	mockWorkspaceRepoMap[deployment].EXPECT().Ping(mock.Anything).Return(nil).Twice()
	mockWorkspaceRepoMap[deployment].EXPECT().GetPermissionsOnResource(mock.Anything, catalog.SecurableTypeMetastore, "metastore-id1").Return(nil, nil).Once()

	mockWorkspaceRepoMap[deployment].EXPECT().ListCatalogs(mock.Anything).Return([]catalog.CatalogInfo{
		{
			Name:        "catalog-1",
			FullName:    "catalog-1",
			MetastoreId: metastore1.MetastoreId,
			Comment:     "comment on catalog-1",
		},
	}, nil).Once()
	mockWorkspaceRepoMap[deployment].EXPECT().GetPermissionsOnResource(mock.Anything, catalog.SecurableTypeCatalog, "catalog-1").
		Return(&catalog.PermissionsList{
			PrivilegeAssignments: []catalog.PrivilegeAssignment{
				{
					Principal:  "ruben@raito.io",
					Privileges: []catalog.Privilege{catalog.PrivilegeUseCatalog, catalog.PrivilegeExecute},
				},
				{
					Principal:  "group1",
					Privileges: []catalog.Privilege{catalog.PrivilegeUseCatalog, catalog.PrivilegeSelect},
				},
			},
		}, nil).Once()

	mockWorkspaceRepoMap[deployment].EXPECT().ListSchemas(mock.Anything, "catalog-1").Return([]catalog.SchemaInfo{
		{
			Name:        "schema-1",
			MetastoreId: metastore1.MetastoreId,
			CatalogName: "catalog-1",
			Comment:     "comment on schema-1",
			FullName:    "catalog-1.schema-1",
		},
	}, nil).Once()
	mockWorkspaceRepoMap[deployment].EXPECT().GetPermissionsOnResource(mock.Anything, catalog.SecurableTypeSchema, "catalog-1.schema-1").
		Return(&catalog.PermissionsList{
			PrivilegeAssignments: []catalog.PrivilegeAssignment{
				{
					Principal:  "principal1",
					Privileges: []catalog.Privilege{catalog.PrivilegeSelect, catalog.PrivilegeExecute},
				},
				{
					Principal:  "ruben@raito.io",
					Privileges: []catalog.Privilege{catalog.PrivilegeSelect, catalog.PrivilegeModify},
				},
			},
		}, nil).Once()

	mockWorkspaceRepoMap[deployment].EXPECT().ListTables(mock.Anything, "catalog-1", "schema-1").Return([]catalog.TableInfo{
		{
			Name:        "table-1",
			MetastoreId: metastore1.MetastoreId,
			CatalogName: "catalog-1",
			SchemaName:  "schema-1",
			Comment:     "comment on table-1",
			FullName:    "catalog-1.schema-1.table-1",
			TableType:   catalog.TableTypeManaged,
			Columns: []catalog.ColumnInfo{
				{
					Name: "column-1",
					Mask: &catalog.ColumnMask{
						FunctionName: "catalog-1.schema-1.function-2",
					},
				},
				{
					Name: "column-2",
				},
			},
		},
	}, nil)
	mockWorkspaceRepoMap[deployment].EXPECT().GetPermissionsOnResource(mock.Anything, catalog.SecurableTypeTable, "catalog-1.schema-1.table-1").
		Return(&catalog.PermissionsList{
			PrivilegeAssignments: []catalog.PrivilegeAssignment{
				{
					Principal:  "bart@raito.io",
					Privileges: []catalog.Privilege{catalog.PrivilegeSelect},
				},
			},
		}, nil).Once()

	mockWorkspaceRepoMap[deployment].EXPECT().ListFunctions(mock.Anything, "catalog-1", "schema-1").Return([]repo.FunctionInfo{
		{
			Name:        "function-1",
			MetastoreId: metastore1.MetastoreId,
			CatalogName: "catalog-1",
			SchemaName:  "schema-1",
			Comment:     "comment on function-1",
			FullName:    "catalog-1.schema-1.function-1",
		},
		{
			Name:              "function-2",
			MetastoreId:       metastore1.MetastoreId,
			CatalogName:       "catalog-1",
			SchemaName:        "schema-1",
			Comment:           "Used as mask",
			FullName:          "catalog-1.schema-1.function-2",
			RoutineDefinition: "CASE username() IN ('ruben@raito.io') THEN val else '****'",
		},
	}, nil).Once()
	mockWorkspaceRepoMap[deployment].EXPECT().GetPermissionsOnResource(mock.Anything, catalog.SecurableTypeFunction, "catalog-1.schema-1.function-1").
		Return(&catalog.PermissionsList{
			PrivilegeAssignments: []catalog.PrivilegeAssignment{
				{
					Principal:  "bart@raito.io",
					Privileges: []catalog.Privilege{catalog.PrivilegeExecute},
				},
			},
		}, nil).Once()

	// When
	err := accessSyncer.SyncAccessProvidersFromTarget(context.Background(), accessProviderHandlerMock, configMap)

	// Then
	require.NoError(t, err)

	assert.ElementsMatch(t, accessProviderHandlerMock.AccessProviders, []sync_from_target.AccessProvider{
		{
			ExternalId: "test-workspace_USER",
			Name:       "test-workspace_USER",
			NamingHint: "test-workspace_USER",
			ActualName: "test-workspace_USER",
			Action:     sync_from_target.Grant,
			Type:       ptr.String(access_provider.AclSet),
			Who: &sync_from_target.WhoItem{
				Users: []string{"ruben@raito.io"},
			},
			What: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "42",
						Type:     workspaceType,
					},
					Permissions: []string{"USER"},
				},
			},
		},
		{
			ExternalId: "test-workspace_ADMIN",
			Name:       "test-workspace_ADMIN",
			NamingHint: "test-workspace_ADMIN",
			ActualName: "test-workspace_ADMIN",
			Action:     sync_from_target.Grant,
			Type:       ptr.String(access_provider.AclSet),
			Who: &sync_from_target.WhoItem{
				Users: []string{"dieter@raito.io"},
			},
			What: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "42",
						Type:     workspaceType,
					},
					Permissions: []string{"ADMIN"},
				},
			},
		},
		{
			ExternalId: "metastore-id1.catalog-1_SELECT",
			Name:       "metastore-id1.catalog-1_SELECT",
			NamingHint: "metastore-id1.catalog-1_SELECT",
			ActualName: "metastore-id1.catalog-1_SELECT",
			Action:     sync_from_target.Grant,
			Type:       ptr.String(access_provider.AclSet),
			Who: &sync_from_target.WhoItem{
				Groups: []string{"group1"},
			},
			What: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastore-id1.catalog-1",
						Type:     catalogType,
					},
					Permissions: []string{"SELECT"},
				},
			},
		},
		{
			ExternalId: "metastore-id1.catalog-1_USE_CATALOG",
			Name:       "metastore-id1.catalog-1_USE_CATALOG",
			NamingHint: "metastore-id1.catalog-1_USE_CATALOG",
			ActualName: "metastore-id1.catalog-1_USE_CATALOG",
			Action:     sync_from_target.Grant,
			Type:       ptr.String(access_provider.AclSet),
			Who: &sync_from_target.WhoItem{
				Users:  []string{"ruben@raito.io"},
				Groups: []string{"group1"},
			},
			What: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastore-id1.catalog-1",
						Type:     catalogType,
					},
					Permissions: []string{"USE CATALOG"},
				},
			},
		},
		{
			ExternalId: "metastore-id1.catalog-1_EXECUTE",
			Name:       "metastore-id1.catalog-1_EXECUTE",
			NamingHint: "metastore-id1.catalog-1_EXECUTE",
			ActualName: "metastore-id1.catalog-1_EXECUTE",
			Action:     sync_from_target.Grant,
			Type:       ptr.String(access_provider.AclSet),
			Who: &sync_from_target.WhoItem{
				Users: []string{"ruben@raito.io"},
			},
			What: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastore-id1.catalog-1",
						Type:     catalogType,
					},
					Permissions: []string{"EXECUTE"},
				},
			},
		},
		{
			ExternalId: "metastore-id1.catalog-1.schema-1_EXECUTE",
			Name:       "metastore-id1.catalog-1.schema-1_EXECUTE",
			NamingHint: "metastore-id1.catalog-1.schema-1_EXECUTE",
			ActualName: "metastore-id1.catalog-1.schema-1_EXECUTE",
			Action:     sync_from_target.Grant,
			Type:       ptr.String(access_provider.AclSet),
			Who: &sync_from_target.WhoItem{
				Groups: []string{"principal1"},
			},
			What: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastore-id1.catalog-1.schema-1",
						Type:     data_source.Schema,
					},
					Permissions: []string{"EXECUTE"},
				},
			},
		},
		{
			ExternalId: "metastore-id1.catalog-1.schema-1_SELECT",
			Name:       "metastore-id1.catalog-1.schema-1_SELECT",
			NamingHint: "metastore-id1.catalog-1.schema-1_SELECT",
			ActualName: "metastore-id1.catalog-1.schema-1_SELECT",
			Action:     sync_from_target.Grant,
			Type:       ptr.String(access_provider.AclSet),
			Who: &sync_from_target.WhoItem{
				Users: []string{"ruben@raito.io"},
				// Groups principal1 should be excluded
			},
			What: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastore-id1.catalog-1.schema-1",
						Type:     data_source.Schema,
					},
					Permissions: []string{"SELECT"},
				},
			},
		},
		{
			ExternalId: "metastore-id1.catalog-1.schema-1_MODIFY",
			Name:       "metastore-id1.catalog-1.schema-1_MODIFY",
			NamingHint: "metastore-id1.catalog-1.schema-1_MODIFY",
			ActualName: "metastore-id1.catalog-1.schema-1_MODIFY",
			Action:     sync_from_target.Grant,
			Type:       ptr.String(access_provider.AclSet),
			Who: &sync_from_target.WhoItem{
				Users: []string{"ruben@raito.io"},
			},
			What: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastore-id1.catalog-1.schema-1",
						Type:     data_source.Schema,
					},
					Permissions: []string{"MODIFY"},
				},
			},
		},
		{
			ExternalId: "metastore-id1.catalog-1.schema-1.table-1_SELECT",
			Name:       "metastore-id1.catalog-1.schema-1.table-1_SELECT",
			NamingHint: "metastore-id1.catalog-1.schema-1.table-1_SELECT",
			ActualName: "metastore-id1.catalog-1.schema-1.table-1_SELECT",
			Action:     sync_from_target.Grant,
			Type:       ptr.String(access_provider.AclSet),
			Who: &sync_from_target.WhoItem{
				Users: []string{"bart@raito.io"},
			},
			What: []sync_from_target.WhatItem{{
				DataObject: &data_source.DataObjectReference{
					FullName: "metastore-id1.catalog-1.schema-1.table-1",
					Type:     data_source.Table,
				},
				Permissions: []string{"SELECT"},
			}},
		},
		{
			ExternalId: "metastore-id1.catalog-1.schema-1.function-1_EXECUTE",
			Name:       "metastore-id1.catalog-1.schema-1.function-1_EXECUTE",
			NamingHint: "metastore-id1.catalog-1.schema-1.function-1_EXECUTE",
			ActualName: "metastore-id1.catalog-1.schema-1.function-1_EXECUTE",
			Action:     sync_from_target.Grant,
			Type:       ptr.String(access_provider.AclSet),
			Who: &sync_from_target.WhoItem{
				Users: []string{"bart@raito.io"},
			},
			What: []sync_from_target.WhatItem{{
				DataObject: &data_source.DataObjectReference{
					FullName: "metastore-id1.catalog-1.schema-1.function-1",
					Type:     functionType,
				},
				Permissions: []string{"EXECUTE"},
			}},
		},
		{
			ExternalId:        "metastore-id1.catalog-1.schema-1.function-2",
			Name:              "function-2",
			Action:            sync_from_target.Mask,
			Policy:            "CASE username() IN ('ruben@raito.io') THEN val else '****'",
			NotInternalizable: true,
			ActualName:        "metastore-id1.catalog-1.schema-1.function-2",
			What: []sync_from_target.WhatItem{
				{
					DataObject: &data_source.DataObjectReference{
						FullName: "metastore-id1.catalog-1.schema-1.table-1.column-1",
						Type:     data_source.Column,
					},
				},
			},
			Incomplete: ptr.Bool(true),
		},
	})
}

func TestAccessSyncer_SyncAccessProviderToTarget(t *testing.T) {
	// Given
	deployment := "test-deployment"
	workspace := "test-workspace"
	accessSyncer, mockAccountRepo, mockWorkspaceRepoMap := createAccessSyncer(t, deployment)

	accessProviderHandlerMock := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	accessProviders := sync_to_target.AccessProviderImport{
		AccessProviders: []*sync_to_target.AccessProvider{
			{
				Id:     "workspace-ap-id",
				Name:   "workspace-ap",
				Action: sync_to_target.Grant,
				What: []sync_to_target.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "42",
							Type:     workspaceType,
						},
						Permissions: []string{"USER"},
					},
				},
				Who: sync_to_target.WhoItem{
					Users:  []string{"ruben@raito.io"},
					Groups: []string{"group1"},
				},
				DeletedWho: &sync_to_target.WhoItem{
					Users: []string{"dieter@raito.io"},
				},
			},
			{
				Id:     "catalog-ap-id",
				Name:   "catalog-ap",
				Action: sync_to_target.Grant,
				What: []sync_to_target.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "metastore-id1.catalog-1",
							Type:     catalogType,
						},
						Permissions: []string{"SELECT"},
					},
				},
				Who: sync_to_target.WhoItem{
					Users: []string{"wannes@raito.io"},
				},
				DeletedWho: &sync_to_target.WhoItem{
					Users: []string{"jonas@raito.io"},
				},
			},
			{
				Id:     "multiple-do-ap-id",
				Name:   "multiple-do-ap",
				Action: sync_to_target.Grant,
				What: []sync_to_target.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "metastore-id1.catalog-1.schema-1",
							Type:     data_source.Schema,
						},
						Permissions: []string{"SELECT", "MODIFY"},
					},
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "metastore-id1.catalog-2",
							Type:     catalogType,
						},
						Permissions: []string{"CREATE TABLE"},
					},
				},
				Who: sync_to_target.WhoItem{
					Users: []string{"bart@raito.io"},
				},
				DeletedWho: &sync_to_target.WhoItem{},
			},
		},
	}

	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			DatabricksAccountId: "AccountId",
			DatabricksUser:      "User",
			DatabricksPassword:  "Password",
			DatabricksPlatform:  "AWS",
		},
	}

	metastore1 := catalog.MetastoreInfo{
		Name:        "metastore1",
		MetastoreId: "metastore-id1",
	}

	workspaceObject := repo.Workspace{
		WorkspaceId:     42,
		DeploymentName:  deployment,
		WorkspaceName:   workspace,
		WorkspaceStatus: "RUNNING",
	}

	mockAccountRepo.EXPECT().ListMetastores(mock.Anything).Return([]catalog.MetastoreInfo{metastore1}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaces(mock.Anything).Return([]repo.Workspace{workspaceObject}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaceMap(mock.Anything, []catalog.MetastoreInfo{metastore1}, []repo.Workspace{workspaceObject}).Return(map[string][]string{metastore1.MetastoreId: {deployment}}, nil, nil).Once()

	mockWorkspaceRepoMap[deployment].EXPECT().Ping(mock.Anything).Return(nil).Once()
	mockWorkspaceRepoMap[deployment].EXPECT().SetPermissionsOnResource(mock.Anything, catalog.SecurableTypeCatalog, "catalog-1", mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, securableType catalog.SecurableType, s string, change ...catalog.PermissionsChange) error {
		assert.Len(t, change, 3)

		wannes := -1
		bart := -1
		jonas := -1

		for i, c := range change {
			switch c.Principal {
			case "bart@raito.io":
				bart = i
			case "wannes@raito.io":
				wannes = i
			case "jonas@raito.io":
				jonas = i
			}
		}

		require.NotEqual(t, -1, bart)
		require.NotEqual(t, -1, wannes)
		require.NotEqual(t, -1, jonas)

		assert.ElementsMatch(t, []catalog.Privilege{catalog.PrivilegeUseCatalog, catalog.PrivilegeSelect}, change[wannes].Add)
		assert.ElementsMatch(t, []catalog.Privilege{catalog.PrivilegeUseCatalog}, change[bart].Add)
		assert.ElementsMatch(t, []catalog.Privilege{}, change[jonas].Add)
		assert.ElementsMatch(t, []catalog.Privilege{}, change[bart].Remove)
		assert.ElementsMatch(t, []catalog.Privilege{}, change[wannes].Remove)
		assert.ElementsMatch(t, []catalog.Privilege{catalog.PrivilegeSelect}, change[jonas].Remove) // USE CATALOG should not be removed

		return nil
	}).Once()
	mockWorkspaceRepoMap[deployment].EXPECT().SetPermissionsOnResource(mock.Anything, catalog.SecurableTypeCatalog, "catalog-2", mock.Anything).RunAndReturn(func(_ context.Context, securableType catalog.SecurableType, s string, change ...catalog.PermissionsChange) error {
		assert.ElementsMatch(t, []catalog.Privilege{catalog.PrivilegeCreateTable, catalog.PrivilegeUseCatalog}, change[0].Add)

		return nil
	}).Once()
	mockWorkspaceRepoMap[deployment].EXPECT().SetPermissionsOnResource(mock.Anything, catalog.SecurableTypeSchema, "catalog-1.schema-1", mock.Anything).RunAndReturn(func(ctx context.Context, securableType catalog.SecurableType, s string, change ...catalog.PermissionsChange) error {
		assert.ElementsMatch(t, []catalog.Privilege{catalog.PrivilegeModify, catalog.PrivilegeUseSchema, catalog.PrivilegeSelect}, change[0].Add)
		assert.Equal(t, "bart@raito.io", change[0].Principal)

		return nil
	}).Once()

	mockAccountRepo.EXPECT().ListUsers(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, f ...func(filter *repo.DatabricksUsersFilter)) <-chan interface{} {
		options := repo.DatabricksUsersFilter{}
		for _, fn := range f {
			fn(&options)
		}

		require.NotNil(t, options.Username)

		if *options.Username == "ruben@raito.io" {
			return array.ArrayToChannel([]interface{}{
				iam.User{
					DisplayName: "Ruben Mennes",
					Id:          "314",
				},
			})
		} else if *options.Username == "dieter@raito.io" {
			return array.ArrayToChannel([]interface{}{
				iam.User{
					DisplayName: "Dieter Wachters",
					Id:          "1592",
				},
			})
		} else {
			assert.Fail(t, "unexpected username")
		}

		return array.ArrayToChannel[interface{}]([]interface{}{})
	})
	mockAccountRepo.EXPECT().ListGroups(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, f ...func(filter *repo.DatabricksGroupsFilter)) <-chan interface{} {
		options := repo.DatabricksGroupsFilter{}
		for _, fn := range f {
			fn(&options)
		}

		require.NotNil(t, options.Groupname)
		require.Equal(t, "group1", *options.Groupname)

		return array.ArrayToChannel([]interface{}{iam.Group{DisplayName: "group1", Id: "6535"}})
	})
	mockAccountRepo.EXPECT().UpdateWorkspaceAssignment(mock.Anything, 42, int64(314), []iam.WorkspacePermission{iam.WorkspacePermissionUser}).Return(nil).Once()
	mockAccountRepo.EXPECT().UpdateWorkspaceAssignment(mock.Anything, 42, int64(6535), []iam.WorkspacePermission{iam.WorkspacePermissionUser}).Return(nil).Once()
	mockAccountRepo.EXPECT().UpdateWorkspaceAssignment(mock.Anything, 42, int64(1592), []iam.WorkspacePermission{}).Return(nil).Once()

	// When
	err := accessSyncer.SyncAccessProviderToTarget(context.Background(), &accessProviders, accessProviderHandlerMock, configMap)

	// Then
	require.NoError(t, err)

	assert.Len(t, accessProviderHandlerMock.AccessProviderFeedback, 3)
	assert.ElementsMatch(t, []sync_to_target.AccessProviderSyncFeedback{
		{
			AccessProvider: "workspace-ap-id",
			ActualName:     "workspace-ap-id",
			Type:           ptr.String(access_provider.AclSet),
		},
		{
			AccessProvider: "catalog-ap-id",
			ActualName:     "catalog-ap-id",
			Type:           ptr.String(access_provider.AclSet),
		},
		{
			AccessProvider: "multiple-do-ap-id",
			ActualName:     "multiple-do-ap-id",
			Type:           ptr.String(access_provider.AclSet),
		},
	}, accessProviderHandlerMock.AccessProviderFeedback)
}

func TestAccessSyncer_SyncAccessProviderToTarget_withMasks(t *testing.T) {
	// Given
	deployment := "test-deployment"
	workspace := "test-workspace"
	accessSyncer, mockAccountRepo, mockWorkspaceRepoMap := createAccessSyncer(t, deployment)

	accessProviderHandlerMock := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	accessProviders := sync_to_target.AccessProviderImport{
		AccessProviders: []*sync_to_target.AccessProvider{
			{
				Id:         "workspace-ap-id",
				Name:       "workspace-ap",
				NamingHint: "workspace-ap",
				Action:     sync_to_target.Mask,
				What: []sync_to_target.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "metastore-id1.catalog-1.schema-1.table-1.column-1",
							Type:     data_source.Column,
						},
						Permissions: []string{"USER"},
					},
				},
				Who: sync_to_target.WhoItem{
					Users:  []string{"ruben@raito.io"},
					Groups: []string{"group1"},
				},
				DeletedWho: &sync_to_target.WhoItem{
					Users: []string{"dieter@raito.io"},
				},
			},
		},
	}

	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			DatabricksAccountId:     "AccountId",
			DatabricksUser:          "User",
			DatabricksPassword:      "Password",
			DatabricksSqlWarehouses: fmt.Sprintf(`{"metastore-id1": {"workspace": "%s", "warehouse": "sqlWarehouse1"}}`, deployment),
			DatabricksPlatform:      "AWS",
		},
	}

	metastore1 := catalog.MetastoreInfo{
		Name:        "metastore1",
		MetastoreId: "metastore-id1",
	}

	workspaceObject := repo.Workspace{
		WorkspaceId:     42,
		DeploymentName:  deployment,
		WorkspaceName:   workspace,
		WorkspaceStatus: "RUNNING",
	}

	mockAccountRepo.EXPECT().ListMetastores(mock.Anything).Return([]catalog.MetastoreInfo{metastore1}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaces(mock.Anything).Return([]repo.Workspace{workspaceObject}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaceMap(mock.Anything, []catalog.MetastoreInfo{metastore1}, []repo.Workspace{workspaceObject}).Return(map[string][]string{metastore1.MetastoreId: {deployment}}, nil, nil).Once()

	mockWarehouseRepo := repo.NewMockWarehouseRepository(t)

	mockWorkspaceRepoMap[deployment].EXPECT().SqlWarehouseRepository("sqlWarehouse1").Return(mockWarehouseRepo)

	mockWarehouseRepo.EXPECT().GetTableInformation(mock.Anything, "catalog-1", "schema-1", "table-1").Return(map[string]*repo.ColumnInformation{
		"column-1": {
			Name: "column-1",
			Type: "string",
		},
	}, nil).Once()
	mockWarehouseRepo.EXPECT().ExecuteStatement(mock.Anything, "catalog-1", "schema-1", "CREATE OR REPLACE FUNCTION raito_workspaceap_string(val string)\nRETURN CASE\n\tWHEN current_user() IN ('ruben@raito.io') THEN val\n\tWHEN is_account_group_member('group1') THEN val\n\tELSE '*****'\nEND;").Return(nil, nil).Once()
	mockWarehouseRepo.EXPECT().SetMask(mock.Anything, "catalog-1", "schema-1", "table-1", "column-1", "raito_workspaceap_string").Return(nil).Once()

	// When
	err := accessSyncer.SyncAccessProviderToTarget(context.Background(), &accessProviders, accessProviderHandlerMock, configMap)

	// Then
	require.NoError(t, err)

	assert.Len(t, accessProviderHandlerMock.AccessProviderFeedback, 1)
	assert.ElementsMatch(t, []sync_to_target.AccessProviderSyncFeedback{
		{
			AccessProvider: "workspace-ap-id",
			ActualName:     "raito_workspace-ap",
			ExternalId:     ptr.String("raito_workspace-ap"),
		},
	}, accessProviderHandlerMock.AccessProviderFeedback)
}

func TestAccessSyncer_SyncAccessProviderToTarget_withFilters(t *testing.T) {
	// Given
	deployment := "test-deployment"
	workspace := "test-workspace"
	accessSyncer, mockAccountRepo, mockWorkspaceRepoMap := createAccessSyncer(t, deployment)

	accessProviderHandlerMock := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	accessProviders := sync_to_target.AccessProviderImport{
		AccessProviders: []*sync_to_target.AccessProvider{
			{
				Id:         "filter-ap-id1",
				Name:       "filter-ap-1",
				NamingHint: "filter-ap-1",
				Action:     sync_to_target.Filtered,
				What: []sync_to_target.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "metastore-id1.catalog-1.schema-1.table-1",
							Type:     data_source.Table,
						},
					},
				},
				Who: sync_to_target.WhoItem{
					Users:  []string{"ruben@raito.io"},
					Groups: []string{"group1"},
				},
				DeletedWho: &sync_to_target.WhoItem{
					Users: []string{"dieter@raito.io"},
				},
				FilterCriteria: &bexpression.DataComparisonExpression{
					Comparison: &datacomparison.DataComparison{
						LeftOperand: datacomparison.Operand{
							Reference: &datacomparison.Reference{
								EntityType: datacomparison.EntityTypeColumnReferenceByName,
								EntityID:   `column1`,
							},
						},
						Operator: datacomparison.ComparisonOperatorGreaterThanOrEqual,
						RightOperand: datacomparison.Operand{
							Literal: &datacomparison.Literal{
								Float: ptr.Float64(3.14),
							},
						},
					},
				},
			},
			{
				Id:         "filter-ap-id2",
				Name:       "filter-ap-2",
				NamingHint: "filter-ap-2",
				Action:     sync_to_target.Filtered,
				What: []sync_to_target.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "metastore-id1.catalog-1.schema-2.table-1",
							Type:     data_source.Table,
						},
					},
				},
				Who: sync_to_target.WhoItem{
					Groups: []string{"group2"},
				},
				PolicyRule: ptr.String("{refColumn} = 'NJ'"),
			},
		},
	}

	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			DatabricksAccountId:     "AccountId",
			DatabricksUser:          "User",
			DatabricksPassword:      "Password",
			DatabricksSqlWarehouses: fmt.Sprintf(`{"metastore-id1": {"workspace": "%s", "warehouse": "sqlWarehouse1"}}`, deployment),
			DatabricksPlatform:      "AWS",
		},
	}

	metastore1 := catalog.MetastoreInfo{
		Name:        "metastore1",
		MetastoreId: "metastore-id1",
	}

	workspaceObject := repo.Workspace{
		WorkspaceId:     42,
		DeploymentName:  deployment,
		WorkspaceName:   workspace,
		WorkspaceStatus: "RUNNING",
	}

	mockAccountRepo.EXPECT().ListMetastores(mock.Anything).Return([]catalog.MetastoreInfo{metastore1}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaces(mock.Anything).Return([]repo.Workspace{workspaceObject}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaceMap(mock.Anything, []catalog.MetastoreInfo{metastore1}, []repo.Workspace{workspaceObject}).Return(map[string][]string{metastore1.MetastoreId: {deployment}}, nil, nil).Once()

	mockWarehouseRepo := repo.NewMockWarehouseRepository(t)

	mockWorkspaceRepoMap[deployment].EXPECT().SqlWarehouseRepository("sqlWarehouse1").Return(mockWarehouseRepo)
	mockWorkspaceRepoMap[deployment].EXPECT().GetOwner(mock.Anything, catalog.SecurableTypeTable, "catalog-1.schema-1.table-1").Return("owner@raito.io", nil).Once()
	mockWorkspaceRepoMap[deployment].EXPECT().GetOwner(mock.Anything, catalog.SecurableTypeTable, "catalog-1.schema-2.table-1").Return("owner2@raito.io", nil).Once()
	mockWorkspaceRepoMap[deployment].EXPECT().SetPermissionsOnResource(mock.Anything, catalog.SecurableTypeFunction, "catalog-1.schema-1.raito_table-1_filter", catalog.PermissionsChange{Add: []catalog.Privilege{catalog.PrivilegeExecute}, Principal: "owner@raito.io"}).Return(nil)
	mockWorkspaceRepoMap[deployment].EXPECT().SetPermissionsOnResource(mock.Anything, catalog.SecurableTypeFunction, "catalog-1.schema-2.raito_table-1_filter", catalog.PermissionsChange{Add: []catalog.Privilege{catalog.PrivilegeExecute}, Principal: "owner2@raito.io"}).Return(nil)

	mockWarehouseRepo.EXPECT().GetTableInformation(mock.Anything, "catalog-1", "schema-1", "table-1").Return(map[string]*repo.ColumnInformation{
		"column1": {
			Type: "float",
			Name: "column1",
		},
	}, nil)

	mockWarehouseRepo.EXPECT().GetTableInformation(mock.Anything, "catalog-1", "schema-2", "table-1").Return(map[string]*repo.ColumnInformation{
		"refColumn": {
			Name: "refColumn",
			Type: "string",
		},
	}, nil)

	mockWarehouseRepo.EXPECT().ExecuteStatement(mock.Anything, "catalog-1", "schema-1", "CREATE OR REPLACE FUNCTION raito_table-1_filter(column1 float)\n RETURN ((current_user() IN ('ruben@raito.io') OR is_account_group_member('group1')) AND ((column1 >= 3.140000)));").Return(nil, nil).Once()
	mockWarehouseRepo.EXPECT().ExecuteStatement(mock.Anything, "catalog-1", "schema-2", "CREATE OR REPLACE FUNCTION raito_table-1_filter(refColumn string)\n RETURN ((is_account_group_member('group2')) AND (refColumn = 'NJ'));").Return(nil, nil).Once()
	mockWarehouseRepo.EXPECT().SetRowFilter(mock.Anything, "catalog-1", "schema-1", "table-1", "raito_table-1_filter", []string{"column1"}).Return(nil)
	mockWarehouseRepo.EXPECT().SetRowFilter(mock.Anything, "catalog-1", "schema-2", "table-1", "raito_table-1_filter", []string{"refColumn"}).Return(nil)

	// When
	err := accessSyncer.SyncAccessProviderToTarget(context.Background(), &accessProviders, accessProviderHandlerMock, configMap)

	// Then
	require.NoError(t, err)

	assert.Len(t, accessProviderHandlerMock.AccessProviderFeedback, 2)
	assert.ElementsMatch(t, []sync_to_target.AccessProviderSyncFeedback{
		{
			AccessProvider: "filter-ap-id1",
			ActualName:     "raito_table-1_filter",
			ExternalId:     ptr.String("metastore-id1.catalog-1.schema-1.table-1.filter"),
		},
		{
			AccessProvider: "filter-ap-id2",
			ActualName:     "raito_table-1_filter",
			ExternalId:     ptr.String("metastore-id1.catalog-1.schema-2.table-1.filter"),
		},
	}, accessProviderHandlerMock.AccessProviderFeedback)
}

func TestAccessSyncer_SyncAccessProviderToTarget_withFilters_singleTable(t *testing.T) {
	// Given
	deployment := "test-deployment"
	workspace := "test-workspace"
	accessSyncer, mockAccountRepo, mockWorkspaceRepoMap := createAccessSyncer(t, deployment)

	accessProviderHandlerMock := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	accessProviders := sync_to_target.AccessProviderImport{
		AccessProviders: []*sync_to_target.AccessProvider{
			{
				Id:         "filter-ap-id1",
				Name:       "filter-ap-1",
				NamingHint: "filter-ap-1",
				Action:     sync_to_target.Filtered,
				What: []sync_to_target.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "metastore-id1.catalog-1.schema-1.table-1",
							Type:     data_source.Table,
						},
					},
				},
				Who: sync_to_target.WhoItem{
					Users:  []string{"ruben@raito.io"},
					Groups: []string{"group1"},
				},
				DeletedWho: &sync_to_target.WhoItem{
					Users: []string{"dieter@raito.io"},
				},
				FilterCriteria: &bexpression.DataComparisonExpression{
					Comparison: &datacomparison.DataComparison{
						LeftOperand: datacomparison.Operand{
							Reference: &datacomparison.Reference{
								EntityType: datacomparison.EntityTypeDataObject,
								EntityID:   `{"fullName":"metastore-id1.catalog-1.schema-1.table-1.column1","id":"LXDVAhFywOe9hfIRC4ubm","type":"column"}`,
							},
						},
						Operator: datacomparison.ComparisonOperatorGreaterThanOrEqual,
						RightOperand: datacomparison.Operand{
							Literal: &datacomparison.Literal{
								Float: ptr.Float64(3.14),
							},
						},
					},
				},
			},
			{
				Id:         "filter-ap-id2",
				Name:       "filter-ap-2",
				NamingHint: "filter-ap-2",
				Action:     sync_to_target.Filtered,
				What: []sync_to_target.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "metastore-id1.catalog-1.schema-1.table-1",
							Type:     data_source.Table,
						},
					},
				},
				Who: sync_to_target.WhoItem{
					Groups: []string{"group2"},
				},
				PolicyRule: ptr.String("{refColumn} = 'NJ'"),
			},
		},
	}

	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			DatabricksAccountId:     "AccountId",
			DatabricksUser:          "User",
			DatabricksPassword:      "Password",
			DatabricksSqlWarehouses: fmt.Sprintf(`{"metastore-id1": {"workspace": "%s", "warehouse": "sqlWarehouse1"}}`, deployment),
			DatabricksPlatform:      "AWS",
		},
	}

	metastore1 := catalog.MetastoreInfo{
		Name:        "metastore1",
		MetastoreId: "metastore-id1",
	}

	workspaceObject := repo.Workspace{
		WorkspaceId:     42,
		DeploymentName:  deployment,
		WorkspaceName:   workspace,
		WorkspaceStatus: "RUNNING",
	}

	mockAccountRepo.EXPECT().ListMetastores(mock.Anything).Return([]catalog.MetastoreInfo{metastore1}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaces(mock.Anything).Return([]repo.Workspace{workspaceObject}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaceMap(mock.Anything, []catalog.MetastoreInfo{metastore1}, []repo.Workspace{workspaceObject}).Return(map[string][]string{metastore1.MetastoreId: {deployment}}, nil, nil).Once()

	mockWarehouseRepo := repo.NewMockWarehouseRepository(t)

	mockWorkspaceRepoMap[deployment].EXPECT().SqlWarehouseRepository("sqlWarehouse1").Return(mockWarehouseRepo)
	mockWorkspaceRepoMap[deployment].EXPECT().GetOwner(mock.Anything, catalog.SecurableTypeTable, "catalog-1.schema-1.table-1").Return("owner@raito.io", nil).Once()
	mockWorkspaceRepoMap[deployment].EXPECT().SetPermissionsOnResource(mock.Anything, catalog.SecurableTypeFunction, "catalog-1.schema-1.raito_table-1_filter", catalog.PermissionsChange{Add: []catalog.Privilege{catalog.PrivilegeExecute}, Principal: "owner@raito.io"}).Return(nil)

	mockWarehouseRepo.EXPECT().GetTableInformation(mock.Anything, "catalog-1", "schema-1", "table-1").Return(map[string]*repo.ColumnInformation{
		"column1": {
			Type: "float",
			Name: "column1",
		},
		"refColumn": {
			Name: "refColumn",
			Type: "string",
		},
	}, nil)

	var arguments []string

	c := mockWarehouseRepo.EXPECT().ExecuteStatement(mock.Anything, "catalog-1", "schema-1", mock.AnythingOfType("string")).RunAndReturn(func(_ context.Context, _ string, _ string, query string, _ ...sql.StatementParameterListItem) (*sql.ExecuteStatementResponse, error) {
		query1 := "CREATE OR REPLACE FUNCTION raito_table-1_filter(refColumn string, column1 float)\n RETURN ((current_user() IN ('ruben@raito.io') OR is_account_group_member('group1')) AND ((column1 >= 3.140000))) OR ((is_account_group_member('group2')) AND (refColumn = 'NJ'));"
		query2 := "CREATE OR REPLACE FUNCTION raito_table-1_filter(column1 float, refColumn string)\n RETURN ((current_user() IN ('ruben@raito.io') OR is_account_group_member('group1')) AND ((column1 >= 3.140000))) OR ((is_account_group_member('group2')) AND (refColumn = 'NJ'));"

		if query == query1 {
			arguments = append(arguments, "refColumn", "column1")
		} else if query == query2 {
			arguments = append(arguments, "column1", "refColumn")
		} else {
			assert.Failf(t, "Unexpected query: %s NOT IN %v", query, []string{query1, query2})
		}

		return nil, nil
	}).Once()

	mockWarehouseRepo.EXPECT().SetRowFilter(mock.Anything, "catalog-1", "schema-1", "table-1", "raito_table-1_filter", mock.AnythingOfType("[]string")).RunAndReturn(func(_ context.Context, _ string, _ string, _ string, _ string, actualArgs []string) error {
		assert.Equal(t, arguments, actualArgs)

		return nil
	}).NotBefore(c)

	// When
	err := accessSyncer.SyncAccessProviderToTarget(context.Background(), &accessProviders, accessProviderHandlerMock, configMap)

	// Then
	require.NoError(t, err)

	assert.Len(t, accessProviderHandlerMock.AccessProviderFeedback, 2)
	assert.ElementsMatch(t, []sync_to_target.AccessProviderSyncFeedback{
		{
			AccessProvider: "filter-ap-id1",
			ActualName:     "raito_table-1_filter",
			ExternalId:     ptr.String("metastore-id1.catalog-1.schema-1.table-1.filter"),
		},
		{
			AccessProvider: "filter-ap-id2",
			ActualName:     "raito_table-1_filter",
			ExternalId:     ptr.String("metastore-id1.catalog-1.schema-1.table-1.filter"),
		},
	}, accessProviderHandlerMock.AccessProviderFeedback)
}

func TestAccessSyncer_SyncAccessProviderToTarget_withFilters_deletedFilter(t *testing.T) {
	// Given
	deployment := "test-deployment"
	workspace := "test-workspace"
	accessSyncer, mockAccountRepo, mockWorkspaceRepoMap := createAccessSyncer(t, deployment)

	accessProviderHandlerMock := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	accessProviders := sync_to_target.AccessProviderImport{
		AccessProviders: []*sync_to_target.AccessProvider{
			{
				Id:         "filter-ap-id1",
				Name:       "filter-ap-1",
				NamingHint: "filter-ap-1",
				Action:     sync_to_target.Filtered,
				Delete:     true,
				What: []sync_to_target.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "metastore-id1.catalog-1.schema-1.table-1",
							Type:     data_source.Table,
						},
					},
				},
				Who: sync_to_target.WhoItem{
					Users:  []string{"ruben@raito.io"},
					Groups: []string{"group1"},
				},
				DeletedWho: &sync_to_target.WhoItem{
					Users: []string{"dieter@raito.io"},
				},
				FilterCriteria: &bexpression.DataComparisonExpression{
					Comparison: &datacomparison.DataComparison{
						LeftOperand: datacomparison.Operand{
							Reference: &datacomparison.Reference{
								EntityType: datacomparison.EntityTypeDataObject,
								EntityID:   `{"fullName":"metastore-id1.catalog-1.schema-1.table-1.column1","id":"LXDVAhFywOe9hfIRC4ubm","type":"column"}`,
							},
						},
						Operator: datacomparison.ComparisonOperatorGreaterThanOrEqual,
						RightOperand: datacomparison.Operand{
							Literal: &datacomparison.Literal{
								Float: ptr.Float64(3.14),
							},
						},
					},
				},
			},
		},
	}

	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			DatabricksAccountId:     "AccountId",
			DatabricksUser:          "User",
			DatabricksPassword:      "Password",
			DatabricksSqlWarehouses: fmt.Sprintf(`{"metastore-id1": {"workspace": "%s", "warehouse": "sqlWarehouse1"}}`, deployment),
			DatabricksPlatform:      "AWS",
		},
	}

	metastore1 := catalog.MetastoreInfo{
		Name:        "metastore1",
		MetastoreId: "metastore-id1",
	}

	workspaceObject := repo.Workspace{
		WorkspaceId:     42,
		DeploymentName:  deployment,
		WorkspaceName:   workspace,
		WorkspaceStatus: "RUNNING",
	}

	mockAccountRepo.EXPECT().ListMetastores(mock.Anything).Return([]catalog.MetastoreInfo{metastore1}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaces(mock.Anything).Return([]repo.Workspace{workspaceObject}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaceMap(mock.Anything, []catalog.MetastoreInfo{metastore1}, []repo.Workspace{workspaceObject}).Return(map[string][]string{metastore1.MetastoreId: {deployment}}, nil, nil).Once()

	mockWarehouseRepo := repo.NewMockWarehouseRepository(t)

	mockWorkspaceRepoMap[deployment].EXPECT().SqlWarehouseRepository("sqlWarehouse1").Return(mockWarehouseRepo)

	mockWarehouseRepo.EXPECT().DropRowFilter(mock.Anything, "catalog-1", "schema-1", "table-1").Return(nil)
	mockWarehouseRepo.EXPECT().DropFunction(mock.Anything, "catalog-1", "schema-1", "raito_table-1_filter").Return(nil)

	// When
	err := accessSyncer.SyncAccessProviderToTarget(context.Background(), &accessProviders, accessProviderHandlerMock, configMap)

	// Then
	require.NoError(t, err)

	assert.Len(t, accessProviderHandlerMock.AccessProviderFeedback, 1)
	assert.ElementsMatch(t, []sync_to_target.AccessProviderSyncFeedback{
		{
			AccessProvider: "filter-ap-id1",
			ActualName:     "raito_table-1_filter",
			ExternalId:     ptr.String("metastore-id1.catalog-1.schema-1.table-1.filter"),
		},
	}, accessProviderHandlerMock.AccessProviderFeedback)
}

func TestAccessSyncer_SyncAccessProviderToTarget_withErrors(t *testing.T) {
	// Given
	deployment := "test-deployment"
	workspace := "test-workspace"
	accessSyncer, mockAccountRepo, mockWorkspaceRepoMap := createAccessSyncer(t, deployment)

	accessProviderHandlerMock := mocks.NewSimpleAccessProviderFeedbackHandler(t)

	accessProviders := sync_to_target.AccessProviderImport{
		AccessProviders: []*sync_to_target.AccessProvider{
			{
				Id:     "workspace-ap-id",
				Name:   "workspace-ap",
				Action: sync_to_target.Grant,
				What: []sync_to_target.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "42",
							Type:     workspaceType,
						},
						Permissions: []string{"USER"},
					},
				},
				Who: sync_to_target.WhoItem{
					Users:  []string{"ruben@raito.io"},
					Groups: []string{"group1"},
				},
				DeletedWho: &sync_to_target.WhoItem{
					Users: []string{"dieter@raito.io"},
				},
			},
			{
				Id:     "catalog-ap-id",
				Name:   "catalog-ap",
				Action: sync_to_target.Grant,
				What: []sync_to_target.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "metastore-id1.catalog-1",
							Type:     catalogType,
						},
						Permissions: []string{"SELECT"},
					},
				},
				Who: sync_to_target.WhoItem{
					Users: []string{"wannes@raito.io"},
				},
				DeletedWho: &sync_to_target.WhoItem{
					Users: []string{"jonas@raito.io"},
				},
			},
			{
				Id:     "multiple-do-ap-id",
				Name:   "multiple-do-ap",
				Action: sync_to_target.Grant,
				What: []sync_to_target.WhatItem{
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "metastore-id1.catalog-1.schema-1",
							Type:     data_source.Schema,
						},
						Permissions: []string{"SELECT", "MODIFY"},
					},
					{
						DataObject: &data_source.DataObjectReference{
							FullName: "metastore-id1.catalog-2",
							Type:     catalogType,
						},
						Permissions: []string{"CREATE TABLE"},
					},
				},
				Who: sync_to_target.WhoItem{
					Users: []string{"bart@raito.io"},
				},
				DeletedWho: &sync_to_target.WhoItem{},
			},
		},
	}

	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			DatabricksAccountId: "AccountId",
			DatabricksUser:      "User",
			DatabricksPassword:  "Password",
			DatabricksPlatform:  "AWS",
		},
	}

	metastore1 := catalog.MetastoreInfo{
		Name:        "metastore1",
		MetastoreId: "metastore-id1",
	}

	workspaceObject := repo.Workspace{
		WorkspaceId:     42,
		DeploymentName:  deployment,
		WorkspaceName:   workspace,
		WorkspaceStatus: "RUNNING",
	}

	mockAccountRepo.EXPECT().ListMetastores(mock.Anything).Return([]catalog.MetastoreInfo{metastore1}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaces(mock.Anything).Return([]repo.Workspace{workspaceObject}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaceMap(mock.Anything, []catalog.MetastoreInfo{metastore1}, []repo.Workspace{workspaceObject}).Return(map[string][]string{metastore1.MetastoreId: {deployment}}, nil, nil).Once()

	mockWorkspaceRepoMap[deployment].EXPECT().Ping(mock.Anything).Return(nil).Once()
	mockWorkspaceRepoMap[deployment].EXPECT().SetPermissionsOnResource(mock.Anything, catalog.SecurableTypeCatalog, "catalog-1", mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, securableType catalog.SecurableType, s string, change ...catalog.PermissionsChange) error {
		assert.Len(t, change, 3)

		wannes := -1
		bart := -1
		jonas := -1

		for i, c := range change {
			switch c.Principal {
			case "bart@raito.io":
				bart = i
			case "wannes@raito.io":
				wannes = i
			case "jonas@raito.io":
				jonas = i
			}
		}

		require.NotEqual(t, -1, bart)
		require.NotEqual(t, -1, wannes)
		require.NotEqual(t, -1, jonas)

		assert.ElementsMatch(t, []catalog.Privilege{catalog.PrivilegeUseCatalog, catalog.PrivilegeSelect}, change[wannes].Add)
		assert.ElementsMatch(t, []catalog.Privilege{catalog.PrivilegeUseCatalog}, change[bart].Add)
		assert.ElementsMatch(t, []catalog.Privilege{}, change[jonas].Add)
		assert.ElementsMatch(t, []catalog.Privilege{}, change[bart].Remove)
		assert.ElementsMatch(t, []catalog.Privilege{}, change[wannes].Remove)
		assert.ElementsMatch(t, []catalog.Privilege{catalog.PrivilegeSelect}, change[jonas].Remove) // USE CATALOG should not be removed

		return nil
	}).Once()
	mockWorkspaceRepoMap[deployment].EXPECT().SetPermissionsOnResource(mock.Anything, catalog.SecurableTypeCatalog, "catalog-2", mock.Anything).RunAndReturn(func(_ context.Context, securableType catalog.SecurableType, s string, change ...catalog.PermissionsChange) error {
		assert.ElementsMatch(t, []catalog.Privilege{catalog.PrivilegeCreateTable, catalog.PrivilegeUseCatalog}, change[0].Add)

		return nil
	}).Once()
	mockWorkspaceRepoMap[deployment].EXPECT().SetPermissionsOnResource(mock.Anything, catalog.SecurableTypeSchema, "catalog-1.schema-1", mock.Anything).RunAndReturn(func(ctx context.Context, securableType catalog.SecurableType, s string, change ...catalog.PermissionsChange) error {
		assert.ElementsMatch(t, []catalog.Privilege{catalog.PrivilegeModify, catalog.PrivilegeUseSchema, catalog.PrivilegeSelect}, change[0].Add)
		assert.Equal(t, "bart@raito.io", change[0].Principal)

		return errors.New("boom")
	}).Once()

	mockAccountRepo.EXPECT().ListUsers(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, f ...func(filter *repo.DatabricksUsersFilter)) <-chan interface{} {
		options := repo.DatabricksUsersFilter{}
		for _, fn := range f {
			fn(&options)
		}

		require.NotNil(t, options.Username)

		if *options.Username == "ruben@raito.io" {
			return array.ArrayToChannel([]interface{}{
				iam.User{
					DisplayName: "Ruben Mennes",
					Id:          "314",
				},
			})
		} else if *options.Username == "dieter@raito.io" {
			return array.ArrayToChannel([]interface{}{
				iam.User{
					DisplayName: "Dieter Wachters",
					Id:          "1592",
				},
			})
		} else {
			assert.Fail(t, "unexpected username")
		}

		return array.ArrayToChannel[interface{}]([]interface{}{})
	})
	mockAccountRepo.EXPECT().ListGroups(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, f ...func(filter *repo.DatabricksGroupsFilter)) <-chan interface{} {
		options := repo.DatabricksGroupsFilter{}
		for _, fn := range f {
			fn(&options)
		}

		require.NotNil(t, options.Groupname)
		require.Equal(t, "group1", *options.Groupname)

		return array.ArrayToChannel([]interface{}{iam.Group{DisplayName: "group1", Id: "6535"}})
	})
	mockAccountRepo.EXPECT().UpdateWorkspaceAssignment(mock.Anything, 42, int64(314), []iam.WorkspacePermission{iam.WorkspacePermissionUser}).Return(nil).Once()
	mockAccountRepo.EXPECT().UpdateWorkspaceAssignment(mock.Anything, 42, int64(6535), []iam.WorkspacePermission{iam.WorkspacePermissionUser}).Return(nil).Once()
	mockAccountRepo.EXPECT().UpdateWorkspaceAssignment(mock.Anything, 42, int64(1592), []iam.WorkspacePermission{}).Return(nil).Once()

	// When
	err := accessSyncer.SyncAccessProviderToTarget(context.Background(), &accessProviders, accessProviderHandlerMock, configMap)

	// Then
	require.NoError(t, err)

	assert.Len(t, accessProviderHandlerMock.AccessProviderFeedback, 3)
	assert.ElementsMatch(t, []sync_to_target.AccessProviderSyncFeedback{
		{
			AccessProvider: "workspace-ap-id",
			ActualName:     "workspace-ap-id",
			Type:           ptr.String(access_provider.AclSet),
		},
		{
			AccessProvider: "catalog-ap-id",
			ActualName:     "catalog-ap-id",
			Type:           ptr.String(access_provider.AclSet),
		},
		{
			AccessProvider: "multiple-do-ap-id",
			ActualName:     "multiple-do-ap-id",
			Type:           ptr.String(access_provider.AclSet),
			Errors:         []string{"set permissions on schema \"catalog-1.schema-1\": boom"},
		},
	}, accessProviderHandlerMock.AccessProviderFeedback)
}

func createAccessSyncer(t *testing.T, deployments ...string) (*AccessSyncer, *mockDataAccessAccountRepository, map[string]*mockDataAccessWorkspaceRepository) {
	t.Helper()

	accountRepo := newMockDataAccessAccountRepository(t)
	workspaceMockRepos := make(map[string]*mockDataAccessWorkspaceRepository)
	for _, deployment := range deployments {
		workspaceMockRepos[deployment] = newMockDataAccessWorkspaceRepository(t)
	}

	return &AccessSyncer{
		accountRepoFactory: func(pltfrm platform.DatabricksPlatform, accountId string, repoCredentials *repo.RepositoryCredentials) (dataAccessAccountRepository, error) {
			return accountRepo, nil
		},
		workspaceRepoFactory: func(pltfrm platform.DatabricksPlatform, host string, accountId string, repoCredentials *repo.RepositoryCredentials) (dataAccessWorkspaceRepository, error) {
			deploymentRegex := regexp.MustCompile("https://([a-zA-Z0-9_-]*).cloud.databricks.com")

			deployment := deploymentRegex.ReplaceAllString(host, "${1}")

			if workspaceMock, ok := workspaceMockRepos[deployment]; ok {
				return workspaceMock, nil
			}

			return nil, errors.New("no workspace repository")
		},

		privilegeCache: types.NewPrivilegeCache(),
	}, accountRepo, workspaceMockRepos
}
