package databricks

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/raito-io/cli/base/access_provider/sync_from_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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
		},
	}

	accessSyncer.privilegeCache.AddPrivilege(data_source.DataObjectReference{FullName: "metastore-id1.catalog-1.schema-1", Type: data_source.Schema}, "principal1", "SELECT")

	metastore1 := catalog.MetastoreInfo{
		Name:        "metastore1",
		MetastoreId: "metastore-id1",
	}

	workspaceObject := Workspace{
		WorkspaceId:     42,
		DeploymentName:  deployment,
		WorkspaceName:   workspace,
		WorkspaceStatus: "RUNNING",
	}

	mockAccountRepo.EXPECT().ListMetastores(mock.Anything).Return([]catalog.MetastoreInfo{metastore1}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaces(mock.Anything).Return([]Workspace{workspaceObject}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaceMap(mock.Anything, []catalog.MetastoreInfo{metastore1}, []Workspace{workspaceObject}).Return(map[string][]string{metastore1.MetastoreId: {deployment}}, nil).Once()
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

	mockWorkspaceRepoMap[deployment].EXPECT().Ping(mock.Anything).Return(nil).Once()
	mockWorkspaceRepoMap[deployment].EXPECT().GetPermissionsOnResource(mock.Anything, catalog.SecurableTypeMetastore, "metastore-id1").Return(nil, nil).Once()

	mockWorkspaceRepoMap[deployment].EXPECT().ListCatalogs(mock.Anything).Return([]catalog.CatalogInfo{
		{
			Name:        "catalog-1",
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
					Permissions: []string{"USE_CATALOG"},
				},
			},
		},
		{
			ExternalId: "metastore-id1.catalog-1_EXECUTE",
			Name:       "metastore-id1.catalog-1_EXECUTE",
			NamingHint: "metastore-id1.catalog-1_EXECUTE",
			ActualName: "metastore-id1.catalog-1_EXECUTE",
			Action:     sync_from_target.Grant,
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
	})
}

func TestAccessSyncer_SyncAccessProviderToTarget(t *testing.T) {
	// Given
	deployment := "test-deployment"
	workspace := "test-workspace"
	accessSyncer, mockAccountRepo, mockWorkspaceRepoMap := createAccessSyncer(t, deployment)

	accessProviderHandlerMock := mocks.NewSimpleAccessProviderFeedbackHandler(t, 1)

	accessProviders := sync_to_target.AccessProviderImport{
		AccessProviders: []*sync_to_target.AccessProvider{
			{
				Name: "workspace-ap",
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
					UsersInheritedNativeGroupsExcluded: []string{"ruben@raito.io"},
					NativeGroupsInherited:              []string{"group1"},
				},
				DeletedWho: &sync_to_target.WhoItem{
					UsersInheritedNativeGroupsExcluded: []string{"dieter@raito.io"},
				},
			},
			{
				Name: "catalog-ap",
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
					UsersInheritedNativeGroupsExcluded: []string{"wannes@raito.io"},
				},
				DeletedWho: &sync_to_target.WhoItem{
					UsersInheritedNativeGroupsExcluded: []string{"jonas@raito.io"},
				},
			},
			{
				Name: "multiple-do-ap",
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
					UsersInheritedNativeGroupsExcluded: []string{"bart@raito.io"},
					Groups:                             []string{"group2"},
				},
				DeletedWho: &sync_to_target.WhoItem{
					Groups: []string{"group3"},
				},
			},
		},
	}

	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			DatabricksAccountId: "AccountId",
			DatabricksUser:      "User",
			DatabricksPassword:  "Password",
		},
	}

	metastore1 := catalog.MetastoreInfo{
		Name:        "metastore1",
		MetastoreId: "metastore-id1",
	}

	workspaceObject := Workspace{
		WorkspaceId:     42,
		DeploymentName:  deployment,
		WorkspaceName:   workspace,
		WorkspaceStatus: "RUNNING",
	}

	mockAccountRepo.EXPECT().ListMetastores(mock.Anything).Return([]catalog.MetastoreInfo{metastore1}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaces(mock.Anything).Return([]Workspace{workspaceObject}, nil).Once()
	mockAccountRepo.EXPECT().GetWorkspaceMap(mock.Anything, []catalog.MetastoreInfo{metastore1}, []Workspace{workspaceObject}).Return(map[string][]string{metastore1.MetastoreId: {deployment}}, nil).Once()

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

	mockAccountRepo.EXPECT().ListUsers(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, f ...func(*databricksUsersFilter)) <-chan interface{} {
		options := databricksUsersFilter{}
		for _, fn := range f {
			fn(&options)
		}

		require.NotNil(t, options.username)

		if *options.username == "ruben@raito.io" {
			return array.ArrayToChannel([]interface{}{
				iam.User{
					DisplayName: "Ruben Mennes",
					Id:          "314",
				},
			})
		} else if *options.username == "dieter@raito.io" {
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
	mockAccountRepo.EXPECT().ListGroups(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, f ...func(*databricksGroupsFilter)) <-chan interface{} {
		options := databricksGroupsFilter{}
		for _, fn := range f {
			fn(&options)
		}

		require.NotNil(t, options.groupname)
		require.Equal(t, "group1", *options.groupname)

		return array.ArrayToChannel([]interface{}{iam.Group{DisplayName: "group1", Id: "6535"}})
	})
	mockAccountRepo.EXPECT().UpdateWorkspaceAssignment(mock.Anything, 42, int64(314), []iam.WorkspacePermission{iam.WorkspacePermissionUser}).Return(nil).Once()
	mockAccountRepo.EXPECT().UpdateWorkspaceAssignment(mock.Anything, 42, int64(6535), []iam.WorkspacePermission{iam.WorkspacePermissionUser}).Return(nil).Once()
	mockAccountRepo.EXPECT().UpdateWorkspaceAssignment(mock.Anything, 42, int64(1592), []iam.WorkspacePermission{}).Return(nil).Once()

	// When
	err := accessSyncer.SyncAccessProviderToTarget(context.Background(), &accessProviders, accessProviderHandlerMock, configMap)

	// Then
	require.NoError(t, err)
}

func createAccessSyncer(t *testing.T, deployments ...string) (*AccessSyncer, *mockDataAccessAccountRepository, map[string]*mockDataAccessWorkspaceRepository) {
	t.Helper()

	accountRepo := newMockDataAccessAccountRepository(t)
	workspaceMockRepos := make(map[string]*mockDataAccessWorkspaceRepository)
	for _, deployment := range deployments {
		workspaceMockRepos[deployment] = newMockDataAccessWorkspaceRepository(t)
	}

	return &AccessSyncer{
		accountRepoFactory: func(user string, password string, accountId string) dataAccessAccountRepository {
			return accountRepo
		},
		workspaceRepoFactory: func(host string, user string, password string) (dataAccessWorkspaceRepository, error) {
			deploymentRegex := regexp.MustCompile("https://([a-zA-Z0-9_-]*).cloud.databricks.com")

			deployment := deploymentRegex.ReplaceAllString(host, "${1}")

			if workspaceMock, ok := workspaceMockRepos[deployment]; ok {
				return workspaceMock, nil
			}

			return nil, errors.New("no workspace repository")
		},

		privilegeCache: NewPrivilegeCache(),
	}, accountRepo, workspaceMockRepos
}
