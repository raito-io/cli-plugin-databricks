package databricks

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/databricks/databricks-sdk-go/service/provisioning"
	"github.com/raito-io/cli/base/tag"
	"github.com/raito-io/cli/base/util/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/databricks/repo"
	"cli-plugin-databricks/databricks/repo/types"
	types2 "cli-plugin-databricks/databricks/types"
)

func TestDataSourceTagHandler_LoadTags(t *testing.T) {
	// Given
	deployment := "test-deployment"

	c := catalog.CatalogInfo{
		MetastoreId: "metastore1",
		Name:        "catalog1",
		FullName:    "catalog1",
	}

	workspaceMockRepos := make(map[string]*mockDataSourceWorkspaceRepository)
	workspaceRepoMock := newMockDataSourceWorkspaceRepository(t)
	workspaceMockRepos[deployment] = workspaceRepoMock

	sqlRepoMock := repo.NewMockWarehouseRepository(t)
	sqlRepoMock.EXPECT().GetTags(mock.Anything, c.FullName, mock.Anything).RunAndReturn(func(ctx context.Context, s string, f func(context.Context, string, string, string) error) error {
		err := f(ctx, "fn1", "key1", "value2")
		require.NoError(t, err)

		err = f(ctx, "fn2", "key3", "value4")
		require.NoError(t, err)

		err = f(ctx, "fn1", "key5", "value6")
		require.NoError(t, err)

		return nil
	})

	workspaceRepoMock.EXPECT().SqlWarehouseRepository("warehouseId").Return(sqlRepoMock)
	workspaceRepoMock.EXPECT().Me(mock.Anything).Return(&iam.User{UserName: "raito-user"}, nil).Once()
	workspaceRepoMock.EXPECT().SetPermissionsOnResource(mock.Anything, catalog.SecurableTypeCatalog, "catalog1", catalog.PermissionsChange{
		Add:       []catalog.Privilege{"USE_CATALOG"},
		Principal: "raito-user",
	}).Return(nil).Once()
	workspaceRepoMock.EXPECT().SetPermissionsOnResource(mock.Anything, catalog.SecurableTypeSchema, "catalog1.information_schema", catalog.PermissionsChange{
		Add:       []catalog.Privilege{"SELECT", "USE_SCHEMA"},
		Principal: "raito-user",
	}).Return(nil).Once()

	workspaceRepoFactory := func(repoCredentials *types.RepositoryCredentials) (dataSourceWorkspaceRepository, error) {
		deploymentRegex := regexp.MustCompile("https://([a-zA-Z0-9_-]*).cloud.databricks.com")

		deployment := deploymentRegex.ReplaceAllString(repoCredentials.Host, "${1}")

		if workspaceMock, ok := workspaceMockRepos[deployment]; ok {
			return workspaceMock, nil
		}

		return nil, errors.New("no workspace repository")
	}

	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			constants.DatabricksAccountId: "AccountId",
			constants.DatabricksUser:      "User",
			constants.DatabricksPassword:  "Password",
			constants.DatabricksPlatform:  "AWS",
		},
	}

	dstg := DataSourceTagHandler{
		tagCache:             make(map[string][]*tag.Tag),
		configMap:            configMap,
		workspaceRepoFactory: workspaceRepoFactory,
		warehouseIdMap: map[string]types2.WarehouseDetails{"metastore1": {
			Workspace: "workspaceId",
			Warehouse: "warehouseId",
		}},
	}

	// when
	err := dstg.LoadTags(context.Background(), &provisioning.Workspace{
		WorkspaceName:  "workspaceId",
		DeploymentName: "test-deployment",
	}, &c)

	// Then
	require.NoError(t, err)

	assert.Equal(t, map[string][]*tag.Tag{
		"fn1": {
			{
				Key:    "key1",
				Value:  "value2",
				Source: constants.TagSource,
			},
			{
				Key:    "key5",
				Value:  "value6",
				Source: constants.TagSource,
			},
		},
		"fn2": {
			{
				Key:    "key3",
				Value:  "value4",
				Source: constants.TagSource,
			},
		},
	}, dstg.tagCache)
}

func TestDataSourceTagHandler_LoadTags_WarehouseNotDefined(t *testing.T) {
	// Given
	deployment := "test-deployment"

	c := catalog.CatalogInfo{
		MetastoreId: "metastore1",
		Name:        "catalog1",
		FullName:    "catalog1",
	}

	workspaceMockRepos := make(map[string]*mockDataSourceWorkspaceRepository)
	workspaceRepoMock := newMockDataSourceWorkspaceRepository(t)
	workspaceMockRepos[deployment] = workspaceRepoMock

	workspaceRepoFactory := func(repoCredentials *types.RepositoryCredentials) (dataSourceWorkspaceRepository, error) {
		deploymentRegex := regexp.MustCompile("https://([a-zA-Z0-9_-]*).cloud.databricks.com")

		deployment := deploymentRegex.ReplaceAllString(repoCredentials.Host, "${1}")

		if workspaceMock, ok := workspaceMockRepos[deployment]; ok {
			return workspaceMock, nil
		}

		return nil, errors.New("no workspace repository")
	}

	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			constants.DatabricksAccountId: "AccountId",
			constants.DatabricksUser:      "User",
			constants.DatabricksPassword:  "Password",
			constants.DatabricksPlatform:  "AWS",
		},
	}

	dstg := DataSourceTagHandler{
		tagCache:             make(map[string][]*tag.Tag),
		configMap:            configMap,
		workspaceRepoFactory: workspaceRepoFactory,
		warehouseIdMap: map[string]types2.WarehouseDetails{"another": {
			Workspace: "workspaceId",
			Warehouse: "warehouseId",
		}},
	}

	// when
	err := dstg.LoadTags(context.Background(), &provisioning.Workspace{
		WorkspaceName:  "workspaceId",
		DeploymentName: "test-deployment",
	}, &c)

	// Then
	require.NoError(t, err)

	assert.Empty(t, dstg.tagCache)
}

func TestDataSourceTagHandler_GetTag(t *testing.T) {
	type fields struct {
		tagCache map[string][]*tag.Tag
	}
	type args struct {
		fullName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []*tag.Tag
	}{
		{
			name: "ExistingTag",
			fields: fields{
				tagCache: map[string][]*tag.Tag{"existingTag": {&tag.Tag{
					Key:    "key1",
					Value:  "value2",
					Source: "source3",
				}}},
			},
			args: args{
				fullName: "existingTag",
			},
			want: []*tag.Tag{{
				Key:    "key1",
				Value:  "value2",
				Source: "source3",
			}},
		},

		{
			name: "Non-ExistingTag",
			fields: fields{
				tagCache: map[string][]*tag.Tag{"existingTag": {&tag.Tag{
					Key:    "key1",
					Value:  "value2",
					Source: "source3",
				}}},
			},
			args: args{
				fullName: "non-existingTag",
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DataSourceTagHandler{
				tagCache: tt.fields.tagCache,
			}
			assert.Equalf(t, tt.want, d.GetTag(tt.args.fullName), "GetTag(%v)", tt.args.fullName)
		})
	}
}
