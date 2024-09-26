package databricks

import (
	"context"
	"fmt"

	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/databricks/databricks-sdk-go/service/provisioning"
	"github.com/raito-io/cli/base/tag"
	"github.com/raito-io/cli/base/util/config"

	"cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/databricks/repo"
	types2 "cli-plugin-databricks/databricks/repo/types"
	"cli-plugin-databricks/databricks/types"
	"cli-plugin-databricks/databricks/utils"
)

type DataSourceTagHandler struct {
	configMap            *config.ConfigMap
	warehouseIdMap       map[string]string //workspace -> warehouse id
	workspaceRepoFactory func(repoCredentials *types2.RepositoryCredentials) (dataSourceWorkspaceRepository, error)

	tagCache map[string][]*tag.Tag
}

func NewDataSourceTagHandler(configMap *config.ConfigMap, workspaceRepoFactory func(repoCredentials *types2.RepositoryCredentials) (dataSourceWorkspaceRepository, error)) (*DataSourceTagHandler, error) {
	var warehouseIds []types.WarehouseDetails

	if found, err := configMap.Unmarshal(constants.DatabricksSqlWarehouses, &warehouseIds); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", constants.DatabricksSqlWarehouses, err)
	} else if !found {
		logger.Warn("No warehouse id map found in config. Tags will not be loaded.")
	}

	warehouseIdMap := make(map[string]string)
	for _, details := range warehouseIds {
		warehouseIdMap[details.Workspace] = details.Warehouse
	}

	return &DataSourceTagHandler{
		configMap:            configMap,
		warehouseIdMap:       warehouseIdMap,
		workspaceRepoFactory: workspaceRepoFactory,
		tagCache:             make(map[string][]*tag.Tag),
	}, nil
}

func (d *DataSourceTagHandler) LoadTags(ctx context.Context, workspace *provisioning.Workspace, c *catalog.CatalogInfo) error {
	if d.warehouseIdMap == nil {
		return nil
	}

	logger.Info(fmt.Sprintf("Loading tags for catalog %s", c.FullName))

	d.tagCache = make(map[string][]*tag.Tag)

	workspaceRepo, sqlRepo, err := d.getSqlClient(workspace)
	if err != nil {
		return fmt.Errorf("get sql client: %w", err)
	}

	if sqlRepo == nil {
		logger.Warn(fmt.Sprintf("No warehouse found for metastore %s. Will ignore tags for catalog %q", c.MetastoreId, c.Name))

		return nil
	}

	me, err := workspaceRepo.Me(ctx)
	if err != nil {
		return fmt.Errorf("get me: %w", err)
	}

	err = d.setRequiredPermissions(ctx, workspaceRepo, me, c)
	if err != nil {
		return fmt.Errorf("set required permissions: %w", err)
	}

	err = sqlRepo.GetTags(ctx, c.Name, func(ctx context.Context, fullName string, key string, value string) error {
		d.tagCache[fullName] = append(d.tagCache[fullName], &tag.Tag{
			Key:    key,
			Value:  value,
			Source: constants.TagSource,
		})

		return nil
	})
	if err != nil {
		return fmt.Errorf("get tags: %w", err)
	}

	return nil
}

func (d *DataSourceTagHandler) GetTag(fullName string) []*tag.Tag {
	return d.tagCache[fullName]
}

func (d *DataSourceTagHandler) getSqlClient(workspace *provisioning.Workspace) (dataSourceWorkspaceRepository, repo.WarehouseRepository, error) {
	pltfrm, _, repoCredentials, err := utils.GetAndValidateParameters(d.configMap)
	if err != nil {
		return nil, nil, fmt.Errorf("get credentials: %w", err)
	}

	workspaceCredentials, err := utils.InitializeWorkspaceRepoCredentials(repoCredentials, pltfrm, workspace)
	if err != nil {
		return nil, nil, fmt.Errorf("initialize workspace credentials: %w", err)
	}

	workspaceRepo, err := d.workspaceRepoFactory(workspaceCredentials)
	if err != nil {
		return nil, nil, fmt.Errorf("create workspace repo: %w", err)
	}

	if warehouseId, found := d.warehouseIdMap[workspace.DeploymentName]; found {
		return workspaceRepo, workspaceRepo.SqlWarehouseRepository(warehouseId), nil
	}

	return workspaceRepo, nil, nil
}

func (d *DataSourceTagHandler) setRequiredPermissions(ctx context.Context, workspaceRepo dataSourceWorkspaceRepository, me *iam.User, c *catalog.CatalogInfo) error {
	err := workspaceRepo.SetPermissionsOnResource(ctx, catalog.SecurableTypeCatalog, c.FullName, catalog.PermissionsChange{
		Add:       []catalog.Privilege{catalog.PrivilegeUseCatalog},
		Principal: me.UserName,
	})
	if err != nil {
		return fmt.Errorf("use catalog permission: %w", err)
	}

	err = workspaceRepo.SetPermissionsOnResource(ctx, catalog.SecurableTypeSchema, fmt.Sprintf("%s.%s", c.FullName, "information_schema"), catalog.PermissionsChange{
		Add:       []catalog.Privilege{catalog.PrivilegeSelect, catalog.PrivilegeUseSchema},
		Principal: me.UserName,
	})
	if err != nil {
		return fmt.Errorf("use schema permission: %w", err)
	}

	return nil
}
