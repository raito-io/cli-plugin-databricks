package databricks

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/smithy-go/ptr"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/databricks/databricks-sdk-go/service/provisioning"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/tag"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"

	"cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/databricks/platform"
	"cli-plugin-databricks/databricks/repo"
	"cli-plugin-databricks/databricks/repo/types"
	"cli-plugin-databricks/databricks/utils"
)

var _ wrappers.DataSourceSyncer = (*DataSourceSyncer)(nil)

//go:generate go run github.com/vektra/mockery/v2 --name=dataSourceWorkspaceRepository
type dataSourceWorkspaceRepository interface {
	Ping(ctx context.Context) error
	SetPermissionsOnResource(ctx context.Context, securableType catalog.SecurableType, fullName string, changes ...catalog.PermissionsChange) error
	SqlWarehouseRepository(warehouseId string) repo.WarehouseRepository
	Me(ctx context.Context) (*iam.User, error)
	workspaceRepository
}

type DataSourceSyncer struct {
	accountRepoFactory   func(pltfrm platform.DatabricksPlatform, user string, repoCredentials *types.RepositoryCredentials) (accountRepository, error)
	workspaceRepoFactory func(repoCredentials *types.RepositoryCredentials) (dataSourceWorkspaceRepository, error)

	functionUsedAsMaskOrFilter set.Set[string]

	config *ds.DataSourceSyncConfig
}

func NewDataSourceSyncer() *DataSourceSyncer {
	return &DataSourceSyncer{
		accountRepoFactory: func(pltfrm platform.DatabricksPlatform, accountId string, repoCredentials *types.RepositoryCredentials) (accountRepository, error) {
			return repo.NewAccountRepository(pltfrm, repoCredentials, accountId)
		},
		workspaceRepoFactory: func(repoCredentials *types.RepositoryCredentials) (dataSourceWorkspaceRepository, error) {
			return repo.NewWorkspaceRepository(repoCredentials)
		},

		functionUsedAsMaskOrFilter: set.NewSet[string](),
	}
}

func (d *DataSourceSyncer) GetDataSourceMetaData(_ context.Context, _ *config.ConfigMap) (*ds.MetaData, error) {
	logger.Debug("Returning meta data for databricks data source")

	return &databricks_metadata, nil
}

func (d *DataSourceSyncer) SyncDataSource(ctx context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, config *ds.DataSourceSyncConfig) (err error) {
	d.config = config
	configParams := config.ConfigMap

	defer func() {
		if err != nil {
			logger.Error(fmt.Sprintf("SyncDataSource failed: %s", err.Error()))
		}
	}()

	pltfrm, accountId, repoCredentials, err := utils.GetAndValidateParameters(configParams)
	if err != nil {
		return err
	}

	dataSourceHandler.SetDataSourceFullname(accountId)
	dataSourceHandler.SetDataSourceName(accountId)

	traverser, err := NewDataObjectTraverser(config, func() (accountRepository, error) {
		return d.accountRepoFactory(pltfrm, accountId, &repoCredentials)
	}, func(metastoreWorkspace *provisioning.Workspace) (workspaceRepository, error) {
		return utils.InitWorkspaceRepo(ctx, repoCredentials, pltfrm, metastoreWorkspace, d.workspaceRepoFactory)
	}, createFullName)

	if err != nil {
		return fmt.Errorf("creating traverser: %w", err)
	}

	tags, err := NewDataSourceTagHandler(config.ConfigMap, d.workspaceRepoFactory)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to create tag handler: %s", err.Error()))

		tags = &DataSourceTagHandler{
			tagCache: make(map[string][]*tag.Tag),
		}
	}

	visitor := DataSourceVisitor{
		dataSourceHandler: dataSourceHandler,
		syncer:            d,
		tagHandler:        tags,
	}

	err = traverser.Traverse(ctx, visitor, func(traverserOptions *DataObjectTraverserOptions) {
		traverserOptions.SecurableTypesToReturn = set.NewSet[string](constants.MetastoreType, constants.WorkspaceType, constants.CatalogType, ds.Schema, ds.Table, ds.Column, constants.FunctionType)
	})

	if err != nil {
		return fmt.Errorf("traversing: %w", err)
	}

	return nil
}

func createFullName(securableType string, parent interface{}, object interface{}) string {
	switch securableType {
	case constants.MetastoreType:
		return object.(*catalog.MetastoreInfo).MetastoreId
	case constants.WorkspaceType:
		return strconv.FormatInt(object.(*provisioning.Workspace).WorkspaceId, 10)
	case constants.CatalogType:
		c := object.(*catalog.CatalogInfo)

		return createUniqueId(c.MetastoreId, c.Name)
	case ds.Schema:
		schema := object.(*catalog.SchemaInfo)

		return createUniqueId(schema.MetastoreId, schema.FullName)
	case ds.Table:
		table := object.(*catalog.TableInfo)

		return createUniqueId(table.MetastoreId, table.FullName)
	case ds.Column:
		column := object.(*catalog.ColumnInfo)
		table := parent.(*catalog.TableInfo)

		return createTableUniqueId(table.MetastoreId, table.FullName, column.Name)
	case constants.FunctionType:
		function := object.(*catalog.FunctionInfo)

		return createUniqueId(function.MetastoreId, function.FullName)
	}

	return ""
}

func createUniqueId(metastoreId string, fullName string) string {
	return fmt.Sprintf("%s.%s", metastoreId, fullName)
}

func createTableUniqueId(metastoreId string, tableFullName string, columnName string) string {
	return fmt.Sprintf("%s.%s.%s", metastoreId, tableFullName, columnName)
}

func getMetastoreAndFullnameOfUniqueId(uniqueId string) (string, string) {
	parts := strings.SplitN(uniqueId, ".", 2)
	return parts[0], parts[1]
}

var _ DataObjectVisitor = (*DataSourceVisitor)(nil)

type DataSourceVisitor struct {
	dataSourceHandler wrappers.DataSourceObjectHandler
	syncer            *DataSourceSyncer
	tagHandler        *DataSourceTagHandler
}

func (d DataSourceVisitor) VisitWorkspace(_ context.Context, workspace *provisioning.Workspace) error {
	id := strconv.FormatInt(workspace.WorkspaceId, 10)

	return d.dataSourceHandler.AddDataObjects(&ds.DataObject{
		Name:       workspace.WorkspaceName,
		FullName:   id,
		Type:       constants.WorkspaceType,
		ExternalId: id,
	})
}

func (d DataSourceVisitor) VisitMetastore(_ context.Context, metastore *catalog.MetastoreInfo, _ []*provisioning.Workspace) error {
	logger.Info(fmt.Sprintf("Found metastore %q: %+v", metastore.Name, metastore))

	return d.dataSourceHandler.AddDataObjects(&ds.DataObject{
		Name:       metastore.Name,
		Type:       constants.MetastoreType,
		FullName:   metastore.MetastoreId,
		ExternalId: metastore.MetastoreId,
	})
}

func (d DataSourceVisitor) VisitCatalog(ctx context.Context, c *catalog.CatalogInfo, _ *catalog.MetastoreInfo, workspace *provisioning.Workspace) error {
	err := d.tagHandler.LoadTags(ctx, workspace, c)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to load tags for catalog %q: %s", c.Name, err.Error()))
	}

	uniqueId := createUniqueId(c.MetastoreId, c.Name)

	return d.dataSourceHandler.AddDataObjects(&ds.DataObject{
		Name:             c.Name,
		ExternalId:       uniqueId,
		ParentExternalId: c.MetastoreId,
		Description:      c.Comment,
		FullName:         uniqueId,
		Type:             constants.CatalogType,
		Tags:             d.tagHandler.GetTag(c.FullName),
	})
}

func (d DataSourceVisitor) VisitSchema(_ context.Context, schema *catalog.SchemaInfo, c *catalog.CatalogInfo, _ *provisioning.Workspace) error {
	uniqueId := createUniqueId(schema.MetastoreId, schema.FullName)
	parentId := createUniqueId(c.MetastoreId, c.FullName)

	return d.dataSourceHandler.AddDataObjects(&ds.DataObject{
		Name:             schema.Name,
		ExternalId:       uniqueId,
		ParentExternalId: parentId,
		Description:      schema.Comment,
		FullName:         uniqueId,
		Type:             ds.Schema,
		Tags:             d.tagHandler.GetTag(schema.FullName),
	})
}

func (d DataSourceVisitor) VisitTable(_ context.Context, table *catalog.TableInfo, schema *catalog.SchemaInfo, _ *provisioning.Workspace) error {
	databricksTableType := table.TableType
	raitoTableType, found := TableTypeMap[databricksTableType]

	if !found {
		logger.Warn(fmt.Sprintf("Unsupported table type %q found for table %q", databricksTableType, table.FullName))

		return nil
	}

	if table.RowFilter != nil {
		if table.RowFilter.FunctionName == "" {
			// Currently all row filters are unknown due to a bug in Databricks
			logger.Warn(fmt.Sprintf("Unknown row filter applied to table %q", table.FullName))
		} else {
			logger.Debug(fmt.Sprintf("Row filter function %q found on table %q", table.RowFilter.FunctionName, table.FullName))
			d.syncer.functionUsedAsMaskOrFilter.Add(createUniqueId(table.MetastoreId, table.RowFilter.FunctionName))
		}
	}

	uniqueId := createUniqueId(table.MetastoreId, table.FullName)
	parentId := createUniqueId(schema.MetastoreId, schema.FullName)

	return d.dataSourceHandler.AddDataObjects(&ds.DataObject{
		Name:             table.Name,
		ExternalId:       uniqueId,
		ParentExternalId: parentId,
		Description:      table.Comment,
		FullName:         uniqueId,
		Type:             raitoTableType,
		Tags:             d.tagHandler.GetTag(table.FullName),
	})
}

func (d DataSourceVisitor) VisitColumn(_ context.Context, column *catalog.ColumnInfo, table *catalog.TableInfo, _ *provisioning.Workspace) error {
	uniqueId := createTableUniqueId(table.MetastoreId, table.FullName, column.Name)
	parentId := createUniqueId(table.MetastoreId, table.FullName)

	if column.Mask != nil {
		logger.Debug(fmt.Sprintf("Ignoring mask function: '%s'", column.Mask.FunctionName))
		d.syncer.functionUsedAsMaskOrFilter.Add(createUniqueId(table.MetastoreId, column.Mask.FunctionName))
	}

	return d.dataSourceHandler.AddDataObjects(&ds.DataObject{
		Name:             column.Name,
		ExternalId:       uniqueId,
		ParentExternalId: parentId,
		Description:      column.Comment,
		FullName:         uniqueId,
		Type:             ds.Column,
		Tags:             d.tagHandler.GetTag(table.FullName + "." + column.Name),
		DataType:         ptr.String(column.TypeName.String()),
	})
}

func (d DataSourceVisitor) VisitFunction(_ context.Context, function *catalog.FunctionInfo, schema *catalog.SchemaInfo, _ *provisioning.Workspace) error {
	uniqueId := createUniqueId(function.MetastoreId, function.FullName)

	if d.syncer.functionUsedAsMaskOrFilter.Contains(uniqueId) {
		logger.Debug(fmt.Sprintf("Function %s used as mask. Will ignore function", uniqueId))

		return nil
	}

	parentId := createUniqueId(schema.MetastoreId, schema.FullName)

	return d.dataSourceHandler.AddDataObjects(&ds.DataObject{
		Name:             function.Name,
		ExternalId:       uniqueId,
		ParentExternalId: parentId,
		Description:      function.Comment,
		FullName:         uniqueId,
		Type:             constants.FunctionType,
	})
}
