package databricks

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/databricks/databricks-sdk-go/service/catalog"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"

	"cli-plugin-databricks/databricks/repo"
)

var _ wrappers.DataSourceSyncer = (*DataSourceSyncer)(nil)

//go:generate go run github.com/vektra/mockery/v2 --name=dataSourceWorkspaceRepository
type dataSourceWorkspaceRepository interface {
	Ping(ctx context.Context) error
	workspaceRepository
}

type DataSourceSyncer struct {
	accountRepoFactory   func(user string, repoCredentials *repo.RepositoryCredentials) accountRepository
	workspaceRepoFactory func(host string, accountId string, repoCredentials *repo.RepositoryCredentials) (dataSourceWorkspaceRepository, error)

	functionUsedAsMask set.Set[string]
}

func NewDataSourceSyncer() *DataSourceSyncer {
	return &DataSourceSyncer{
		accountRepoFactory: func(accountId string, repoCredentials *repo.RepositoryCredentials) accountRepository {
			return repo.NewAccountRepository(repoCredentials, accountId)
		},
		workspaceRepoFactory: func(host string, accountId string, repoCredentials *repo.RepositoryCredentials) (dataSourceWorkspaceRepository, error) {
			return repo.NewWorkspaceRepository(host, accountId, repoCredentials)
		},

		functionUsedAsMask: set.NewSet[string](),
	}
}

func (d *DataSourceSyncer) GetDataSourceMetaData(_ context.Context, _ *config.ConfigMap) (*ds.MetaData, error) {
	logger.Debug("Returning meta data for databricks data source")

	return &databricks_metadata, nil
}

func (d *DataSourceSyncer) SyncDataSource(ctx context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, configParams *config.ConfigMap) (err error) {
	defer func() {
		if err != nil {
			logger.Error(fmt.Sprintf("SyncDataSource failed: %s", err.Error()))
		}
	}()

	accountId, repoCredentials, err := getAndValidateParameters(configParams)
	if err != nil {
		return err
	}

	dataSourceHandler.SetDataSourceFullname(accountId)
	dataSourceHandler.SetDataSourceName(accountId)

	traverser := NewDataObjectTraverser(func() (accountRepository, error) {
		return d.accountRepoFactory(accountId, &repoCredentials), nil
	}, func(metastoreWorkspaces []string) (workspaceRepository, string, error) {
		return selectWorkspaceRepo(ctx, &repoCredentials, accountId, metastoreWorkspaces, d.workspaceRepoFactory)
	})

	err = traverser.Traverse(ctx, func(ctx context.Context, securableType string, parentObject interface{}, object interface{}, _ *string) error {
		switch securableType {
		case metastoreType:
			return d.parseMetastore(ctx, dataSourceHandler, object)
		case workspaceType:
			return d.parseWorkspace(ctx, dataSourceHandler, object)
		case catalogType:
			return d.parseCatalog(ctx, dataSourceHandler, object)
		case ds.Schema:
			return d.parseSchema(ctx, dataSourceHandler, parentObject, object)
		case ds.Table:
			return d.parseTable(ctx, dataSourceHandler, parentObject, object)
		case ds.Column:
			return d.parseColumn(ctx, dataSourceHandler, parentObject, object)
		case functionType:
			return d.parseFunctions(ctx, dataSourceHandler, parentObject, object)
		}

		return fmt.Errorf("unsupported type: %s", securableType)
	}, func(traverserOptions *DataObjectTraverserOptions) {
		traverserOptions.SecurableTypesToReturn = set.NewSet[string](metastoreType, workspaceType, catalogType, ds.Schema, ds.Table, ds.Column, functionType)
	})

	if err != nil {
		return err
	}

	return nil
}

func (d *DataSourceSyncer) parseMetastore(_ context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, object interface{}) error {
	metastore, ok := object.(*catalog.MetastoreInfo)
	if !ok {
		return fmt.Errorf("unable to parse MetastoreInfo object. Expected *catalog.MetastoreInfo but got %T", object)
	}

	return dataSourceHandler.AddDataObjects(&ds.DataObject{
		Name:       metastore.Name,
		Type:       metastoreType,
		FullName:   metastore.MetastoreId,
		ExternalId: metastore.MetastoreId,
	})
}

func (d *DataSourceSyncer) parseWorkspace(_ context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, object interface{}) error {
	workspace, ok := object.(*repo.Workspace)
	if !ok {
		return fmt.Errorf("unable to parse Workspace object. Expected *repo.Workspace but got %T", object)
	}

	id := strconv.Itoa(workspace.WorkspaceId)

	return dataSourceHandler.AddDataObjects(&ds.DataObject{
		Name:       workspace.WorkspaceName,
		FullName:   id,
		Type:       workspaceType,
		ExternalId: id,
	})
}

func (d *DataSourceSyncer) parseCatalog(_ context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, object interface{}) error {
	c, ok := object.(*catalog.CatalogInfo)
	if !ok {
		return fmt.Errorf("unable to parse CatalogInfo object. Expected *catalog.CatalogInfo but got %T", object)
	}

	uniqueId := createUniqueId(c.MetastoreId, c.Name)

	return dataSourceHandler.AddDataObjects(&ds.DataObject{
		Name:             c.Name,
		ExternalId:       uniqueId,
		ParentExternalId: c.MetastoreId,
		Description:      c.Comment,
		FullName:         uniqueId,
		Type:             catalogType,
	})
}

func (d *DataSourceSyncer) parseSchema(_ context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, parent interface{}, object interface{}) error {
	schema, ok := object.(*catalog.SchemaInfo)
	if !ok {
		return fmt.Errorf("unable to parse SchemaInfo object. Expected *catalog.SchemaInfo but got %T", object)
	}

	c, ok := parent.(*catalog.CatalogInfo)
	if !ok {
		return fmt.Errorf("unable to parse parent CatalogInfo object. Expected *catalog.CatalogInfo but got %T", object)
	}

	uniqueId := createUniqueId(schema.MetastoreId, schema.FullName)
	parentId := createUniqueId(c.MetastoreId, c.FullName)

	return dataSourceHandler.AddDataObjects(&ds.DataObject{
		Name:             schema.Name,
		ExternalId:       uniqueId,
		ParentExternalId: parentId,
		Description:      schema.Comment,
		FullName:         uniqueId,
		Type:             ds.Schema,
	})
}

func (d *DataSourceSyncer) parseTable(_ context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, parent interface{}, object interface{}) error {
	table, ok := object.(*catalog.TableInfo)
	if !ok {
		return fmt.Errorf("unable to parse TableInfo object. Expected *catalog.TableInfo but got %T", object)
	}

	schema, ok := parent.(*catalog.SchemaInfo)
	if !ok {
		return fmt.Errorf("unable to parse parent SchemaInfo object. Expected *catalog.SchemaInfo but got %T", parent)
	}

	uniqueId := createUniqueId(table.MetastoreId, table.FullName)
	parentId := createUniqueId(schema.MetastoreId, schema.FullName)

	return dataSourceHandler.AddDataObjects(&ds.DataObject{
		Name:             table.Name,
		ExternalId:       uniqueId,
		ParentExternalId: parentId,
		Description:      table.Comment,
		FullName:         uniqueId,
		Type:             ds.Table,
	})
}

func (d *DataSourceSyncer) parseColumn(_ context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, parentObject interface{}, object interface{}) error {
	column, ok := object.(*catalog.ColumnInfo)
	if !ok {
		return fmt.Errorf("unable to parse ColumnInfo object. Expected *catalog.ColumnInfo but got %T", object)
	}

	table, ok := parentObject.(*catalog.TableInfo)
	if !ok {
		return fmt.Errorf("unable to parse parent TableInfo object. Expected *catalog.TableInfo but got %T", parentObject)
	}

	uniqueId := createTableUniqueId(table.MetastoreId, table.FullName, column.Name)
	parentId := createUniqueId(table.MetastoreId, table.FullName)

	if column.Mask != nil {
		d.functionUsedAsMask.Add(createUniqueId(table.MetastoreId, column.Mask.FunctionName))
	}

	return dataSourceHandler.AddDataObjects(&ds.DataObject{
		Name:             column.Name,
		ExternalId:       uniqueId,
		ParentExternalId: parentId,
		Description:      column.Comment,
		FullName:         uniqueId,
		Type:             ds.Column,
	})
}

func (d *DataSourceSyncer) parseFunctions(_ context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, parentObject interface{}, object interface{}) error {
	function, ok := object.(*repo.FunctionInfo)
	if !ok {
		return fmt.Errorf("unable to parse Function. Expected *catalog.FunctionInfo but got %T", object)
	}

	uniqueId := createUniqueId(function.MetastoreId, function.FullName)

	if d.functionUsedAsMask.Contains(uniqueId) {
		logger.Debug(fmt.Sprintf("Function %s used as mask. Will ignore function", uniqueId))

		return nil
	}

	schema, ok := parentObject.(*catalog.SchemaInfo)
	if !ok {
		return fmt.Errorf("unable to parse parent SchemaInfo object. Expected *catalog.SchemaInfo but got %T", parentObject)
	}

	parentId := createUniqueId(schema.MetastoreId, schema.FullName)

	return dataSourceHandler.AddDataObjects(&ds.DataObject{
		Name:             function.Name,
		ExternalId:       uniqueId,
		ParentExternalId: parentId,
		Description:      function.Comment,
		FullName:         uniqueId,
		Type:             functionType,
	})
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

func TableTypeToRaitoType(tType catalog.TableType) (string, error) {
	switch tType {
	case catalog.TableTypeManaged, catalog.TableTypeExternal, catalog.TableTypeStreamingTable:
		//Regular table
		return ds.Table, nil
	case catalog.TableTypeView, catalog.TableTypeMaterializedView:
		return ds.View, nil
	default:
		return "", fmt.Errorf("unknown table type %s", tType)
	}
}
