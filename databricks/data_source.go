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
)

var _ wrappers.DataSourceSyncer = (*DataSourceSyncer)(nil)

//go:generate go run github.com/vektra/mockery/v2 --name=dataSourceAccountRepository
type dataSourceAccountRepository interface {
	ListMetastores(ctx context.Context) ([]catalog.MetastoreInfo, error)
	GetWorkspaces(ctx context.Context) ([]Workspace, error)
	GetWorkspaceMap(ctx context.Context, metastores []catalog.MetastoreInfo, workspaces []Workspace) (map[string][]string, map[string]string, error)
}

//go:generate go run github.com/vektra/mockery/v2 --name=dataSourceWorkspaceRepository
type dataSourceWorkspaceRepository interface {
	Ping(ctx context.Context) error
	ListCatalogs(ctx context.Context) ([]catalog.CatalogInfo, error)
	ListSchemas(ctx context.Context, catalogName string) ([]catalog.SchemaInfo, error)
	ListTables(ctx context.Context, catalogName string, schemaName string) ([]catalog.TableInfo, error)
	ListFunctions(ctx context.Context, catalogName string, schemaName string) ([]catalog.FunctionInfo, error)
}

type DataSourceSyncer struct {
	accountRepoFactory   func(user string, repoCredentials RepositoryCredentials) dataSourceAccountRepository
	workspaceRepoFactory func(host string, repoCredentials RepositoryCredentials) (dataSourceWorkspaceRepository, error)
}

func NewDataSourceSyncer() *DataSourceSyncer {
	return &DataSourceSyncer{
		accountRepoFactory: func(accountId string, repoCredentials RepositoryCredentials) dataSourceAccountRepository {
			return NewAccountRepository(repoCredentials, accountId)
		},
		workspaceRepoFactory: func(host string, repoCredentials RepositoryCredentials) (dataSourceWorkspaceRepository, error) {
			return NewWorkspaceRepository(host, repoCredentials)
		},
	}
}

func (d *DataSourceSyncer) GetDataSourceMetaData(_ context.Context) (*ds.MetaData, error) {
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

	accountClient := d.accountRepoFactory(accountId, repoCredentials)

	dataSourceHandler.SetDataSourceFullname(accountId)
	dataSourceHandler.SetDataSourceName(accountId)

	metastores, err := d.getMetastores(ctx, dataSourceHandler, accountClient)
	if err != nil {
		return err
	}

	if len(metastores) == 0 {
		logger.Warn("No metastores found")
		return nil
	}

	workspaces, err := d.getWorkspaces(ctx, dataSourceHandler, accountClient)
	if err != nil {
		return err
	}

	metastoreWorkspaceMap, _, err := accountClient.GetWorkspaceMap(ctx, metastores, workspaces)
	if err != nil {
		return err
	}

	for i := range metastores {
		metastore := &metastores[i]

		if metastoreWorkspaces, ok := metastoreWorkspaceMap[metastore.MetastoreId]; ok {
			err = d.getDataObjectsForMetastore(ctx, dataSourceHandler, configParams, metastore, metastoreWorkspaces)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *DataSourceSyncer) getMetastores(ctx context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, accountClient dataSourceAccountRepository) ([]catalog.MetastoreInfo, error) {
	logger.Debug("Load metastores")

	metastores, err := accountClient.ListMetastores(ctx)
	if err != nil {
		return nil, err
	}

	for i := range metastores {
		metastore := &metastores[i]

		err = dataSourceHandler.AddDataObjects(&ds.DataObject{
			Name:       metastore.Name,
			Type:       metastoreType,
			FullName:   metastore.MetastoreId,
			ExternalId: metastore.MetastoreId,
		})
		if err != nil {
			return nil, err
		}
	}

	logger.Debug(fmt.Sprintf("Found %d metastores", len(metastores)))

	return metastores, nil
}

func (d *DataSourceSyncer) getWorkspaces(ctx context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, accountClient dataSourceAccountRepository) ([]Workspace, error) {
	workspaces, err := accountClient.GetWorkspaces(ctx)
	if err != nil {
		return nil, err
	}

	logger.Debug(fmt.Sprintf("Found %d workspaces", len(workspaces)))

	for _, workspace := range workspaces {
		id := strconv.Itoa(workspace.WorkspaceId)

		err = dataSourceHandler.AddDataObjects(&ds.DataObject{
			Name:       workspace.WorkspaceName,
			FullName:   id,
			Type:       workspaceType,
			ExternalId: id,
		})
		if err != nil {
			return nil, err
		}
	}

	return workspaces, nil
}

func (d *DataSourceSyncer) getDataObjectsForMetastore(ctx context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, configParams *config.ConfigMap, metastore *catalog.MetastoreInfo, workspaceDeploymentNames []string) error {
	_, repoCredentials, err := getAndValidateParameters(configParams)
	if err != nil {
		return err
	}

	logger.Debug(fmt.Sprintf("Get data objects for metastore %s", metastore.Name))
	logger.Debug(fmt.Sprintf("Will try %d workspaces. %+v", len(workspaceDeploymentNames), workspaceDeploymentNames))

	// Select workspace
	repo, err := selectWorkspaceRepo(ctx, repoCredentials, workspaceDeploymentNames, d.workspaceRepoFactory)
	if err != nil {
		return err
	}

	workspaceRepo := *repo

	catalogs, err := d.getCatalogs(ctx, dataSourceHandler, metastore.MetastoreId, workspaceRepo)
	if err != nil {
		return err
	}

	for i := range catalogs {
		schemas, schemaErr := d.getSchemasInCatalog(ctx, dataSourceHandler, metastore.MetastoreId, &catalogs[i], workspaceRepo)
		if schemaErr != nil {
			return schemaErr
		}

		for j := range schemas {
			_, tableErr := d.getTablesAndColumnsInSchema(ctx, dataSourceHandler, metastore.MetastoreId, &schemas[j], workspaceRepo)
			if tableErr != nil {
				return tableErr
			}
		}
	}

	return nil
}

func (d *DataSourceSyncer) getCatalogs(ctx context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, metastoreId string, repo dataSourceWorkspaceRepository) ([]catalog.CatalogInfo, error) {
	logger.Debug(fmt.Sprintf("Load catalogs for metastore %s", metastoreId))

	catalogs, err := repo.ListCatalogs(ctx)
	if err != nil {
		return nil, err
	}

	for i := range catalogs {
		c := &catalogs[i]

		uniqueId := createUniqueId(c.MetastoreId, c.Name)

		err = dataSourceHandler.AddDataObjects(&ds.DataObject{
			Name:             c.Name,
			ExternalId:       uniqueId,
			ParentExternalId: c.MetastoreId,
			Description:      c.Comment,
			FullName:         uniqueId,
			Type:             catalogType,
		})
		if err != nil {
			return nil, err
		}
	}

	return catalogs, nil
}

func (d *DataSourceSyncer) getSchemasInCatalog(ctx context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, metastoreId string, catalogInfo *catalog.CatalogInfo, repo dataSourceWorkspaceRepository) ([]catalog.SchemaInfo, error) {
	logger.Debug(fmt.Sprintf("Load schemas in catalog %s in metastore %s", catalogInfo.Name, metastoreId))

	schemas, err := repo.ListSchemas(ctx, catalogInfo.Name)
	if err != nil {
		return nil, err
	}

	parentId := createUniqueId(metastoreId, catalogInfo.Name)

	for i := range schemas {
		schema := &schemas[i]

		uniqueId := createUniqueId(metastoreId, schema.FullName)

		err = dataSourceHandler.AddDataObjects(&ds.DataObject{
			Name:             schema.Name,
			ExternalId:       uniqueId,
			ParentExternalId: parentId,
			Description:      schema.Comment,
			FullName:         uniqueId,
			Type:             ds.Schema,
		})
		if err != nil {
			return nil, err
		}
	}

	return schemas, nil
}

func (d *DataSourceSyncer) getTablesAndColumnsInSchema(ctx context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, metastoreId string, schemaInfo *catalog.SchemaInfo, repo dataSourceWorkspaceRepository) ([]catalog.TableInfo, error) {
	logger.Debug(fmt.Sprintf("Load tables in schema %s in metastore %s", schemaInfo.FullName, metastoreId))

	tables, err := repo.ListTables(ctx, schemaInfo.CatalogName, schemaInfo.Name)
	if err != nil {
		return nil, err
	}

	parentId := createUniqueId(metastoreId, schemaInfo.FullName)

	for i := range tables {
		table := &tables[i]

		uniqueId := createUniqueId(metastoreId, table.FullName)

		doType, doErr := tableTypeToRaitoType(table.TableType)
		if doErr != nil {
			logger.Warn(doErr.Error())
			continue
		}

		err = dataSourceHandler.AddDataObjects(&ds.DataObject{
			Name:             table.Name,
			ExternalId:       uniqueId,
			ParentExternalId: parentId,
			Description:      table.Comment,
			FullName:         uniqueId,
			Type:             doType,
		})
		if err != nil {
			return nil, err
		}

		for i := range table.Columns {
			column := &table.Columns[i]

			uniqueColumnId := createTableUniqueId(metastoreId, table.FullName, column.Name)

			err = dataSourceHandler.AddDataObjects(&ds.DataObject{
				Name:             column.Name,
				ExternalId:       uniqueColumnId,
				ParentExternalId: uniqueId,
				Description:      column.Comment,
				FullName:         uniqueColumnId,
				Type:             ds.Column,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	return tables, nil
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

func tableTypeToRaitoType(tType catalog.TableType) (string, error) {
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
