package databricks

import (
	"context"
	"fmt"
	"strings"

	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/provisioning"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/golang-set/set"

	"cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/databricks/repo"
)

//go:generate go run github.com/vektra/mockery/v2 --name=accountRepository
type accountRepository interface {
	ListMetastores(ctx context.Context) ([]catalog.MetastoreInfo, error)
	GetWorkspaceMap(ctx context.Context, metastores []catalog.MetastoreInfo, workspaces []provisioning.Workspace) (map[string][]*provisioning.Workspace, map[string]string, error)
	GetWorkspaces(ctx context.Context) ([]provisioning.Workspace, error)
	GetWorkspaceByName(ctx context.Context, workspaceName string) (*provisioning.Workspace, error)
}

//go:generate go run github.com/vektra/mockery/v2 --name=workspaceRepository
type workspaceRepository interface {
	ListCatalogs(ctx context.Context) <-chan repo.ChannelItem[catalog.CatalogInfo]
	ListSchemas(ctx context.Context, catalogName string) <-chan repo.ChannelItem[catalog.SchemaInfo]
	ListAllTables(ctx context.Context, catalogName string, schemaName string) ([]catalog.TableInfo, error)
	ListFunctions(ctx context.Context, catalogName string, schemaName string) <-chan repo.ChannelItem[catalog.FunctionInfo]
}

//go:generate go run github.com/vektra/mockery/v2 --name=DataObjectVisitor
type DataObjectVisitor interface {
	// VisitWorkspace is called for each workspace found in the account
	VisitWorkspace(ctx context.Context, workspace *provisioning.Workspace) error

	// VisitMetastore is called for each metastore found in the account.
	// workspace argument may be nil if we were not able to find any active workspace for the metastore.
	VisitMetastore(ctx context.Context, metastore *catalog.MetastoreInfo, workspace *provisioning.Workspace) error

	// VisitCatalog is called for each catalog found in a metastore with active workspace
	VisitCatalog(ctx context.Context, catalog *catalog.CatalogInfo, parent *catalog.MetastoreInfo, workspace *provisioning.Workspace) error

	// VisitSchema is called for each schema found in a catalog with active workspace
	VisitSchema(ctx context.Context, schema *catalog.SchemaInfo, parent *catalog.CatalogInfo, workspace *provisioning.Workspace) error

	// VisitTable is called for each table found in a schema with active workspace
	VisitTable(ctx context.Context, table *catalog.TableInfo, parent *catalog.SchemaInfo, workspace *provisioning.Workspace) error

	// VisitColumn is called for each column found in a table with active workspace
	VisitColumn(ctx context.Context, column *catalog.ColumnInfo, parent *catalog.TableInfo, workspace *provisioning.Workspace) error

	// VisitFunction is called for each function found in a table with active workspace
	VisitFunction(ctx context.Context, function *catalog.FunctionInfo, parent *catalog.SchemaInfo, workspace *provisioning.Workspace) error
}

type DataObjectTraverserOptions struct {
	SecurableTypesToReturn set.Set[string]
}

type AccountRepoFactory func() (accountRepository, error)

type WorkspaceRepoFactory func(metastoreWorkspaces []*provisioning.Workspace) (workspaceRepository, *provisioning.Workspace, error)

type CreateFullName func(securableType string, parent interface{}, object interface{}) string

type DataObjectTraverser struct {
	accountRepoFactory   AccountRepoFactory
	workspaceRepoFactory WorkspaceRepoFactory
	createFullName       CreateFullName
	config               *ds.DataSourceSyncConfig
}

func NewDataObjectTraverser(config *ds.DataSourceSyncConfig, accountFactory AccountRepoFactory, workspaceFactory WorkspaceRepoFactory, createFullName CreateFullName) *DataObjectTraverser {
	return &DataObjectTraverser{
		config:               config,
		accountRepoFactory:   accountFactory,
		workspaceRepoFactory: workspaceFactory,
		createFullName:       createFullName,
	}
}

func (t *DataObjectTraverser) Traverse(ctx context.Context, visitor DataObjectVisitor, optionFunc ...func(traverserOptions *DataObjectTraverserOptions)) error {
	options := DataObjectTraverserOptions{}
	for _, option := range optionFunc {
		option(&options)
	}

	accountRepo, err := t.accountRepoFactory()
	if err != nil {
		return fmt.Errorf("account repo factory: %w", err)
	}

	metastores, workspaces, err := t.traverseAccount(ctx, accountRepo, visitor, options)
	if err != nil {
		return fmt.Errorf("traverse acocunt: %w", err)
	}

	err = t.traverseCatalog(ctx, visitor, options, accountRepo, metastores, workspaces)
	if err != nil {
		return fmt.Errorf("traverse catalog: %w", err)
	}

	return nil
}

func (t *DataObjectTraverser) traverseCatalog(ctx context.Context, visitor DataObjectVisitor, options DataObjectTraverserOptions, accountRepo accountRepository, metastores []catalog.MetastoreInfo, workspaces []provisioning.Workspace) error {
	logger.Debug("Traversing catalogs")

	if options.SecurableTypesToReturn.Contains(constants.CatalogType) || options.SecurableTypesToReturn.Contains(ds.Schema) || options.SecurableTypesToReturn.Contains(ds.Table) || options.SecurableTypesToReturn.Contains(ds.Column) {
		metastoreWorkspaceMap, _, err := accountRepo.GetWorkspaceMap(ctx, metastores, workspaces)
		if err != nil {
			return fmt.Errorf("get workspaces: %w", err)
		}

		for i := range metastores {
			metastore := &metastores[i]

			logger.Debug(fmt.Sprintf("Traversing catalogs for metastore %q", metastore.Name))

			if metastoreWorkspaces, ok := metastoreWorkspaceMap[metastore.MetastoreId]; ok {
				workspaceClient, selectedWorkspace, err2 := t.workspaceRepoFactory(metastoreWorkspaces)
				if err2 != nil {
					logger.Warn(fmt.Sprintf("Failed to login for metastore %s: %s. Will skip all dataobjects in catalog.", metastore.MetastoreId, err2.Error()))

					continue
				}

				catalogs := workspaceClient.ListCatalogs(ctx)

				for catalogItem := range catalogs {
					if catalogItem.HasError() {
						logger.Warn(fmt.Sprintf("Unable to list catalogs for metastore %s: %s. Will skip all dataobjects in catalog.", metastore.MetastoreId, catalogItem.Err.Error()))

						break
					}

					fullName := t.createFullName(constants.CatalogType, metastore, catalogItem.I)

					logger.Debug(fmt.Sprintf("traversing catalog %s", fullName))

					if t.shouldGoInto(fullName) {
						if options.SecurableTypesToReturn.Contains(constants.CatalogType) && t.shouldHandle(fullName) {
							err = visitor.VisitCatalog(ctx, catalogItem.I, metastore, selectedWorkspace)
							if err != nil {
								return fmt.Errorf("handle %s: %w", fullName, err)
							}
						}

						err = t.traverseSchemas(ctx, options, workspaceClient, catalogItem.I, visitor, selectedWorkspace)
						if err != nil {
							logger.Warn(fmt.Sprintf("Unable to list schemas for catalog %s: %s. Will skip all dataobjects in catalog.", fullName, err.Error()))

							continue
						}
					}
				}
			}
		}
	}

	return nil
}

func (t *DataObjectTraverser) traverseSchemas(ctx context.Context, options DataObjectTraverserOptions, workspaceClient workspaceRepository, cat *catalog.CatalogInfo, visitor DataObjectVisitor, selectedWorkspace *provisioning.Workspace) error {
	if options.SecurableTypesToReturn.Contains(ds.Schema) || options.SecurableTypesToReturn.Contains(ds.Table) || options.SecurableTypesToReturn.Contains(ds.Column) {
		schemas := workspaceClient.ListSchemas(ctx, cat.Name)

		for schemaItem := range schemas {
			if schemaItem.HasError() {
				return fmt.Errorf("list schemas of catalog %q: %w", cat.Name, schemaItem.Err)
			}

			schema := schemaItem.I

			fullName := t.createFullName(ds.Schema, cat, schema)
			logger.Debug(fmt.Sprintf("traversing schema %s", fullName))

			if t.shouldGoInto(fullName) {
				if options.SecurableTypesToReturn.Contains(ds.Schema) && t.shouldHandle(fullName) {
					err := visitor.VisitSchema(ctx, schema, cat, selectedWorkspace)
					if err != nil {
						return fmt.Errorf("handle schema %s: %w", fullName, err)
					}
				}

				if options.SecurableTypesToReturn.Contains(ds.Table) || options.SecurableTypesToReturn.Contains(ds.Column) {
					tables, err := workspaceClient.ListAllTables(ctx, schema.CatalogName, schema.Name)
					if err != nil {
						logger.Warn(fmt.Sprintf("Unable to list tables for schema %s: %s. Will skip all tables and functions in schema", fullName, err.Error()))

						continue
					}

					err = t.traverseTablesAndColumns(ctx, tables, options, schema, visitor, selectedWorkspace)
					if err != nil {
						logger.Warn(fmt.Sprintf("Unable to traverse tables and columns for schema %s: %s", fullName, err.Error()))
					}

					err = t.traverseFunctions(ctx, options, workspaceClient, schema, visitor, selectedWorkspace) // should be executed after list tables and traverse tables and columns to check filters and masks
					if err != nil {
						logger.Warn(fmt.Sprintf("Unable to traverse functions for schema %s: %s", fullName, err.Error()))
					}
				}
			}
		}
	}

	return nil
}

func (t *DataObjectTraverser) traverseTablesAndColumns(ctx context.Context, tables []catalog.TableInfo, options DataObjectTraverserOptions, schema *catalog.SchemaInfo, visitor DataObjectVisitor, selectedWorkspace *provisioning.Workspace) error {
	for i := range tables {
		fullName := t.createFullName(ds.Table, schema, &tables[i])

		logger.Debug(fmt.Sprintf("traversing table %s", fullName))

		if t.shouldGoInto(fullName) {
			if options.SecurableTypesToReturn.Contains(ds.Table) && t.shouldHandle(fullName) {
				err := visitor.VisitTable(ctx, &tables[i], schema, selectedWorkspace)
				if err != nil {
					return fmt.Errorf("handle table %s: %w", fullName, err)
				}
			}

			if options.SecurableTypesToReturn.Contains(ds.Column) {
				for j := range tables[i].Columns {
					columnFullName := t.createFullName(ds.Column, &tables[i], &tables[i].Columns[j])

					if t.shouldHandle(columnFullName) {
						err := visitor.VisitColumn(ctx, &tables[i].Columns[j], &tables[i], selectedWorkspace)
						if err != nil {
							return fmt.Errorf("handle column %s: %w", columnFullName, err)
						}
					}
				}
			}
		}
	}

	return nil
}

func (t *DataObjectTraverser) traverseFunctions(ctx context.Context, options DataObjectTraverserOptions, workspaceClient workspaceRepository, schema *catalog.SchemaInfo, visitor DataObjectVisitor, selectedWorkspace *provisioning.Workspace) error {
	if options.SecurableTypesToReturn.Contains(constants.FunctionType) {
		functions := workspaceClient.ListFunctions(ctx, schema.CatalogName, schema.Name)

		for functionItem := range functions {
			if functionItem.HasError() {
				return fmt.Errorf("list functions of schema %s: %w", schema.FullName, functionItem.Err)
			}

			function := functionItem.I

			logger.Debug(fmt.Sprintf("traversing function %s", function.FullName))

			if options.SecurableTypesToReturn.Contains(constants.FunctionType) && t.shouldHandle(t.createFullName(constants.FunctionType, schema, function)) {
				err := visitor.VisitFunction(ctx, function, schema, selectedWorkspace)
				if err != nil {
					return fmt.Errorf("handle function %s: %w", function.FullName, err)
				}
			}
		}
	}

	return nil
}

func (t *DataObjectTraverser) traverseAccount(ctx context.Context, accountRepo accountRepository, visitor DataObjectVisitor, options DataObjectTraverserOptions) ([]catalog.MetastoreInfo, []provisioning.Workspace, error) {
	logger.Debug("Traversing account")

	metastores, err := accountRepo.ListMetastores(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("list metastores: %w", err)
	}

	logger.Debug(fmt.Sprintf("Found %d metastores", len(metastores)))

	metastores = filterObjects(metastores, func(m catalog.MetastoreInfo) bool {
		return t.shouldGoInto(t.createFullName(constants.MetastoreType, nil, &m))
	})

	workspaces, err := accountRepo.GetWorkspaces(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get workspaces: %w", err)
	}

	logger.Debug(fmt.Sprintf("Found %d workspaces", len(workspaces)))

	if options.SecurableTypesToReturn.Contains(constants.WorkspaceType) {
		for i := range workspaces {
			if t.shouldHandle(t.createFullName(constants.WorkspaceType, nil, &workspaces[i])) {
				err = visitor.VisitWorkspace(ctx, &workspaces[i])
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}

	metastoreWorkspaceMap, _, err := accountRepo.GetWorkspaceMap(ctx, metastores, workspaces)
	if err != nil {
		return nil, nil, fmt.Errorf("get workspace map: %w", err)
	}

	if options.SecurableTypesToReturn.Contains(constants.MetastoreType) {
		for i := range metastores {
			metastore := &metastores[i]

			logger.Debug(fmt.Sprintf("Traversing metastore %q", metastore.Name))

			var selectedWorkspace *provisioning.Workspace

			if metastoreWorkspaces, ok := metastoreWorkspaceMap[metastore.MetastoreId]; ok {
				logger.Debug(fmt.Sprintf("Searching for workspace for metastore %q", metastore.Name))

				_, selectedWorkspace, err = t.workspaceRepoFactory(metastoreWorkspaces)
				if err != nil {
					logger.Warn(fmt.Sprintf("Unable to get workspace for metastore %q. Will ignore metastore. Error: %s", metastore.Name, err))

					continue
				}

				logger.Debug(fmt.Sprintf("Found workspace for metastore %q => %q", metastore.Name, selectedWorkspace.WorkspaceName))
			}

			if t.shouldHandle(t.createFullName(constants.MetastoreType, nil, metastore)) {
				err = visitor.VisitMetastore(ctx, &metastores[i], selectedWorkspace)
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}

	return metastores, workspaces, nil
}

func filterObjects[T any](input []T, filter func(o T) bool) []T {
	filtered := make([]T, 0)

	for i := range input {
		if filter(input[i]) {
			filtered = append(filtered, input[i])
		}
	}

	return filtered
}

// shouldHandle determines if this data object needs to be handled by the syncer or not. It does this by looking at the configuration options to only sync a part.
func (t *DataObjectTraverser) shouldHandle(fullName string) bool {
	// No partial sync specified, so do everything
	if t.config == nil || t.config.DataObjectParent == "" {
		return true
	}

	// Check if the data object is under the data object to start from
	if !strings.HasPrefix(fullName, t.config.DataObjectParent) || t.config.DataObjectParent == fullName {
		return false
	}

	// Check if we hit any excludes
	for _, exclude := range t.config.DataObjectExcludes {
		if strings.HasPrefix(fullName, t.config.DataObjectParent+"."+exclude) {
			return false
		}
	}

	return true
}

// shouldGoInto checks if we need to go deeper into this data object or not.
func (t *DataObjectTraverser) shouldGoInto(fullName string) bool {
	// No partial sync specified, so do everything
	if t.config == nil || t.config.DataObjectParent == "" || strings.HasPrefix(t.config.DataObjectParent, fullName) || strings.HasPrefix(fullName, t.config.DataObjectParent) {
		return true
	}

	return false
}
