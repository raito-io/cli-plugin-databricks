package databricks

import (
	"context"

	"github.com/databricks/databricks-sdk-go/service/catalog"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/golang-set/set"

	"cli-plugin-databricks/databricks/repo"
)

//go:generate go run github.com/vektra/mockery/v2 --name=accountRepository
type accountRepository interface {
	ListMetastores(ctx context.Context) ([]catalog.MetastoreInfo, error)
	GetWorkspaceMap(ctx context.Context, metastores []catalog.MetastoreInfo, workspaces []repo.Workspace) (map[string][]string, map[string]string, error)
	GetWorkspaces(ctx context.Context) ([]repo.Workspace, error)
}

//go:generate go run github.com/vektra/mockery/v2 --name=workspaceRepository
type workspaceRepository interface {
	ListCatalogs(ctx context.Context) ([]catalog.CatalogInfo, error)
	ListSchemas(ctx context.Context, catalogName string) ([]catalog.SchemaInfo, error)
	ListTables(ctx context.Context, catalogName string, schemaName string) ([]catalog.TableInfo, error)
	ListFunctions(ctx context.Context, catalogName string, schemaName string) ([]repo.FunctionInfo, error)
}

type DataObjectTraverserOptions struct {
	SecurableTypesToReturn set.Set[string]
}

type AccountRepoFactory func() (accountRepository, error)

type WorkspaceRepoFactory func(metastoreWorkspaces []string) (workspaceRepository, string, error)

type DataObjectTraverser struct {
	accountRepoFactory   AccountRepoFactory
	workspaceRepoFactory WorkspaceRepoFactory
}

type TraverseObjectFnc func(ctx context.Context, securableType string, parentObject interface{}, object interface{}, workspaceId *string) error

func NewDataObjectTraverser(accountFactory AccountRepoFactory, workspaceFactory WorkspaceRepoFactory) *DataObjectTraverser {
	return &DataObjectTraverser{
		accountRepoFactory:   accountFactory,
		workspaceRepoFactory: workspaceFactory,
	}
}

func (t *DataObjectTraverser) Traverse(ctx context.Context, f TraverseObjectFnc, optionFunc ...func(traverserOptions *DataObjectTraverserOptions)) error {
	options := DataObjectTraverserOptions{}
	for _, option := range optionFunc {
		option(&options)
	}

	accountRepo, err := t.accountRepoFactory()
	if err != nil {
		return err
	}

	metastores, workspaces, err := t.traverseAccount(ctx, accountRepo, f, options)
	if err != nil {
		return err
	}

	return t.traverseCatalog(ctx, f, options, accountRepo, metastores, workspaces)
}

func (t *DataObjectTraverser) traverseCatalog(ctx context.Context, f TraverseObjectFnc, options DataObjectTraverserOptions, accountRepo accountRepository, metastores []catalog.MetastoreInfo, workspaces []repo.Workspace) error {
	if options.SecurableTypesToReturn.Contains(catalogType) || options.SecurableTypesToReturn.Contains(ds.Schema) || options.SecurableTypesToReturn.Contains(ds.Table) || options.SecurableTypesToReturn.Contains(ds.Column) {
		metastoreWorkspaceMap, _, err := accountRepo.GetWorkspaceMap(ctx, metastores, workspaces)
		if err != nil {
			return err
		}

		for i := range metastores {
			metastore := &metastores[i]

			if metastoreWorkspaces, ok := metastoreWorkspaceMap[metastore.MetastoreId]; ok {
				workspaceClient, selectedWorkspace, err2 := t.workspaceRepoFactory(metastoreWorkspaces)
				if err2 != nil {
					return err2
				}

				catalogs, err2 := workspaceClient.ListCatalogs(ctx)
				if err2 != nil {
					return err2
				}

				for j := range catalogs {
					if options.SecurableTypesToReturn.Contains(catalogType) {
						err = f(ctx, catalogType, metastore, &catalogs[j], &selectedWorkspace)
						if err != nil {
							return err
						}
					}

					err = t.traverseSchemas(ctx, options, workspaceClient, &catalogs[j], func(ctx context.Context, securableType string, parentObject interface{}, object interface{}) error {
						return f(ctx, securableType, parentObject, object, &selectedWorkspace)
					})
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (t *DataObjectTraverser) traverseSchemas(ctx context.Context, options DataObjectTraverserOptions, workspaceClient workspaceRepository, cat *catalog.CatalogInfo, f func(ctx context.Context, securableType string, parentObject interface{}, object interface{}) error) error {
	if options.SecurableTypesToReturn.Contains(ds.Schema) || options.SecurableTypesToReturn.Contains(ds.Table) || options.SecurableTypesToReturn.Contains(ds.Column) {
		schemas, schemaErr := workspaceClient.ListSchemas(ctx, cat.Name)
		if schemaErr != nil {
			return schemaErr
		}

		for i := range schemas {
			if options.SecurableTypesToReturn.Contains(ds.Schema) {
				err := f(ctx, ds.Schema, cat, &schemas[i])
				if err != nil {
					return err
				}
			}

			if options.SecurableTypesToReturn.Contains(ds.Table) || options.SecurableTypesToReturn.Contains(ds.Column) {
				tables, err := workspaceClient.ListTables(ctx, schemas[i].CatalogName, schemas[i].Name)
				if err != nil {
					return err
				}

				err = t.traverseTablesAndColumns(ctx, tables, options, &schemas[i], f)
				if err != nil {
					return err
				}

				err = t.traverseFunctions(ctx, options, workspaceClient, &schemas[i], f)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (t *DataObjectTraverser) traverseTablesAndColumns(ctx context.Context, tables []catalog.TableInfo, options DataObjectTraverserOptions, schema *catalog.SchemaInfo, f func(ctx context.Context, securableType string, parentObject interface{}, object interface{}) error) error {
	for i := range tables {
		if options.SecurableTypesToReturn.Contains(ds.Table) {
			err := f(ctx, ds.Table, schema, &tables[i])
			if err != nil {
				return err
			}
		}

		if options.SecurableTypesToReturn.Contains(ds.Column) {
			for j := range tables[i].Columns {
				err := f(ctx, ds.Column, &tables[i], &tables[i].Columns[j])
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (t *DataObjectTraverser) traverseFunctions(ctx context.Context, options DataObjectTraverserOptions, workspaceClient workspaceRepository, schema *catalog.SchemaInfo, f func(ctx context.Context, securableType string, parentObject interface{}, object interface{}) error) error {
	if options.SecurableTypesToReturn.Contains(functionType) {
		functions, err := workspaceClient.ListFunctions(ctx, schema.CatalogName, schema.Name)
		if err != nil {
			return err
		}

		for l := range functions {
			if options.SecurableTypesToReturn.Contains(functionType) {
				err = f(ctx, functionType, schema, &functions[l])
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (t *DataObjectTraverser) traverseAccount(ctx context.Context, accountRepo accountRepository, f TraverseObjectFnc, options DataObjectTraverserOptions) ([]catalog.MetastoreInfo, []repo.Workspace, error) {
	metastores, err := accountRepo.ListMetastores(ctx)
	if err != nil {
		return nil, nil, err
	}

	workspaces, err := accountRepo.GetWorkspaces(ctx)
	if err != nil {
		return nil, nil, err
	}

	if options.SecurableTypesToReturn.Contains(workspaceType) {
		for i := range workspaces {
			err = f(ctx, workspaceType, nil, &workspaces[i], nil)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	metastoreWorkspaceMap, _, err := accountRepo.GetWorkspaceMap(ctx, metastores, workspaces)
	if err != nil {
		return nil, nil, err
	}

	if options.SecurableTypesToReturn.Contains(metastoreType) {
		for i := range metastores {
			metastore := &metastores[i]

			var selectedWorkspace string

			if metastoreWorkspaces, ok := metastoreWorkspaceMap[metastore.MetastoreId]; ok {
				_, selectedWorkspace, err = t.workspaceRepoFactory(metastoreWorkspaces)
				if err != nil {
					return nil, nil, err
				}
			}

			err = f(ctx, metastoreType, nil, &metastores[i], &selectedWorkspace)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	return metastores, workspaces, nil
}
