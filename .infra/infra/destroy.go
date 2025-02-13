package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/databricks/databricks-sdk-go"
	catalog2 "github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/raito-io/golang-set/set"
)

var dbClientId, dbClientSecret, dbHost string
var catalogsStr string
var nonDryRun bool

func dropAllGrantsOfCatalogs(ctx context.Context, catalogs []string) error {
	client, err := databricks.NewWorkspaceClient(&databricks.Config{
		ClientID:     dbClientId,
		ClientSecret: dbClientSecret,
		Host:         dbHost,
	})
	if err != nil {
		return fmt.Errorf("create workspace client: %w", err)
	}

	existingCatalogs, err := client.Catalogs.ListAll(ctx, catalog2.ListCatalogsRequest{})
	if err != nil {
		return fmt.Errorf("list catalogs: %w", err)
	}

	existingCatalogNames := set.NewSet[string]()

	for _, catalog := range existingCatalogs {
		existingCatalogNames.Add(catalog.Name)
	}

	for _, catalog := range catalogs {
		if !existingCatalogNames.Contains(catalog) {
			fmt.Printf("Catalog %s not found\n", catalog)
			continue
		}

		err = dropAllGrantsInCatalog(ctx, client, catalog)
		if err != nil {
			return fmt.Errorf("drop grants in catalog %s: %w", catalog, err)
		}
	}

	return nil
}

func dropAllGrantsInCatalog(ctx context.Context, client *databricks.WorkspaceClient, catalogName string) error {
	schemaInfo, err := client.Schemas.ListAll(ctx, catalog2.ListSchemasRequest{
		CatalogName: catalogName,
	})
	if err != nil {
		return fmt.Errorf("list schemas: %w", err)
	}

	for i := range schemaInfo {
		err = dropAllGrantInSchemas(ctx, client, &schemaInfo[i])
		if err != nil {
			return fmt.Errorf("drop all grants in schema %s: %w", schemaInfo[i].FullName, err)
		}
	}

	grants, err := client.Grants.Get(ctx, catalog2.GetGrantRequest{
		FullName:      catalogName,
		SecurableType: catalog2.SecurableTypeCatalog,
	})
	if err != nil {
		return fmt.Errorf("get grants: %w", err)
	}

	err = removeGrants(ctx, client, catalogName, catalog2.SecurableTypeCatalog, grants)
	if err != nil {
		return fmt.Errorf("remove grants: %w", err)
	}

	return nil
}

func dropAllGrantInSchemas(ctx context.Context, client *databricks.WorkspaceClient, schema *catalog2.SchemaInfo) error {
	tables, err := client.Tables.ListAll(ctx, catalog2.ListTablesRequest{
		CatalogName:    schema.CatalogName,
		SchemaName:     schema.Name,
		OmitColumns:    false,
		OmitProperties: false,
	})

	for i := range tables {
		err = dropAllGrantsInTable(ctx, client, &tables[i])
		if err != nil {
			return fmt.Errorf("drop all grants in table %s: %w", tables[i].FullName, err)
		}
	}

	grants, err := client.Grants.Get(ctx, catalog2.GetGrantRequest{
		FullName:      schema.FullName,
		SecurableType: catalog2.SecurableTypeSchema,
	})
	if err != nil {
		return fmt.Errorf("get grants: %w", err)
	}

	err = removeGrants(ctx, client, schema.FullName, catalog2.SecurableTypeSchema, grants)
	if err != nil {
		return fmt.Errorf("remove grants: %w", err)
	}

	return nil
}

func dropAllGrantsInTable(ctx context.Context, client *databricks.WorkspaceClient, table *catalog2.TableInfo) error {
	grants, err := client.Grants.Get(ctx, catalog2.GetGrantRequest{
		FullName:      table.FullName,
		SecurableType: catalog2.SecurableTypeTable,
	})
	if err != nil {
		return fmt.Errorf("get grants: %w", err)
	}

	err = removeGrants(ctx, client, table.FullName, catalog2.SecurableTypeTable, grants)
	if err != nil {
		return fmt.Errorf("remove grants: %w", err)
	}

	return nil
}

func removeGrants(ctx context.Context, client *databricks.WorkspaceClient, fullname string, securableType catalog2.SecurableType, grants *catalog2.PermissionsList) error {
	changes := make([]catalog2.PermissionsChange, 0, len(grants.PrivilegeAssignments))

	for _, grant := range grants.PrivilegeAssignments {
		changes = append(changes, catalog2.PermissionsChange{
			Remove:    grant.Privileges,
			Principal: grant.Principal,
		})
	}

	if len(changes) == 0 {
		return nil
	}

	if nonDryRun {
		fmt.Printf("Removing grants for %s\n", fullname)

		_, err := client.Grants.Update(ctx, catalog2.UpdatePermissions{
			Changes:       changes,
			FullName:      fullname,
			SecurableType: securableType,
		})
		if err != nil {
			return fmt.Errorf("drop grant for %s: %w", fullname, err)
		}
	} else {
		fmt.Printf("Will drop grants %+v for item: %q\n", changes, fullname)
	}

	return nil
}

func main() {
	flag.StringVar(&dbClientId, "dbClientId", "", "databricks client id")
	flag.StringVar(&dbClientSecret, "dbClientSecret", "", "databricks client secret")
	flag.StringVar(&dbHost, "dbHost", "", "databricks workspace host")
	flag.StringVar(&catalogsStr, "catalogs", "", "comma separated list of catalogs")
	flag.BoolVar(&nonDryRun, "drop", false, "Execute drop roles. If not set or false a dry run will be executed.")
	flag.Parse()

	catalogs := strings.Split(catalogsStr, ",")

	if len(catalogs) == 0 {
		fmt.Println("No catalogs specified")
		return
	}

	if dbHost == "" {
		fmt.Println("No databricks host specified")
		return
	}

	if dbClientId == "" {
		fmt.Println("No databricks username specified")
		return
	}

	if dbClientSecret == "" {
		fmt.Println("No databricks password specified")
		return
	}

	err := dropAllGrantsOfCatalogs(context.Background(), catalogs)
	if err != nil {
		panic(err)
	}
}
