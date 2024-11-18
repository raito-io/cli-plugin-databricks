package repo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/smithy-go/ptr"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/sql"

	"cli-plugin-databricks/databricks/repo/types"
	"cli-plugin-databricks/utils/array"
)

const (
	startTimeout = time.Minute * 10
)

var _ WarehouseRepository = (*SqlWarehouseRepository)(nil)

//go:generate go run github.com/vektra/mockery/v2 --name=WarehouseRepository --testonly=false
type WarehouseRepository interface {
	ExecuteStatement(ctx context.Context, catalog, schema, statement string, parameters ...sql.StatementParameterListItem) (*sql.StatementResponse, error)
	GetTableInformation(ctx context.Context, catalog, schema, tableName string) (map[string]*types.ColumnInformation, error)
	DropMask(ctx context.Context, catalog, schema, table, column string) error
	DropRowFilter(ctx context.Context, catalog, schema, table string) error
	DropFunction(ctx context.Context, catalog, schema, functionName string) error
	SetMask(ctx context.Context, catalog, schema, table, column, function string) error
	SetRowFilter(ctx context.Context, catalog, schema, table, functionName string, arguments []string) error
	GetTags(ctx context.Context, catalog string, fn func(ctx context.Context, fullName string, key string, value string) error) error
}

type SqlWarehouseRepository struct {
	warehouseId string

	executionClient sql.StatementExecutionInterface
	warehouseClient sql.WarehousesInterface
}

func NewSqlWarehouseRepository(client *databricks.WorkspaceClient, warehouseId string) *SqlWarehouseRepository {
	return &SqlWarehouseRepository{
		warehouseId: warehouseId,

		executionClient: client.StatementExecution,
		warehouseClient: client.Warehouses,
	}
}

func (r *SqlWarehouseRepository) ExecuteStatement(ctx context.Context, catalog, schema, statement string, parameters ...sql.StatementParameterListItem) (*sql.StatementResponse, error) {
	err := r.waitForWarehouse(ctx)
	if err != nil {
		return nil, err
	}

	logger.Debug(fmt.Sprintf("Executing statement: %q on '%s.%s", statement, catalog, schema))

	response, err := r.executionClient.ExecuteAndWait(ctx, sql.ExecuteStatementRequest{
		Parameters:  parameters,
		Schema:      schema,
		Catalog:     catalog,
		Statement:   statement,
		WarehouseId: r.warehouseId,
	})

	if err != nil {
		return response, fmt.Errorf("execute statement %q on '%s.%s': %w", statement, catalog, schema, err)
	}

	return response, nil
}

func (r *SqlWarehouseRepository) GetTableInformation(ctx context.Context, catalog, schema, tableName string) (map[string]*types.ColumnInformation, error) {
	response, err := r.ExecuteStatement(ctx, catalog, schema, fmt.Sprintf("DESCRIBE TABLE EXTENDED %s", tableName))
	if err != nil {
		return nil, err
	}

	if response.Result == nil {
		return nil, fmt.Errorf("no result on describe table %q", tableName)
	}

	result := make(map[string]*types.ColumnInformation)

	section := "" // section 0 is the column name + type // section 2 is

	for _, row := range response.Result.DataArray {
		if len(row) == 0 || row[0] == "" {
			continue
		}

		if strings.HasPrefix(row[0], "#") {
			section = row[0]
			continue
		}

		switch section {
		case "":
			result[row[0]] = &types.ColumnInformation{
				Name: row[0],
				Type: row[1],
			}
		case "# Column Masks":
			result[row[0]].Mask = ptr.String(strings.Split(strings.ReplaceAll(row[1], "`", ""), ".")[2])
		default:
			continue
		}

		logger.Debug(fmt.Sprintf("Parsed row (secltion %s) %+v", row[0], *result[row[0]]))
	}

	return result, nil
}

func (r *SqlWarehouseRepository) DropMask(ctx context.Context, catalog, schema, table, column string) error {
	_, err := r.ExecuteStatement(ctx, catalog, schema, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP MASK", escapeName(table), escapeName(column)))

	return err
}

func (r *SqlWarehouseRepository) DropRowFilter(ctx context.Context, catalog, schema, table string) error {
	_, err := r.ExecuteStatement(ctx, catalog, schema, fmt.Sprintf("ALTER TABLE %s DROP ROW FILTER", escapeName(table)))

	return err
}

func (r *SqlWarehouseRepository) DropFunction(ctx context.Context, catalog, schema, functionName string) error {
	_, err := r.ExecuteStatement(ctx, catalog, schema, fmt.Sprintf("DROP FUNCTION IF EXISTS %s", functionName))

	return err
}

func (r *SqlWarehouseRepository) SetMask(ctx context.Context, catalog, schema, table, column, function string) error {
	_, err := r.ExecuteStatement(ctx, catalog, schema, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET MASK %s", escapeName(table), escapeName(column), function))

	return err
}

func (r *SqlWarehouseRepository) SetRowFilter(ctx context.Context, catalog, schema, table, functionName string, arguments []string) error {
	logger.Debug(fmt.Sprintf("Setting row filter %q on %s.%s.%s with arguments %v", functionName, catalog, schema, table, arguments))

	_, err := r.ExecuteStatement(ctx, catalog, schema, fmt.Sprintf("ALTER TABLE %s SET ROW FILTER %s ON (%s);", escapeName(table), functionName, strings.Join(escapeColumnNames(arguments...), ", ")))

	return err
}

func escapeColumnNames(columnNames ...string) []string {
	return array.Map(columnNames, func(s *string) string {
		return escapeName(*s)
	})
}

func escapeName(columnName string) string {
	return fmt.Sprintf("`%s`", columnName)
}

func (r *SqlWarehouseRepository) GetTags(ctx context.Context, catalog string, fn func(ctx context.Context, fullName string, key string, value string) error) error {
	// Catalog tags
	response, err := r.ExecuteStatement(ctx, catalog, "", "SELECT catalog_name, tag_name, tag_value FROM information_schema.catalog_tags")
	if err != nil {
		return fmt.Errorf("get catalog tags: %w", err)
	}

	for _, row := range response.Result.DataArray {
		err = fn(ctx, row[0], row[1], row[2])
		if err != nil {
			return err
		}
	}

	// Schema tags
	response, err = r.ExecuteStatement(ctx, catalog, "", "SELECT catalog_name, schema_name, tag_name, tag_value FROM information_schema.schema_tags")
	if err != nil {
		return fmt.Errorf("get schema tags: %w", err)
	}

	for _, row := range response.Result.DataArray {
		err = fn(ctx, row[0]+"."+row[1], row[2], row[3])
		if err != nil {
			return err
		}
	}

	// Table tags
	response, err = r.ExecuteStatement(ctx, catalog, "", "SELECT catalog_name, schema_name, table_name, tag_name, tag_value FROM information_schema.table_tags")
	if err != nil {
		return fmt.Errorf("get table tags: %w", err)
	}

	for _, row := range response.Result.DataArray {
		err = fn(ctx, row[0]+"."+row[1]+"."+row[2], row[3], row[4])
		if err != nil {
			return err
		}
	}

	// Column tags
	response, err = r.ExecuteStatement(ctx, catalog, "", "SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value FROM information_schema.column_tags")
	if err != nil {
		return fmt.Errorf("get column tags: %w", err)
	}

	for _, row := range response.Result.DataArray {
		err = fn(ctx, row[0]+"."+row[1]+"."+row[2]+"."+row[3], row[4], row[5])
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *SqlWarehouseRepository) waitForWarehouse(ctx context.Context) error {
	requestToStart := false

	timeout := time.Now().Add(startTimeout)
	for time.Now().Before(timeout) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			warehouse, err := r.warehouseClient.Get(ctx, sql.GetWarehouseRequest{Id: r.warehouseId})
			if err != nil {
				return err
			} else if warehouse.State == sql.StateRunning {
				return nil
			} else if !requestToStart {
				_, err = r.warehouseClient.Start(ctx, sql.StartRequest{Id: r.warehouseId})
				if err != nil {
					return err
				}

				requestToStart = true
			} else {
				time.Sleep(time.Second) // busy wait for warehouse
			}
		}
	}

	return errors.New("warehouse start timeout")
}
