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
)

const (
	startTimeout = time.Minute * 10
)

var _ WarehouseRepository = (*SqlWarehouseRepository)(nil)

//go:generate go run github.com/vektra/mockery/v2 --name=WarehouseRepository --testonly=false
type WarehouseRepository interface {
	ExecuteStatement(ctx context.Context, catalog, schema, statement string, parameters ...sql.StatementParameterListItem) (*sql.ExecuteStatementResponse, error)
	GetTableInformation(ctx context.Context, catalog, schema, tableName string) (map[string]*ColumnInformation, error)
	DropMask(ctx context.Context, catalog, schema, table, column string) error
	DropRowFilter(ctx context.Context, catalog, schema, table string) error
	DropFunction(ctx context.Context, catalog, schema, functionName string) error
	SetMask(ctx context.Context, catalog, schema, table, column, function string) error
	SetRowFilter(ctx context.Context, catalog, schema, table, functionName string, arguments []string) error
}

type SqlWarehouseRepository struct {
	warehouseId string

	executionClient *sql.StatementExecutionAPI
	warehouseClient *sql.WarehousesAPI
}

func NewSqlWarehouseRepository(client *databricks.WorkspaceClient, warehouseId string) *SqlWarehouseRepository {
	return &SqlWarehouseRepository{
		warehouseId: warehouseId,

		executionClient: client.StatementExecution,
		warehouseClient: client.Warehouses,
	}
}

func (r *SqlWarehouseRepository) ExecuteStatement(ctx context.Context, catalog, schema, statement string, parameters ...sql.StatementParameterListItem) (*sql.ExecuteStatementResponse, error) {
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

func (r *SqlWarehouseRepository) GetTableInformation(ctx context.Context, catalog, schema, tableName string) (map[string]*ColumnInformation, error) {
	response, err := r.ExecuteStatement(ctx, catalog, schema, fmt.Sprintf("DESCRIBE TABLE EXTENDED %s", tableName))
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	if response.Result == nil {
		return nil, fmt.Errorf("no result on describe table %q", tableName)
	}

	result := make(map[string]*ColumnInformation)

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
			result[row[0]] = &ColumnInformation{
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
	_, err := r.ExecuteStatement(ctx, catalog, schema, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP MASK", table, column))

	return err
}

func (r *SqlWarehouseRepository) DropRowFilter(ctx context.Context, catalog, schema, table string) error {
	_, err := r.ExecuteStatement(ctx, catalog, schema, fmt.Sprintf("ALTER TABLE %s DROP ROW FILTER", table))

	return err
}

func (r *SqlWarehouseRepository) DropFunction(ctx context.Context, catalog, schema, functionName string) error {
	_, err := r.ExecuteStatement(ctx, catalog, schema, fmt.Sprintf("DROP FUNCTION %s", functionName))

	return err
}

func (r *SqlWarehouseRepository) SetMask(ctx context.Context, catalog, schema, table, column, function string) error {
	_, err := r.ExecuteStatement(ctx, catalog, schema, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET MASK %s", table, column, function))

	return err
}

func (r *SqlWarehouseRepository) SetRowFilter(ctx context.Context, catalog, schema, table, functionName string, arguments []string) error {
	_, err := r.ExecuteStatement(ctx, catalog, schema, fmt.Sprintf("ALTER TABLE %s SET ROW FILTER %s ON (%s);", table, functionName, strings.Join(arguments, ", ")))

	return err
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
