package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-multierror"
)

var logger hclog.Logger

type Persona struct {
	Id           string `json:"id"`
	ClientId     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	Name         string `json:"name"`
}

type InfrastructureInput struct {
	TestingTables struct {
		Value []string `json:"value"`
	} `json:"testing_tables,omitempty"`
	DemoTables struct {
		Value []string `json:"value"`
	} `json:"demo_tables,omitempty"`
	Personas struct {
		Value []Persona `json:"value"`
	} `json:"personas"`
}

func CreateUsage(infraInput *InfrastructureInput, users []Persona, host string, warehouseId string) error {
	ctx := context.Background()

	wg := multierror.Group{}

	for _, user := range users {
		wg.Go(func() error {
			logger.Info(fmt.Sprintf("Executing queries for user %q", user.Name))

			err := ExecuteQueriesForUser(ctx, infraInput, user.Name, user.ClientId, user.ClientSecret, host, warehouseId)
			if err != nil {
				return fmt.Errorf("execute queries for user %s: %w", user, err)
			}

			return nil
		})
	}

	return wg.Wait().ErrorOrNil()
}

func ExecuteQueriesForUser(ctx context.Context, infraInput *InfrastructureInput, name, clientId, clientSecret, host string, warehouseId string) error {
	logger.Info(fmt.Sprintf("Executing queries for user %s", name))

	client, err := databricks.NewWorkspaceClient(&databricks.Config{
		ClientID:     clientId,
		ClientSecret: clientSecret,
		Host:         host,
	})
	if err != nil {
		return fmt.Errorf("create workspace client: %w", err)
	}

	allTables := make([]string, 0, len(infraInput.TestingTables.Value)+len(infraInput.DemoTables.Value))
	allTables = append(allTables, infraInput.TestingTables.Value...)
	allTables = append(allTables, infraInput.DemoTables.Value...)

	for _, table := range allTables {
		r := rand.Intn(5)

		for range r {
			statement := fmt.Sprintf("SELECT * FROM %s LIMIT 1000", table)
			logger.Info(fmt.Sprintf("Executing statement: %s", statement))

			_, err = client.StatementExecution.ExecuteStatement(ctx, sql.ExecuteStatementRequest{
				RowLimit:    1000,
				Statement:   statement,
				WarehouseId: warehouseId,
			})
			if err != nil {
				logger.Info(fmt.Sprintf("execute statement failed for table %s: %v", table, err.Error()))
			} else {
				logger.Info(fmt.Sprintf("execute statement succeeded for table %s", table))
			}
		}
	}

	return nil
}

func main() {
	logger = hclog.New(&hclog.LoggerOptions{Name: "usage-logger", Level: hclog.Info})

	var host string
	var warehouseId string

	flag.StringVar(&host, "dbHost", "", "databricks workspace host")
	flag.StringVar(&warehouseId, "dbWarehouseId", "", "databricks warehouse id")
	flag.Parse()

	info, err := os.Stdin.Stat()
	if err != nil {
		panic(err)
	}

	if info.Mode()&os.ModeCharDevice != 0 {
		fmt.Println("The command is intended to work with pipes.")
		return
	}

	dec := json.NewDecoder(os.Stdin)

	usageConfig := InfrastructureInput{}

	err = dec.Decode(&usageConfig)
	if err != nil {
		panic(err)
	}

	err = CreateUsage(&usageConfig, usageConfig.Personas.Value, host, warehouseId)
	if err != nil {
		panic(err)
	}
}
