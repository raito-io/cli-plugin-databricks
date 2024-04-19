package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/hashicorp/go-hclog"
)

var logger hclog.Logger

type InfrastructureInput struct {
	TestingTables struct {
		Value []string `json:"value"`
	} `json:"testing_tables,omitempty"`
	DemoTables struct {
		Value []string `json:"value"`
	} `json:"demo_tables,omitempty"`
}

type UserInput struct {
	Username string
	Password string
}

func CreateUsage(infraInput *InfrastructureInput, users []UserInput, host string, warehouseId string) error {
	ctx := context.Background()

	for _, user := range users {
		logger.Info(fmt.Sprintf("Executing queries for user %q", user.Username))

		err := ExecuteQueriesForUser(ctx, infraInput, user.Username, user.Password, host, warehouseId)
		if err != nil {
			return fmt.Errorf("execute queries for user %s: %w", user, err)
		}
	}

	return nil
}

func ExecuteQueriesForUser(ctx context.Context, infraInput *InfrastructureInput, user string, password string, host string, warehouseId string) error {
	logger.Info(fmt.Sprintf("Executing queries for user %s", user))

	client, err := databricks.NewWorkspaceClient(&databricks.Config{
		Username: user,
		Password: password,
		Host:     host,
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

	var users string
	var host string
	var warehouseId string

	flag.StringVar(&host, "dbHost", "", "databricks workspace host")
	flag.StringVar(&users, "dbUsers", "", "list of users and passwords user1,password1;user2,password2")
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

	userArray := strings.Split(users, ";")
	userInput := make([]UserInput, 0, len(userArray))

	for _, user := range userArray {
		userPassword := strings.SplitN(user, ",", 2)
		userInput = append(userInput, UserInput{Username: userPassword[0], Password: userPassword[1]})
	}

	err = CreateUsage(&usageConfig, userInput, host, warehouseId)
	if err != nil {
		panic(err)
	}
}
