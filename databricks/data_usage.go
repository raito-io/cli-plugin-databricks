package databricks

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/raito-io/cli/base/access_provider/sync_from_target"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/data_usage"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"
)

const (
	DATABRICKS_DEFAULT_SCHEMA  = "default"
	DATABRICKS_DEFAULT_CATALOG = "hive_metastore"
)

type dataUsageAccountRepository interface {
	ListMetastores(ctx context.Context) ([]catalog.MetastoreInfo, error)
	GetWorkspaces(ctx context.Context) ([]Workspace, error)
	GetWorkspaceMap(ctx context.Context, metastores []catalog.MetastoreInfo, workspaces []Workspace) (map[string][]string, map[string]string, error)
}

type dataUsageWorkspaceRepository interface {
	QueryHistory(ctx context.Context, startTime *time.Time) ([]sql.QueryInfo, error)
	ListCatalogs(ctx context.Context) ([]catalog.CatalogInfo, error)
	ListSchemas(ctx context.Context, catalogName string) ([]catalog.SchemaInfo, error)
	ListTables(ctx context.Context, catalogName string, schemaName string) ([]catalog.TableInfo, error)
}

var _ wrappers.DataUsageSyncer = (*DataUsageSyncer)(nil)

type DataUsageSyncer struct {
	accountRepoFactory   func(user string, password string, accountId string) dataUsageAccountRepository
	workspaceRepoFactory func(host string, user string, password string) (dataUsageWorkspaceRepository, error)
}

func NewDataUsageSyncer() *DataUsageSyncer {
	return &DataUsageSyncer{
		accountRepoFactory: func(user string, password string, accountId string) dataUsageAccountRepository {
			return NewAccountRepository(user, password, accountId)
		},
		workspaceRepoFactory: func(host string, user string, password string) (dataUsageWorkspaceRepository, error) {
			return NewWorkspaceRepository(host, user, password)
		},
	}
}

func (d *DataUsageSyncer) SyncDataUsage(ctx context.Context, fileCreator wrappers.DataUsageStatementHandler, configParams *config.ConfigMap) (err error) {
	defer func() {
		if err != nil {
			logger.Error(fmt.Sprintf("SyncDataUsage failed: %s", err.Error()))
		}
	}()

	metastores, worspaces, workspaceMetastoreMap, err := d.loadMetastores(ctx, configParams)
	if err != nil {
		return err
	}

	metastoreMap := make(map[string]catalog.MetastoreInfo)
	for _, metastore := range metastores {
		metastoreMap[metastore.MetastoreId] = metastore
	}

	for _, workspace := range worspaces {
		metastoreId := workspaceMetastoreMap[workspace.DeploymentName]
		metastore, ok := metastoreMap[metastoreId]
		if !ok {
			return fmt.Errorf("metastore %s not found", metastoreId)
		}

		err = d.syncWorkspace(ctx, &workspace, &metastore, fileCreator, configParams)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *DataUsageSyncer) syncWorkspace(ctx context.Context, workspace *Workspace, metastore *catalog.MetastoreInfo, fileCreator wrappers.DataUsageStatementHandler, configParams *config.ConfigMap) error {
	logger.Info(fmt.Sprintf("Syncing workspace %s", workspace.DeploymentName))

	_, username, password, err := getAndValidateParameters(configParams)
	if err != nil {
		return err
	}

	repo, err := d.workspaceRepoFactory(GetWorkspaceAddress(workspace.DeploymentName), username, password)
	if err != nil {
		return err
	}

	numberOfDays := configParams.GetIntWithDefault(DatabricksDataUsageWindow, 14)
	if numberOfDays > 90 {
		logger.Info(fmt.Sprintf("Capping data usage window to 90 days (from %d days)", numberOfDays))
		numberOfDays = 90
	}

	if numberOfDays <= 0 {
		logger.Info(fmt.Sprintf("Invalid input for data usage window (%d), setting to default 14 days", numberOfDays))
		numberOfDays = 14
	}

	startDate := time.Now().Truncate(24*time.Hour).AddDate(0, 0, -numberOfDays)

	userLastUsage := make(map[string]*UserDefaults)

	tableInfoMap, err := d.getTableInfoMap(ctx, repo)
	if err != nil {
		return err
	}

	queryHistory, err := repo.QueryHistory(ctx, &startDate)
	if err != nil {
		return err
	}

	for i := len(queryHistory) - 1; i >= 0; i-- {
		queryInfo := queryHistory[i]

		var whatItems []sync_from_target.WhatItem
		var bytes int
		var rows int

		switch queryInfo.StatementType {
		case sql.QueryStatementTypeUse:
			err = d.useStatement(&queryInfo, userLastUsage)
		case sql.QueryStatementTypeSelect:
			whatItems, bytes, rows = d.selectStatement(&queryInfo, tableInfoMap, userLastUsage, metastore)
		case sql.QueryStatementTypeInsert:
			whatItems, bytes, rows = d.insertStatement(&queryInfo, tableInfoMap, userLastUsage, metastore)
		case sql.QueryStatementTypeMerge:
			whatItems, bytes, rows = d.mergeStatement(&queryInfo, tableInfoMap, userLastUsage, metastore)
		case sql.QueryStatementTypeUpdate:
			whatItems, bytes, rows = d.updateStatement(&queryInfo, tableInfoMap, userLastUsage, metastore)
		case sql.QueryStatementTypeDelete:
			whatItems, bytes, rows = d.deleteStatement(&queryInfo, tableInfoMap, userLastUsage, metastore)
		default:
			logger.Debug(fmt.Sprintf("Ignore query type: %s", queryInfo.StatementType))
		}

		if err != nil {
			return err
		}

		if len(whatItems) > 0 {
			err = fileCreator.AddStatements([]data_usage.Statement{
				{
					ExternalId:          queryInfo.QueryId,
					AccessedDataObjects: whatItems,
					User:                queryInfo.UserName,
					Success:             queryInfo.Status == sql.QueryStatusFinished,
					Status:              string(queryInfo.Status),
					//Query:               queryInfo.QueryText, // App server assumes that the query is not parsable if we add this field
					StartTime: time.UnixMilli(int64(queryInfo.QueryStartTimeMs)).Unix(),
					EndTime:   time.UnixMilli(int64(queryInfo.QueryEndTimeMs)).Unix(),
					Bytes:     bytes,
					Rows:      rows,
				},
			})

			if err != nil {
				return err
			}
		} else {
			err = fileCreator.AddStatements([]data_usage.Statement{
				{
					ExternalId:          queryInfo.QueryId,
					AccessedDataObjects: nil,
					User:                queryInfo.UserName,
					Success:             queryInfo.Status == sql.QueryStatusFinished,
					Status:              string(queryInfo.Status),
					Query:               queryInfo.QueryText, // App server assumes that the query is not parsable if we add this field
					StartTime:           time.UnixMilli(int64(queryInfo.QueryStartTimeMs)).Unix(),
					EndTime:             time.UnixMilli(int64(queryInfo.QueryEndTimeMs)).Unix(),
					Bytes:               bytes,
					Rows:                rows,
				},
			})
		}
	}

	return nil
}

func (d *DataUsageSyncer) getTableInfoMap(ctx context.Context, workspaceRepo dataUsageWorkspaceRepository) (map[string][]catalog.TableInfo, error) {
	tableSchemaCatalogMap := make(map[string][]catalog.TableInfo)

	catalogs, err := workspaceRepo.ListCatalogs(ctx)
	if err != nil {
		return nil, err
	}

	for _, c := range catalogs {
		schemas, err := workspaceRepo.ListSchemas(ctx, c.Name)
		if err != nil {
			return nil, err
		}

		for _, s := range schemas {
			tables, err := workspaceRepo.ListTables(ctx, c.Name, s.Name)
			if err != nil {
				return nil, err
			}

			for _, t := range tables {
				tableSchemaCatalogMap[t.Name] = append(tableSchemaCatalogMap[t.Name], t)
			}
		}
	}

	return tableSchemaCatalogMap, nil
}

var useCatalogRegex = regexp.MustCompile("(?i)^use[[:space:]]+(catalog|database)[[:space:]]+(?P<catalog>.*)$")
var useSchemaRegex = regexp.MustCompile("(?i)^use[[:space:]]+(schema[[:space:]]+)?(?P<schema>.*)$")

func (d *DataUsageSyncer) useStatement(queryInfo *sql.QueryInfo, userLastUsage map[string]*UserDefaults) error {
	queryText := strings.TrimSpace(queryInfo.QueryText)

	catalogSubstrings := useCatalogRegex.FindStringSubmatch(queryText)

	if len(catalogSubstrings) > 0 {
		i := useCatalogRegex.SubexpIndex("catalog")
		catalogName := catalogSubstrings[i]

		if _, ok := userLastUsage[queryInfo.UserName]; !ok {
			userLastUsage[queryInfo.UserName] = NewUserDefaults()
		}

		userLastUsage[queryInfo.UserName].SetCatalogName(catalogName)

		return nil
	}

	schemaSubstrings := useSchemaRegex.FindStringSubmatch(queryText)

	if len(schemaSubstrings) > 0 {
		i := useSchemaRegex.SubexpIndex("schema")
		schemaName := schemaSubstrings[i]

		if _, ok := userLastUsage[queryInfo.UserName]; !ok {
			userLastUsage[queryInfo.UserName] = &UserDefaults{}
		}

		userLastUsage[queryInfo.UserName].SetSchemaName(schemaName)

		return nil
	}

	return fmt.Errorf("unable to parse use query: %s", queryInfo.QueryText)
}

var selectRegex = regexp.MustCompile(`(?i)select .*? FROM (?P<table>(\x60?[a-zA-Z0-9_-]+\x60?)((\.\x60?[a-zA-Z0-9_-]+\x60?){0,2}))`)

func (d *DataUsageSyncer) selectStatement(queryInfo *sql.QueryInfo, tableInfo map[string][]catalog.TableInfo, userLastUsage map[string]*UserDefaults, metastore *catalog.MetastoreInfo) ([]sync_from_target.WhatItem, int, int) {
	logger.Debug(fmt.Sprintf("parsing select query: %s", queryInfo.QueryText))

	matchingTables := selectRegex.FindAllStringSubmatch(queryInfo.QueryText, -1)
	if len(matchingTables) == 0 {
		return nil, 0, 0
	}

	matchingTablesStrings := make([]string, 0, len(matchingTables))

	tableIndex := selectRegex.SubexpIndex("table")

	for i := range matchingTables {
		matchingTablesStrings = append(matchingTablesStrings, matchingTables[i][tableIndex])
	}

	return d.generateWhatItemsFromTable(matchingTablesStrings, queryInfo.UserName, tableInfo, userLastUsage, metastore, "SELECT"), queryInfo.Metrics.ReadBytes, queryInfo.Metrics.RowsProducedCount
}

var updateRegex = regexp.MustCompile(`(?i)UPDATE[[:space:]](?P<table>(\x60?[a-zA-Z0-9_-]+\x60?)((\.\x60?[a-zA-Z0-9_-]+\x60?){0,2}))`)

func (d *DataUsageSyncer) updateStatement(queryInfo *sql.QueryInfo, tableInfo map[string][]catalog.TableInfo, userLastUsage map[string]*UserDefaults, metastore *catalog.MetastoreInfo) ([]sync_from_target.WhatItem, int, int) {
	logger.Debug(fmt.Sprintf("parsing update query: %s", queryInfo.QueryText))

	matchingTables := updateRegex.FindAllStringSubmatch(queryInfo.QueryText, -1)
	if len(matchingTables) == 0 {
		return nil, 0, 0
	}

	matchingTablesStrings := make([]string, 0, len(matchingTables))

	tableIndex := updateRegex.SubexpIndex("table")

	for i := range matchingTables {
		matchingTablesStrings = append(matchingTablesStrings, matchingTables[i][tableIndex])
	}

	whatItems := d.generateWhatItemsFromTable(matchingTablesStrings, queryInfo.UserName, tableInfo, userLastUsage, metastore, "UPDATE")

	selectWhatItems, _, _ := d.selectStatement(queryInfo, tableInfo, userLastUsage, metastore)

	whatItems = append(whatItems, selectWhatItems...)

	return whatItems, queryInfo.Metrics.ReadBytes, queryInfo.Metrics.RowsProducedCount
}

var mergeRegex = regexp.MustCompile(`(?im)MERGE INTO[[:space:]](?P<target_table>(\x60?[a-zA-Z0-9_-]+\x60?)((\.\x60?[a-zA-Z0-9_-]+\x60?){0,2})) .*? USING (?P<source_table>(\x60?[a-zA-Z0-9_-]+\x60?)((\.\x60?[a-zA-Z0-9_-]+\x60?){0,2}))`)

func (d *DataUsageSyncer) mergeStatement(queryInfo *sql.QueryInfo, tableInfo map[string][]catalog.TableInfo, userLastUsage map[string]*UserDefaults, metastore *catalog.MetastoreInfo) ([]sync_from_target.WhatItem, int, int) {
	logger.Debug(fmt.Sprintf("parsing merge query: %s", queryInfo.QueryText))

	matchingTables := mergeRegex.FindAllStringSubmatch(queryInfo.QueryText, -1)
	if len(matchingTables) == 0 {
		return nil, 0, 0
	}

	matchingTablesStrings := make([]string, 0, len(matchingTables))

	tableIndex := mergeRegex.SubexpIndex("target_table")
	sourceTableIndex := mergeRegex.SubexpIndex("source_table")

	for i := range matchingTables {
		matchingTablesStrings = append(matchingTablesStrings, matchingTables[i][tableIndex])
		matchingTablesStrings = append(matchingTablesStrings, matchingTables[i][sourceTableIndex])
	}

	whatItems := d.generateWhatItemsFromTable(matchingTablesStrings, queryInfo.UserName, tableInfo, userLastUsage, metastore, "MERGE")

	return whatItems, queryInfo.Metrics.ReadBytes, queryInfo.Metrics.RowsProducedCount
}

var insertRegex = regexp.MustCompile(`(?im)INSERT[[:space:]]+(INTO|OVERWRITE)?([[:space:]]+TABLE)?[[:space:]]+(?P<tale>(\x60?[a-zA-Z0-9_-]+\x60?)((\.\x60?[a-zA-Z0-9_-]+\x60?){0,2}))`)

func (d *DataUsageSyncer) insertStatement(queryInfo *sql.QueryInfo, tableInfo map[string][]catalog.TableInfo, userLastUsage map[string]*UserDefaults, metastore *catalog.MetastoreInfo) ([]sync_from_target.WhatItem, int, int) {
	logger.Debug(fmt.Sprintf("parsing insert query: %s", queryInfo.QueryText))

	matchingTables := insertRegex.FindAllStringSubmatch(queryInfo.QueryText, -1)
	if len(matchingTables) == 0 {
		return nil, 0, 0
	}

	matchingTablesStrings := make([]string, 0, len(matchingTables))

	tableIndex := insertRegex.SubexpIndex("tale")

	for i := range matchingTables {
		matchingTablesStrings = append(matchingTablesStrings, matchingTables[i][tableIndex])
	}

	whatItems := d.generateWhatItemsFromTable(matchingTablesStrings, queryInfo.UserName, tableInfo, userLastUsage, metastore, "INSERT")

	selectWhatItems, _, _ := d.selectStatement(queryInfo, tableInfo, userLastUsage, metastore)
	whatItems = append(whatItems, selectWhatItems...)

	return whatItems, queryInfo.Metrics.ReadBytes, queryInfo.Metrics.RowsProducedCount
}

var deleteRegex = regexp.MustCompile(`(?im)DELETE[[:space:]]+FROM[[:space:]]+(?P<tale>(\x60?[a-zA-Z0-9_-]+\x60?)((\.\x60?[a-zA-Z0-9_-]+\x60?){0,2}))`)

func (d *DataUsageSyncer) deleteStatement(queryInfo *sql.QueryInfo, tableInfo map[string][]catalog.TableInfo, userLastUsage map[string]*UserDefaults, metastore *catalog.MetastoreInfo) ([]sync_from_target.WhatItem, int, int) {
	logger.Debug(fmt.Sprintf("parsing delete query: %s", queryInfo.QueryText))

	matchingTables := deleteRegex.FindAllStringSubmatch(queryInfo.QueryText, -1)
	if len(matchingTables) == 0 {
		return nil, 0, 0
	}

	matchingTablesStrings := make([]string, 0, len(matchingTables))

	tableIndex := deleteRegex.SubexpIndex("tale")

	for i := range matchingTables {
		matchingTablesStrings = append(matchingTablesStrings, matchingTables[i][tableIndex])
	}

	whatItems := d.generateWhatItemsFromTable(matchingTablesStrings, queryInfo.UserName, tableInfo, userLastUsage, metastore, "DELETE")

	selectWhatItems, _, _ := d.selectStatement(queryInfo, tableInfo, userLastUsage, metastore)
	whatItems = append(whatItems, selectWhatItems...)

	return whatItems, queryInfo.Metrics.ReadBytes, queryInfo.Metrics.RowsProducedCount
}

func (d *DataUsageSyncer) generateWhatItemsFromTable(tableNames []string, userId string, tableInfo map[string][]catalog.TableInfo, userLastUsage map[string]*UserDefaults, metastore *catalog.MetastoreInfo, permissions ...string) []sync_from_target.WhatItem {
	data_object_names := set.NewSet[string]()

	for _, tableNameString := range tableNames {
		tableName := strings.ReplaceAll(tableNameString, "\x60", "")

		logger.Debug(fmt.Sprintf("Search for table: %s", tableName))

		tableNameParts := strings.Split(tableName, ".")

		if possibleTables, ok := tableInfo[tableNameParts[len(tableNameParts)-1]]; !ok || len(possibleTables) == 0 {
			logger.Warn(fmt.Sprintf("Table %s not found in metastore", tableName))
			continue
		}

		if len(tableNameParts) > 3 {
			logger.Warn(fmt.Sprintf("Ignoring table %s because it has too many parts", tableName))
		} else if len(tableNameParts) == 3 {
			logger.Debug(fmt.Sprintf("Full name defined: %s", tableName))
			data_object_names.Add(fmt.Sprintf("%s.%s", metastore.MetastoreId, tableName))
		} else if len(tableNameParts) == 2 {
			possibleCatalogs := make([]string, 0, 2)

			if userDefault, ok := userLastUsage[userId]; ok {
				possibleCatalogs = append(possibleCatalogs, userDefault.CatalogName)
			}

			possibleCatalogs = append(possibleCatalogs, DATABRICKS_DEFAULT_CATALOG)

			catalogFound := false
			for _, possibleCatalog := range possibleCatalogs {
				if catalogFound {
					break
				}

				fullName := fmt.Sprintf("%s.%s", possibleCatalog, tableName)

				for _, possibleTable := range tableInfo[tableNameParts[len(tableNameParts)-1]] {
					if possibleTable.FullName == fullName {
						logger.Debug(fmt.Sprintf("Found possible catalog by assuming use catalog %q", possibleCatalog))
						data_object_names.Add(fmt.Sprintf("%s.%s.%s", metastore.MetastoreId, possibleCatalog, tableName))
						catalogFound = true
						break
					}
				}
			}

			if !catalogFound {
				if len(tableInfo[tableNameParts[len(tableNameParts)-1]]) == 1 && tableInfo[tableNameParts[len(tableNameParts)-1]][0].SchemaName == tableNameParts[0] {
					ti := tableInfo[tableNameParts[len(tableNameParts)-1]][0]
					logger.Debug(fmt.Sprintf("Found possible catalog %q because only one option exists", ti.CatalogName))
					data_object_names.Add(fmt.Sprintf("%s.%s", metastore.MetastoreId, tableInfo[tableNameParts[len(tableNameParts)-1]][0].FullName))
				} else {
					logger.Warn(fmt.Sprintf("Table %s not found in metastore", tableName))
				}
			}
		} else if len(tableNameParts) == 1 {
			possibleCatalogSchemaNames := make([]string, 0, 2)

			if userDefault, ok := userLastUsage[userId]; ok {
				possibleCatalogSchemaNames = append(possibleCatalogSchemaNames, fmt.Sprintf("%s.%s", userDefault.CatalogName, userDefault.SchemaName))
			}

			possibleCatalogSchemaNames = append(possibleCatalogSchemaNames, fmt.Sprintf("%s.%s", DATABRICKS_DEFAULT_CATALOG, DATABRICKS_DEFAULT_SCHEMA))

			catalogSchemaFound := false
			for _, possibleCatalogSchemaName := range possibleCatalogSchemaNames {
				if catalogSchemaFound {
					break
				}

				fullName := fmt.Sprintf("%s.%s", possibleCatalogSchemaName, tableName)

				for _, possibleTable := range tableInfo[tableName] {
					if possibleTable.FullName == fullName {
						logger.Debug(fmt.Sprintf("Found possible catalog by assuming use catalog and schema %q", possibleCatalogSchemaName))
						data_object_names.Add(fmt.Sprintf("%s.%s.%s", metastore.MetastoreId, possibleCatalogSchemaName, tableName))
						catalogSchemaFound = true
						break
					}
				}
			}

			if !catalogSchemaFound {
				if len(tableInfo[tableName]) == 1 {
					ti := tableInfo[tableName][0]
					logger.Debug(fmt.Sprintf("Found possible catalog and schema \"%s.%s\" because only one option exists", ti.CatalogName, ti.SchemaName))
					data_object_names.Add(fmt.Sprintf("%s.%s", metastore.MetastoreId, ti.FullName))
				} else {
					logger.Warn(fmt.Sprintf("Table %s not found in metastore", tableName))
				}
			}
		}
	}

	result := make([]sync_from_target.WhatItem, 0, len(data_object_names))

	for dataObject := range data_object_names {
		result = append(result, sync_from_target.WhatItem{
			DataObject: &data_source.DataObjectReference{
				FullName: dataObject,
				Type:     data_source.Table,
			},
			Permissions: permissions,
		})
	}

	return result
}

func (d *DataUsageSyncer) loadMetastores(ctx context.Context, configMap *config.ConfigMap) ([]catalog.MetastoreInfo, []Workspace, map[string]string, error) {
	accountId, username, password, err := getAndValidateParameters(configMap)
	if err != nil {
		return nil, nil, nil, err
	}

	accountClient := d.accountRepoFactory(username, password, accountId)

	metastores, err := accountClient.ListMetastores(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	if len(metastores) == 0 {
		return nil, nil, nil, nil
	}

	workspaces, err := accountClient.GetWorkspaces(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	_, workspaceToMetastoreMap, err := accountClient.GetWorkspaceMap(ctx, metastores, workspaces)
	if err != nil {
		return nil, nil, nil, err
	}

	return metastores, workspaces, workspaceToMetastoreMap, nil
}

type UserDefaults struct {
	CatalogName string
	SchemaName  string
}

func NewUserDefaults() *UserDefaults {
	return &UserDefaults{
		CatalogName: DATABRICKS_DEFAULT_CATALOG,
		SchemaName:  DATABRICKS_DEFAULT_SCHEMA,
	}
}

func (d *UserDefaults) SetCatalogName(catalogName string) *UserDefaults {
	d.CatalogName = catalogName
	d.SchemaName = DATABRICKS_DEFAULT_SCHEMA

	return d
}

func (d *UserDefaults) SetSchemaName(schemaName string) *UserDefaults {
	d.SchemaName = schemaName

	return d
}
