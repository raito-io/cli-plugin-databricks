package databricks

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/provisioning"
	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/raito-io/cli/base/access_provider/sync_from_target"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/data_usage"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"

	"cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/databricks/platform"
	"cli-plugin-databricks/databricks/repo"
	"cli-plugin-databricks/databricks/repo/types"
	"cli-plugin-databricks/databricks/utils"
)

const (
	DATABRICKS_DEFAULT_SCHEMA  = "default"
	DATABRICKS_DEFAULT_CATALOG = "hive_metastore"

	table_regex = "\x60?[a-zA-Z0-9_-]+\x60?((\\.\x60?[a-zA-Z0-9_-]+\x60?){0,2})"
)

//go:generate go run github.com/vektra/mockery/v2 --name=dataUsageAccountRepository
type dataUsageAccountRepository interface {
	ListMetastores(ctx context.Context) ([]catalog.MetastoreInfo, error)
	GetWorkspaces(ctx context.Context) ([]provisioning.Workspace, error)
	GetWorkspaceMap(ctx context.Context, metastores []catalog.MetastoreInfo, workspaces []provisioning.Workspace) (map[string][]*provisioning.Workspace, map[string]string, error)
}

//go:generate go run github.com/vektra/mockery/v2 --name=dataUsageWorkspaceRepository
type dataUsageWorkspaceRepository interface {
	QueryHistory(ctx context.Context, startTime *time.Time, f func(context.Context, *sql.QueryInfo) error) error
	ListCatalogs(ctx context.Context) <-chan repo.ChannelItem[catalog.CatalogInfo]
	ListSchemas(ctx context.Context, catalogName string) <-chan repo.ChannelItem[catalog.SchemaInfo]
	ListTables(ctx context.Context, catalogName string, schemaName string) <-chan repo.ChannelItem[catalog.TableInfo]
}

var _ wrappers.DataUsageSyncer = (*DataUsageSyncer)(nil)

type DataUsageSyncer struct {
	accountRepoFactory   func(pltfrm platform.DatabricksPlatform, accountId string, repoCredentials *types.RepositoryCredentials) (dataUsageAccountRepository, error)
	workspaceRepoFactory func(*types.RepositoryCredentials) (dataUsageWorkspaceRepository, error)
}

func NewDataUsageSyncer() *DataUsageSyncer {
	return &DataUsageSyncer{
		accountRepoFactory: func(pltfrm platform.DatabricksPlatform, accountId string, repoCredentials *types.RepositoryCredentials) (dataUsageAccountRepository, error) {
			return repo.NewAccountRepository(pltfrm, repoCredentials, accountId)
		},
		workspaceRepoFactory: func(repoCredentials *types.RepositoryCredentials) (dataUsageWorkspaceRepository, error) {
			return repo.NewWorkspaceRepository(repoCredentials)
		},
	}
}

func (d *DataUsageSyncer) SyncDataUsage(ctx context.Context, fileCreator wrappers.DataUsageStatementHandler, configParams *config.ConfigMap) (err error) {
	defer func() {
		if err != nil {
			logger.Error(fmt.Sprintf("SyncDataUsage failed: %s", err.Error()))
		}
	}()

	metastores, workspaces, workspaceMetastoreMap, err := d.loadMetastores(ctx, configParams)
	if err != nil {
		return err
	}

	metastoreMap := make(map[string]catalog.MetastoreInfo)
	for i := range metastores {
		metastoreMap[metastores[i].MetastoreId] = metastores[i]
	}

	for wi := range workspaces {
		metastoreId := workspaceMetastoreMap[workspaces[wi].DeploymentName]

		metastore, ok := metastoreMap[metastoreId]
		if !ok {
			return fmt.Errorf("metastore %s not found", metastoreId)
		}

		err = d.syncWorkspace(ctx, &workspaces[wi], &metastore, fileCreator, configParams)
		if err != nil {
			logger.Warn(fmt.Sprintf("Sync data usage for metastore %s failed: %s", metastore.Name, err.Error()))
		}
	}

	return nil
}

func (d *DataUsageSyncer) syncWorkspace(ctx context.Context, workspace *provisioning.Workspace, metastore *catalog.MetastoreInfo, fileCreator wrappers.DataUsageStatementHandler, configParams *config.ConfigMap) error {
	logger.Info(fmt.Sprintf("Syncing workspace %s", workspace.DeploymentName))

	pltfrm, _, repoCredentials, err := utils.GetAndValidateParameters(configParams)
	if err != nil {
		return fmt.Errorf("get credentials: %w", err)
	}

	credentials, err := utils.InitializeWorkspaceRepoCredentials(repoCredentials, pltfrm, workspace)
	if err != nil {
		return fmt.Errorf("initialize workspace repo credentials: %w", err)
	}

	repo, err := d.workspaceRepoFactory(credentials)
	if err != nil {
		return fmt.Errorf("get workspace repository: %w", err)
	}

	numberOfDays := configParams.GetIntWithDefault(constants.DatabricksDataUsageWindow, 30)
	if numberOfDays > 30 {
		logger.Info(fmt.Sprintf("Capping data usage window to 30 days (from %d days)", numberOfDays))
		numberOfDays = 30
	}

	if numberOfDays <= 0 {
		logger.Info(fmt.Sprintf("Invalid input for data usage window (%d), setting to default 14 days", numberOfDays))
		numberOfDays = 14
	}

	startDate := time.Now().Truncate(24*time.Hour).AddDate(0, 0, -numberOfDays)

	userLastUsage := make(map[string]*UserDefaults)

	tableInfoMap, err := d.getTableInfoMap(ctx, repo)
	if err != nil {
		return fmt.Errorf("get table info map: %w", err)
	}

	err = repo.QueryHistory(ctx, &startDate, func(ctx context.Context, queryInfo *sql.QueryInfo) error {
		query := cleanUpQueryText(queryInfo.QueryText)

		var whatItems []sync_from_target.WhatItem
		var bytes int
		var rows int

		switch queryInfo.StatementType {
		case sql.QueryStatementTypeUse:
			err = d.useStatement(queryInfo, query, userLastUsage)
		case sql.QueryStatementTypeSelect:
			whatItems, bytes, rows = d.selectStatement(queryInfo, query, tableInfoMap, userLastUsage, metastore)
		case sql.QueryStatementTypeInsert:
			whatItems, bytes, rows = d.insertStatement(queryInfo, query, tableInfoMap, userLastUsage, metastore)
		case sql.QueryStatementTypeMerge:
			whatItems, bytes, rows = d.mergeStatement(queryInfo, query, tableInfoMap, userLastUsage, metastore)
		case sql.QueryStatementTypeUpdate:
			whatItems, bytes, rows = d.updateStatement(queryInfo, query, tableInfoMap, userLastUsage, metastore)
		case sql.QueryStatementTypeDelete:
			whatItems, bytes, rows = d.deleteStatement(queryInfo, query, tableInfoMap, userLastUsage, metastore)
		case sql.QueryStatementTypeCopy:
			whatItems, bytes, rows = d.copyStatement(queryInfo, query, tableInfoMap, userLastUsage, metastore)
		default:
			logger.Debug(fmt.Sprintf("Ignore query type: %s", queryInfo.StatementType))
		}

		if err != nil {
			logger.Warn(fmt.Sprintf("Failed to parse query: %s", err.Error()))

			return nil
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
				return fmt.Errorf("add query history statement: %w", err)
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
			if err != nil {
				return fmt.Errorf("add query history statement: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *DataUsageSyncer) getTableInfoMap(ctx context.Context, workspaceRepo dataUsageWorkspaceRepository) (map[string][]catalog.TableInfo, error) {
	tableSchemaCatalogMap := make(map[string][]catalog.TableInfo)

	catalogs := workspaceRepo.ListCatalogs(ctx)

	for ci := range catalogs {
		if ci.HasError() {
			return nil, fmt.Errorf("list catalogs: %w", ci.Error())
		}

		catalogItem := ci.I

		schemas := workspaceRepo.ListSchemas(ctx, catalogItem.Name)

		for si := range schemas {
			if si.HasError() {
				return nil, fmt.Errorf("list schemas: %w", si.Error())
			}

			schemaItem := si.I

			tables := workspaceRepo.ListTables(ctx, catalogItem.Name, schemaItem.Name)

			for ti := range tables {
				if ti.HasError() {
					return nil, fmt.Errorf("list tables: %w", ti.Error())
				}

				tableItem := ti.Item()

				tableSchemaCatalogMap[tableItem.Name] = append(tableSchemaCatalogMap[tableItem.Name], tableItem)
			}
		}
	}

	return tableSchemaCatalogMap, nil
}

var useCatalogRegex = regexp.MustCompile(`(?im)^use\s+(catalog|database)\s+(?P<catalog>.*)$`)
var useSchemaRegex = regexp.MustCompile(`(?im)^use\s+(schema\s+)?(?P<schema>.*)$`)

func (d *DataUsageSyncer) useStatement(queryInfo *sql.QueryInfo, parsedQuery string, userLastUsage map[string]*UserDefaults) error {
	catalogSubstrings := useCatalogRegex.FindStringSubmatch(parsedQuery)

	if len(catalogSubstrings) > 0 {
		i := useCatalogRegex.SubexpIndex("catalog")
		catalogName := strings.ReplaceAll(catalogSubstrings[i], "`", "")

		if _, ok := userLastUsage[queryInfo.UserName]; !ok {
			userLastUsage[queryInfo.UserName] = NewUserDefaults()
		}

		userLastUsage[queryInfo.UserName].SetCatalogName(catalogName)

		return nil
	}

	schemaSubstrings := useSchemaRegex.FindStringSubmatch(parsedQuery)

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

var selectRegex = regexp.MustCompile(fmt.Sprintf(`(?im)select\s+.*?\s+FROM\s+(?P<tables>%[1]s(\s*,\s*%[1]s)*)`, table_regex))

func (d *DataUsageSyncer) selectStatement(queryInfo *sql.QueryInfo, parsedQuery string, tableInfo map[string][]catalog.TableInfo, userLastUsage map[string]*UserDefaults, metastore *catalog.MetastoreInfo) ([]sync_from_target.WhatItem, int, int) {
	logger.Debug(fmt.Sprintf("parsing select query: %s", parsedQuery))

	matchingTables := selectRegex.FindAllStringSubmatch(parsedQuery, -1)
	if len(matchingTables) == 0 {
		return nil, 0, 0
	}

	matchingTablesStrings := make([]string, 0, len(matchingTables))

	tableIndex := selectRegex.SubexpIndex("tables")

	for i := range matchingTables {
		tables := strings.Split(matchingTables[i][tableIndex], ",")
		for _, table := range tables {
			t := strings.TrimSpace(table)
			matchingTablesStrings = append(matchingTablesStrings, t)
		}
	}

	return d.generateWhatItemsFromTable(matchingTablesStrings, queryInfo.UserName, tableInfo, userLastUsage, metastore, "SELECT"), queryInfo.Metrics.ReadBytes, queryInfo.Metrics.RowsProducedCount
}

var updateRegex = regexp.MustCompile(fmt.Sprintf(`(?im)UPDATE\s+(?P<table>%[1]s)`, table_regex))

func (d *DataUsageSyncer) updateStatement(queryInfo *sql.QueryInfo, parsedQuery string, tableInfo map[string][]catalog.TableInfo, userLastUsage map[string]*UserDefaults, metastore *catalog.MetastoreInfo) ([]sync_from_target.WhatItem, int, int) {
	logger.Debug(fmt.Sprintf("parsing update query: %s", parsedQuery))

	return d.parseRegularStatementWithSingleGroup(updateRegex, parsedQuery, "table", "UPDATE", queryInfo, tableInfo, userLastUsage, metastore, d.selectStatement)
}

var mergeRegex = regexp.MustCompile(fmt.Sprintf(`(?im)MERGE\s+INTO\s+(?P<target_table>%[1]s).*?\sUSING\s+(?P<source_table>%[1]s)`, table_regex))

func (d *DataUsageSyncer) mergeStatement(queryInfo *sql.QueryInfo, parsedQuery string, tableInfo map[string][]catalog.TableInfo, userLastUsage map[string]*UserDefaults, metastore *catalog.MetastoreInfo) ([]sync_from_target.WhatItem, int, int) {
	logger.Debug(fmt.Sprintf("parsing merge query: %s", parsedQuery))

	matchingTables := mergeRegex.FindAllStringSubmatch(parsedQuery, -1)
	if len(matchingTables) == 0 {
		return nil, 0, 0
	}

	matchingTargetTablesStrings := make([]string, 0, 1)
	matchingSourceTablesStrings := make([]string, 0, 1)

	tableIndex := mergeRegex.SubexpIndex("target_table")
	sourceTableIndex := mergeRegex.SubexpIndex("source_table")

	for i := range matchingTables {
		matchingTargetTablesStrings = append(matchingTargetTablesStrings, matchingTables[i][tableIndex])
		matchingSourceTablesStrings = append(matchingSourceTablesStrings, matchingTables[i][sourceTableIndex])
	}

	whatItems := d.generateWhatItemsFromTable(matchingTargetTablesStrings, queryInfo.UserName, tableInfo, userLastUsage, metastore, "MERGE")
	whatItems = append(whatItems, d.generateWhatItemsFromTable(matchingSourceTablesStrings, queryInfo.UserName, tableInfo, userLastUsage, metastore, "SELECT")...)

	return whatItems, queryInfo.Metrics.ReadBytes, queryInfo.Metrics.RowsProducedCount
}

var insertRegex = regexp.MustCompile(fmt.Sprintf(`(?mi)INSERT\s+(INTO|OVERWRITE)\s+(?P<table>%[1]s)(\s+TABLE\s+(?P<sourceTable>%[1]s))?`, table_regex))

func (d *DataUsageSyncer) insertStatement(queryInfo *sql.QueryInfo, parsedQuery string, tableInfo map[string][]catalog.TableInfo, userLastUsage map[string]*UserDefaults, metastore *catalog.MetastoreInfo) ([]sync_from_target.WhatItem, int, int) {
	logger.Debug(fmt.Sprintf("parsing insert query: %s", parsedQuery))

	matchingTables := insertRegex.FindAllStringSubmatch(parsedQuery, -1)
	if len(matchingTables) == 0 {
		return nil, 0, 0
	}

	matchingTargetTablesStrings := make([]string, 0, len(matchingTables))
	matchingSourceTablesStrings := make([]string, 0, len(matchingTables))

	tableIndex := insertRegex.SubexpIndex("table")
	sourceTableIndex := insertRegex.SubexpIndex("sourceTable")

	for i := range matchingTables {
		matchingTargetTablesStrings = append(matchingTargetTablesStrings, matchingTables[i][tableIndex])

		if len(matchingTables[i]) > sourceTableIndex {
			matchingSourceTablesStrings = append(matchingSourceTablesStrings, matchingTables[i][sourceTableIndex])
		}
	}

	whatItems := d.generateWhatItemsFromTable(matchingTargetTablesStrings, queryInfo.UserName, tableInfo, userLastUsage, metastore, "INSERT")
	whatItems = append(whatItems, d.generateWhatItemsFromTable(matchingSourceTablesStrings, queryInfo.UserName, tableInfo, userLastUsage, metastore, "SELECT")...)

	selectWhatItems, _, _ := d.selectStatement(queryInfo, parsedQuery, tableInfo, userLastUsage, metastore)
	whatItems = append(whatItems, selectWhatItems...)

	return whatItems, queryInfo.Metrics.ReadBytes, queryInfo.Metrics.RowsProducedCount
}

var deleteRegex = regexp.MustCompile(fmt.Sprintf(`(?mi)DELETE\s+FROM\s+(?P<table>%[1]s)`, table_regex))

func (d *DataUsageSyncer) deleteStatement(queryInfo *sql.QueryInfo, parsedQuery string, tableInfo map[string][]catalog.TableInfo, userLastUsage map[string]*UserDefaults, metastore *catalog.MetastoreInfo) ([]sync_from_target.WhatItem, int, int) {
	logger.Debug(fmt.Sprintf("parsing delete query: %s", parsedQuery))

	return d.parseRegularStatementWithSingleGroup(deleteRegex, parsedQuery, "table", "DELETE", queryInfo, tableInfo, userLastUsage, metastore, d.selectStatement)
}

var copyRegex = regexp.MustCompile(fmt.Sprintf(`(?mi)COPY\s+INTO\s+(?P<table>%[1]s)`, table_regex))

func (d *DataUsageSyncer) copyStatement(queryInfo *sql.QueryInfo, parsedQuery string, tableInfo map[string][]catalog.TableInfo, userLastUsage map[string]*UserDefaults, metastore *catalog.MetastoreInfo) ([]sync_from_target.WhatItem, int, int) {
	logger.Debug(fmt.Sprintf("parsing copy query: %s", parsedQuery))

	return d.parseRegularStatementWithSingleGroup(copyRegex, parsedQuery, "table", "COPY", queryInfo, tableInfo, userLastUsage, metastore, d.selectStatement)
}

func (d *DataUsageSyncer) parseRegularStatementWithSingleGroup(regex *regexp.Regexp, parsedQuery string, groupName string, permission string, queryInfo *sql.QueryInfo, tableInfo map[string][]catalog.TableInfo, userLastUsage map[string]*UserDefaults, metastore *catalog.MetastoreInfo, additionalFns ...func(queryInfo *sql.QueryInfo, parsedQuery string, tableInfo map[string][]catalog.TableInfo, userLastUsage map[string]*UserDefaults, metastore *catalog.MetastoreInfo) ([]sync_from_target.WhatItem, int, int)) ([]sync_from_target.WhatItem, int, int) {
	matchingTables := regex.FindAllStringSubmatch(parsedQuery, -1)
	if len(matchingTables) == 0 {
		return nil, 0, 0
	}

	matchingTablesStrings := make([]string, 0, len(matchingTables))

	tableIndex := regex.SubexpIndex(groupName)

	for i := range matchingTables {
		matchingTablesStrings = append(matchingTablesStrings, matchingTables[i][tableIndex])
	}

	whatItems := d.generateWhatItemsFromTable(matchingTablesStrings, queryInfo.UserName, tableInfo, userLastUsage, metastore, permission)

	for _, fn := range additionalFns {
		additionalWhatItems, _, _ := fn(queryInfo, parsedQuery, tableInfo, userLastUsage, metastore)
		whatItems = append(whatItems, additionalWhatItems...)
	}

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

				for i := range tableInfo[tableNameParts[len(tableNameParts)-1]] {
					if tableInfo[tableNameParts[len(tableNameParts)-1]][i].FullName == fullName {
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

				for i := range tableInfo[tableName] {
					if tableInfo[tableName][i].FullName == fullName {
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

func (d *DataUsageSyncer) loadMetastores(ctx context.Context, configMap *config.ConfigMap) ([]catalog.MetastoreInfo, []provisioning.Workspace, map[string]string, error) {
	pltfrm, accountId, repoCredentials, err := utils.GetAndValidateParameters(configMap)
	if err != nil {
		return nil, nil, nil, err
	}

	accountClient, err := d.accountRepoFactory(pltfrm, accountId, &repoCredentials)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("account repository factory: %w", err)
	}

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

var commentLineRegex = regexp.MustCompile(`(?m)--.*$`)
var commentBlockRegex = regexp.MustCompile(`/\\*.*\\*/`)
var withSpaceRegex = regexp.MustCompile(`\s+`)

func cleanUpQueryText(queryText string) string {
	// Remove all comments lines
	queryText = commentLineRegex.ReplaceAllString(queryText, " ")

	// Remove all new lines
	queryText = strings.ReplaceAll(queryText, "\n", "")

	// Remove all comment blocks
	queryText = commentBlockRegex.ReplaceAllString(queryText, " ")

	// Remove all unnecessary whitespace
	queryText = withSpaceRegex.ReplaceAllString(queryText, " ")

	// Add new line after every semicolon that is not part of a literal
	var builder strings.Builder
	literalParsing := false
	escapeChars := 0

	for i := 0; i < len(queryText); i++ {
		r := rune(queryText[i])

		builder.WriteRune(r)

		if r == '\\' {
			escapeChars++
			continue
		}

		escapeChars = 0

		if r == ';' && !literalParsing {
			builder.WriteRune('\n')
			i += 1
		} else if r == '\'' && escapeChars%2 == 0 {
			literalParsing = !literalParsing
		}
	}

	return builder.String()
}
