data "databricks_metastore" "metastore" {
  provider     = databricks.accounts
  metastore_id = var.databricks_metastore_id
}

data "databricks_sql_warehouse" "default_warehouse" {
  provider = databricks.workspace
  id       = var.databricks_sql_warehouse_id
}

data "databricks_mws_workspaces" "workspaces" {
  provider = databricks.accounts
}

locals {
  workspace_id = data.databricks_mws_workspaces.workspaces.ids[var.databricks_workspace_name]
}

module "demo" {
  source = "./demo"

  providers = {
    databricks = databricks.workspace
  }

  master_owner_group_name = var.owner_group_name

  benjamin_user_name = module.benjamin.application_id
  carla_user_name    = module.carla.application_id
  dustin_user_name   = module.dustin.application_id
  mary_user_name     = module.mary.application_id
  nick_user_name     = module.nick.application_id

  finance_group_name            = module.group_finance.group.display_name
  data_analyst_group_name       = module.group_data_analyst.group.display_name
  human_resources_group_name    = module.group_human_resources.group.display_name
  marketing_group_name          = module.group_marketing.group.display_name
  sales_group_name              = module.group_sales.group.display_name
  sales_analysis_group_name     = module.group_sales_analysis.group.display_name
  sales_ext_group_name          = module.group_sales_ext.group.display_name
  data_engineer_group_name      = module.group_data_engineer.group.display_name
  data_engineer_sync_group_name = module.group_data_engineer_sync.group.display_name
}

module "testing" {
  source = "./testing"

  providers = {
    databricks = databricks.workspace
  }

  master_owner_group_name = var.owner_group_name

  benjamin_user_name = module.benjamin.application_id
  carla_user_name    = module.carla.application_id
  dustin_user_name   = module.dustin.application_id
  mary_user_name     = module.mary.application_id
  nick_user_name     = module.nick.application_id

  finance_group_name            = module.group_finance.group.display_name
  data_analyst_group_name       = module.group_data_analyst.group.display_name
  human_resources_group_name    = module.group_human_resources.group.display_name
  marketing_group_name          = module.group_marketing.group.display_name
  sales_group_name              = module.group_sales.group.display_name
  sales_analysis_group_name     = module.group_sales_analysis.group.display_name
  sales_ext_group_name          = module.group_sales_ext.group.display_name
  data_engineer_group_name      = module.group_data_engineer.group.display_name
  data_engineer_sync_group_name = module.group_data_engineer_sync.group.display_name
}