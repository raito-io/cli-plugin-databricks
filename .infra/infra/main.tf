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

  benjamin_user_name = var.benjamin_user_name
  carla_user_name    = var.carla_user_name
  dustin_user_name   = var.dustin_user_name
  mary_user_name     = var.mary_user_name
  nick_user_name     = var.nick_user_name

  finance_group_name            = var.finance_group_name
  data_analyst_group_name       = var.data_analyst_group_name
  human_resources_group_name    = var.human_resources_group_name
  marketing_group_name          = var.marketing_group_name
  sales_group_name              = var.sales_group_name
  sales_analysis_group_name     = var.sales_analysis_group_name
  sales_ext_group_name          = var.sales_ext_group_name
  data_engineer_group_name      = var.data_analyst_group_name
  data_engineer_sync_group_name = var.data_engineer_sync_group_name
}

module "testing" {
  source = "./testing"

  providers = {
    databricks = databricks.workspace
  }

  master_owner_group_name = var.owner_group_name

  benjamin_user_name = var.benjamin_user_name
  carla_user_name    = var.carla_user_name
  dustin_user_name   = var.dustin_user_name
  mary_user_name     = var.mary_user_name
  nick_user_name     = var.nick_user_name

  finance_group_name            = var.finance_group_name
  data_analyst_group_name       = var.data_analyst_group_name
  human_resources_group_name    = var.human_resources_group_name
  marketing_group_name          = var.marketing_group_name
  sales_group_name              = var.sales_group_name
  sales_analysis_group_name     = var.sales_analysis_group_name
  sales_ext_group_name          = var.sales_ext_group_name
  data_engineer_group_name      = var.data_analyst_group_name
  data_engineer_sync_group_name = var.data_engineer_sync_group_name
}