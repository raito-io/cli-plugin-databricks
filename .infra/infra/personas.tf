data "databricks_user" "benjamin" {
  provider  = databricks.accounts
  user_name = var.benjamin_user_name
}
// This list is not complete but is only used to generate data usage
data "databricks_user" "carla" {
  provider  = databricks.accounts
  user_name = var.carla_user_name
}

data "databricks_user" "dustin" {
  provider  = databricks.accounts
  user_name = var.dustin_user_name
}

data "databricks_user" "mary" {
  provider  = databricks.accounts
  user_name = var.mary_user_name
}

data "databricks_user" "nick" {
  provider  = databricks.accounts
  user_name = var.nick_user_name
}

data "databricks_user" "current_user" {
  provider  = databricks.accounts
  user_name = var.databricks_username
}

data "databricks_service_principal" "raitoServicePrincipal" {
  provider     = databricks.accounts
  display_name = "RaitoSync"
}

module "group_finance" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_group"
  display_name = "FINANCE_TF_2"
  workspace_id = [local.workspace_id]
}

module "group_data_analyst" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_group"
  display_name = "DATA_ANALYST_TF_2"
  workspace_id = [local.workspace_id]
}

module "group_human_resources" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_group"
  display_name = "HUMAN_RESOURCES_TF_2"
  workspace_id = [local.workspace_id]
  members      = { benjamin : data.databricks_user.benjamin.id, mary : data.databricks_user.mary.id }
}

module "group_marketing" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_group"
  display_name = "MARKETING_TF_2"
  workspace_id = [local.workspace_id]
}

module "group_sales" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_group"
  display_name = "SALES_TF_2"
  workspace_id = [local.workspace_id]
  members      = { dustin : data.databricks_user.dustin.id, mary : data.databricks_user.mary.id }
}

module "group_sales_analysis" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_group"
  display_name = "SALES_ANALYSIS_TF_2"
  workspace_id = [local.workspace_id]
  members      = { mary : data.databricks_user.mary.id }
}

module "group_sales_ext" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_group"
  display_name = "SALES_EXT_TF_2"
  workspace_id = [local.workspace_id]
  members      = { nick : data.databricks_user.nick.id, sales : module.group_sales.group.id }
}

module "group_data_engineer" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_group"
  display_name = "DATA_ENGINEER_TF_2"
  workspace_id = [local.workspace_id]
  members      = { benjamin : data.databricks_user.benjamin.id }
}

module "group_data_engineer_sync" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_group"
  display_name = "DATA_ENGINEER_SYNC_TF_2"
  workspace_id = [local.workspace_id]
  members      = { data_engineer : module.group_data_engineer.group.id }
}