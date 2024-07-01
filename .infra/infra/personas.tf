module "benjamin" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_service_principal"
  display_name = "b_stewart"
  workspace_id = [local.workspace_id]
}

module "carla" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_service_principal"
  display_name = "c_harris"
  workspace_id = [local.workspace_id]
}

module "dustin" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_service_principal"
  display_name = "d_hayden"
  workspace_id = [local.workspace_id]
}

module "mary" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_service_principal"
  display_name = "m_carissa"
  workspace_id = [local.workspace_id]
}

module "nick" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_service_principal"
  display_name = "n_nguyen"
  workspace_id = [local.workspace_id]
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
  members      = { benjamin : module.benjamin.id, mary : module.mary.id }
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
  members      = { dustin : module.dustin.id, mary : module.mary.id }
}

module "group_sales_analysis" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_group"
  display_name = "SALES_ANALYSIS_TF_2"
  workspace_id = [local.workspace_id]
  members      = { mary : module.mary.id }
}

module "group_sales_ext" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_group"
  display_name = "SALES_EXT_TF_2"
  workspace_id = [local.workspace_id]
  members      = { nick : module.nick.id, sales : module.group_sales.group.id }
}

module "group_data_engineer" {
  providers = {
    databricks = databricks.accounts
  }

  source       = "./workspace_group"
  display_name = "DATA_ENGINEER_TF_2"
  workspace_id = [local.workspace_id]
  members      = { benjamin : module.benjamin.id }
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