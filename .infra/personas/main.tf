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