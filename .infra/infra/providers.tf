provider "databricks" {
  alias = "accounts"

  host       = "https://accounts.cloud.databricks.com"
  username   = var.databricks_username
  password   = var.databricks_password
  account_id = var.databricks_account_id
}

provider "databricks" {
  alias = "workspace"

  host       = var.databricks_workspace_host
  username   = var.databricks_username
  password   = var.databricks_password
  account_id = var.databricks_account_id
}