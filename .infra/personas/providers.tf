provider "databricks" {
  alias = "accounts"

  host = "https://accounts.cloud.databricks.com"

  client_id     = var.databricks_client_id
  client_secret = var.databricks_client_secret
  account_id    = var.databricks_account_id
}

provider "databricks" {
  alias = "workspace"

  host = var.databricks_workspace_host

  client_id     = var.databricks_client_id
  client_secret = var.databricks_client_secret
  account_id    = var.databricks_account_id
}