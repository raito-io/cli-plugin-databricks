variable "databricks_client_id" {
  type        = string
  description = "Client ID of the Databricks account"
  nullable    = false
  sensitive   = true
}

variable "databricks_client_secret" {
  type        = string
  description = "Client secret of the Databricks account"
  nullable    = false
  sensitive   = true
}

variable "databricks_account_id" {
  type        = string
  description = "ID of the Databricks account"
  nullable    = false
  sensitive   = true
}

variable "databricks_host" {
  type        = string
  description = "Host of the Databricks account"
  default     = "https://accounts.cloud.databricks.com"
  sensitive   = false
}

variable "databricks_workspace_host" {
  type        = string
  description = "Host of the Databricks workspace"
  nullable    = false
  sensitive   = false
}

variable "databricks_metastore_id" {
  type        = string
  description = "ID of the Databricks metastore"
  nullable    = false
  sensitive   = true
}

variable "databricks_sql_warehouse_id" {
  type        = string
  description = "ID of the Databricks SQL Warehouse"
  nullable    = false
  sensitive   = false
}

variable "databricks_workspace_name" {
  type        = string
  description = "Name of the Databricks workspace"
  nullable    = false
  sensitive   = false
}

variable "owner_group_name" {
  type        = string
  description = "Group name that will be used as owner group. This group should contain user that is used by Terraform."
  nullable    = false
  sensitive   = false
}



### Personas Variables
variable "benjamin_user_name" {
  type     = string
  nullable = false
}

variable "carla_user_name" {
  type     = string
  nullable = false
}

variable "dustin_user_name" {
  type     = string
  nullable = false
}

variable "mary_user_name" {
  type     = string
  nullable = false
}

variable "nick_user_name" {
  type     = string
  nullable = false
}

variable "finance_group_name" {
  type     = string
  nullable = false
}

variable "data_analyst_group_name" {
  type     = string
  nullable = false
}

variable "human_resources_group_name" {
  type     = string
  nullable = false
}

variable "marketing_group_name" {
  type     = string
  nullable = false
}

variable "sales_group_name" {
  type     = string
  nullable = false
}

variable "sales_analysis_group_name" {
  type     = string
  nullable = false
}

variable "sales_ext_group_name" {
  type     = string
  nullable = false
}

variable "data_engineer_group_name" {
  type     = string
  nullable = false
}

variable "data_engineer_sync_group_name" {
  type     = string
  nullable = false
}