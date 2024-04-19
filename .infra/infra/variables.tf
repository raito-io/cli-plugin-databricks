variable "demo_dataset" {
  type        = bool
  sensitive   = false
  description = "Infrastructure for demo purposes"
  default     = true
}

variable "testing_dataset" {
  type        = bool
  sensitive   = false
  description = "Infrastructure for testing purposes"
  default     = false
}

variable "databricks_username" {
  type        = string
  description = "Username of the Databricks account"
  nullable    = false
  sensitive   = false
}

variable "databricks_password" {
  type        = string
  description = "Password of the Databricks account"
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

variable "benjamin_user_name" {
  type        = string
  description = "Username of Benjamin"
  nullable    = false
  sensitive   = false
  default     = "b_stewart+databricks@raito.io"
}

variable "carla_user_name" {
  type        = string
  description = "Username of Carla"
  nullable    = false
  sensitive   = false
  default     = "c_harris+databricks@raito.io"
}

variable "dustin_user_name" {
  type        = string
  description = "Username of Dustin"
  nullable    = false
  sensitive   = false
  default     = "d_hayden+databricks2@raito.io"
}

variable "mary_user_name" {
  type        = string
  description = "Username of Mary"
  nullable    = false
  sensitive   = false
  default     = "m_carissa+databricks@raito.io"
}

variable "nick_user_name" {
  type        = string
  description = "Username of Nick"
  nullable    = false
  sensitive   = false
  default     = "n_nguyen+databricks@raito.io"
}

variable "owner_group_name" {
  type        = string
  description = "Group name that will be used as owner group. This group should contain user that is used by Terraform."
  nullable    = false
  sensitive   = false
}