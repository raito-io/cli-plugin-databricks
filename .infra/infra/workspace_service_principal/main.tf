resource "databricks_service_principal" "principal" {
  display_name = var.display_name
}

resource "databricks_service_principal_secret" "secret" {
  service_principal_id = databricks_service_principal.principal.id
}

resource "databricks_mws_permission_assignment" "workspace_assignment" {
  for_each = var.workspace_id

  permissions = ["USER"]
  workspace_id = each.value
  principal_id = databricks_service_principal.principal.id
}