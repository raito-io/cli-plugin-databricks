output "display_name" {
  value = databricks_service_principal.principal.display_name
}

output "id" {
  value = databricks_service_principal.principal.id
}

output "application_id" {
  value = databricks_service_principal.principal.application_id
}

output "secret" {
  value = databricks_service_principal_secret.secret.secret
  sensitive = true
}