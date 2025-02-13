output "testing_catalog" {
  value = module.testing.catalog
}

output "demo_catalog" {
  value = module.demo.catalog
}

output "testing_tables" {
  value = module.testing.tables
}

output "demo_tables" {
  value = module.demo.tables
}

output "workspace_id" {
  value = local.workspace_id
}