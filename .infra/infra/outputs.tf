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

output "personas" {
  sensitive = true
  value = [
    {
      id: module.benjamin.id
      name: module.benjamin.display_name
      client_id: module.benjamin.application_id
      client_secret: module.benjamin.secret
    },
    {
      id: module.carla.id
      name: module.carla.display_name
      client_id: module.carla.application_id
      client_secret: module.carla.secret
    },
    {
      id: module.dustin.id
      name: module.dustin.display_name
      client_id: module.dustin.application_id
      client_secret: module.dustin.secret
    },
    {
      id: module.mary.id
      name: module.mary.display_name
      client_id: module.mary.application_id
      client_secret: module.mary.secret
    },
    {
      id: module.nick.id
      name: module.nick.display_name
      client_id: module.nick.application_id
      client_secret: module.nick.secret
    }
  ]
}