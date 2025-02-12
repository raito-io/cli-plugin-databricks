output "personas" {
  sensitive = true
  value = [
    {
      username: "benjamin",
      id : module.benjamin.id
      name : module.benjamin.display_name
      client_id : module.benjamin.application_id
      client_secret : module.benjamin.secret
    },
    {
      username: "carla",
      id : module.carla.id
      name : module.carla.display_name
      client_id : module.carla.application_id
      client_secret : module.carla.secret
    },
    {
      username: "dustin",
      id : module.dustin.id
      name : module.dustin.display_name
      client_id : module.dustin.application_id
      client_secret : module.dustin.secret
    },
    {
      username: "mary",
      id : module.mary.id
      name : module.mary.display_name
      client_id : module.mary.application_id
      client_secret : module.mary.secret
    },
    {
      username: "nick",
      id : module.nick.id
      name : module.nick.display_name
      client_id : module.nick.application_id
      client_secret : module.nick.secret
    }
  ]
}

output "groups" {
  sensitive = false
  value = [
    {
      groupname: "finance",
      id : module.group_finance.group.id
      name : module.group_finance.group.depends_on
    },
    {
      groupname: "data_analyst",
      id : module.group_data_analyst.group.id
      name : module.group_data_analyst.group.display_name
    },
    {
      groupname: "human_resources",
      id : module.group_human_resources.group.id
      name : module.group_human_resources.group.display_name
    },
    {
      groupname: "marketing",
      id : module.group_marketing.group.id
      name : module.group_marketing.group.display_name
    },
    {
      groupname: "sales",
      id : module.group_sales.group.id
      name : module.group_sales.group.display_name
    },
    {
      groupname: "sales_analysis",
      id : module.group_sales_analysis.group.id
      name : module.group_sales_analysis.group.display_name
    },
    {
      groupname: "sales_ext",
      id : module.group_sales_ext.group.id
      name : module.group_sales_ext.group.display_name
    },
    {
        groupname: "data_engineer",
        id : module.group_data_engineer.group.id
        name : module.group_data_engineer.group.display_name
      },
      {
        groupname: "data_engineer_sync",
        id : module.group_data_engineer_sync.group.id
        name : module.group_data_engineer_sync.group.display_name
    }
  ]
}