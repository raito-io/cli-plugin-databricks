resource "databricks_grant" "catalog" {
  catalog = databricks_catalog.master_catalog.name

  for_each = tomap({
    benjamin = var.benjamin_user_name,
    carla = var.carla_user_name,
    dustin = var.dustin_user_name,
    mary = var.mary_user_name,
    nick = var.nick_user_name,
    owner_group = var.master_owner_group_name,
    me = data.databricks_current_user.me.user_name
  })

  principal  = each.value
  privileges = ["USE_CATALOG", "USE_SCHEMA"]
}

resource "databricks_grant" "catalog_owner" {
  catalog = databricks_catalog.master_catalog.name

  principal  = data.databricks_current_user.me.user_name
  privileges = ["ALL_PRIVILEGES"]
}


resource "databricks_grant" "catalog_owner_master_group" {
  catalog = databricks_catalog.master_catalog.name

  principal  = var.master_owner_group_name
  privileges = ["ALL_PRIVILEGES"]
}

locals {
  grants = [
    {
      table = databricks_sql_table.bill_of_materials
      grant = [{
        principal  = var.finance_group_name
        privileges = ["SELECT"]
      }]
    },
    {
      table = databricks_sql_table.department
      grant = [
        {
          principal  = var.human_resources_group_name
          privileges = ["SELECT", "MODIFY"]
        },
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.employee
      grant = [
        {
          principal  = var.human_resources_group_name
          privileges = ["SELECT", "MODIFY"]
        },
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.empoyee_department_history
      grant = [
        {
          principal  = var.human_resources_group_name
          privileges = ["SELECT", "MODIFY"]
        },
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.job_candidate
      grant = [
        {
          principal  = var.human_resources_group_name
          privileges = ["SELECT", "MODIFY"]
        },
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.shift
      grant = [
        {
          principal  = var.human_resources_group_name
          privileges = ["SELECT", "MODIFY"]
        },
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.country_region_currency
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.credit_card,
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.currency
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.currency_rate
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.customer
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.customer_eu
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.person_creditcard,
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.sales_order_detail
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.sales_order_header_sales_reason
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.sales_person
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.sales_person_quota_history
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.sales_reason
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.sales_tax_rate
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.sales_territory
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.sales_territory_history
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.shopping_cart_item
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.special_offer
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.special_offer_product
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.store
      grant = [
        {
          principal  = var.sales_analysis_group_name,
          privileges = ["SELECT"]
        },
        {
          principal  = var.sales_ext_group_name,
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.address
      grant = [
        {
          principal  = var.sales_ext_group_name
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.address_type
      grant = [
        {
          principal  = var.sales_ext_group_name
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.business_entity
      grant = [
        {
          principal  = var.sales_ext_group_name
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.business_entity_address
      grant = [
        {
          principal  = var.sales_ext_group_name
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.business_entity_contact
      grant = [
        {
          principal  = var.sales_ext_group_name
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.country_region
      grant = [
        {
          principal  = var.sales_ext_group_name
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.email_address
      grant = [
        {
          principal  = var.sales_ext_group_name
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.person_phone
      grant = [
        {
          principal  = var.sales_ext_group_name
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.phone_number_type
      grant = [
        {
          principal  = var.sales_ext_group_name
          privileges = ["SELECT"]
        }
      ]
    },
    {
      table = databricks_sql_table.state_province
      grant = [
        {
          principal  = var.sales_ext_group_name
          privileges = ["SELECT"]
        }
      ]
    }
  ]

  grants_flatten = toset(flatten([for grant in local.grants : [for g in grant.grant : { table : grant.table, principal : g.principal, privileges : g.privileges }]]))
}

resource "databricks_grant" "table_grant" {
  for_each = { for g in local.grants_flatten : format("%s#%s", g.table.name, g.principal) => g }

  table      = each.value.table.id
  principal  = each.value.principal
  privileges = each.value.privileges
}
