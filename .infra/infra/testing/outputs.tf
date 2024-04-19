output "catalog" {
  value = databricks_catalog.testing
}

output "tables" {
  value = [
    databricks_sql_table.department.id,
    databricks_sql_table.employee.id,
    databricks_sql_table.empoyee_department_history.id,
    databricks_sql_table.job_candidate.id,
    databricks_sql_table.shift.id,
    databricks_sql_table.address.id,
    databricks_sql_table.address_type.id,
    databricks_sql_table.business_entity.id,
    databricks_sql_table.business_entity_address.id,
    databricks_sql_table.business_entity_contact.id,
    databricks_sql_table.contact_type.id,
    databricks_sql_table.country_region.id,
    databricks_sql_table.email_address.id,
    databricks_sql_table.person_phone.id,
    databricks_sql_table.phone_number_type.id,
    databricks_sql_table.state_province.id,
  ]
}