// Catalog - MASTER DATA
resource "databricks_catalog" "testing" {
  name          = "raito_testing"
  comment       = "Testing out catalog for TF"
  owner         = var.master_owner_group_name
  force_destroy = true
}

data "databricks_current_user" "me" {

}

// -- Schema HUMANRESOURCES
resource "databricks_schema" "humanresources" {
  catalog_name = databricks_catalog.testing.id
  name         = "humanresources"
}

// -- -- Table DEPARTMENT
resource "databricks_sql_table" "department" {
  catalog_name = databricks_schema.humanresources.catalog_name
  schema_name  = databricks_schema.humanresources.name
  name         = "department"
  table_type   = "MANAGED"
  comment      = "Lookup table containing the departments within the Adventure Works Cycles company."

  column {
    name = "DepartmentID"
    type = "decimal(38,0)"
  }

  column {
    name = "GroupName"
    type = "string"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "Name"
    type = "string"
  }
}

// -- -- View department_view
resource "databricks_sql_table" "department_view" {
  catalog_name = databricks_schema.humanresources.catalog_name
  schema_name  = databricks_schema.humanresources.name
  name         = "department"
  table_type   = "VIEW"
  comment      = "Lookup table containing the departments within the Adventure Works Cycles company where department ID is bigger than 100."

  view_definition = "SELECT * FROM department WHERE DepartmentID > 100"
}

// -- -- Table EMPLOYEE
resource "databricks_sql_table" "employee" {
  catalog_name = databricks_schema.humanresources.catalog_name
  schema_name  = databricks_schema.humanresources.name
  name         = "employee"
  table_type   = "MANAGED"
  comment      = "Employee information such as salary, department, and title."

  column {
    name = "BirthDate"
    type = "date"
  }

  column {
    name = "BusinessEntityID"
    type = "decimal(38,0)"
  }

  column {
    name = "CurrentFlag"
    type = "string"
  }

  column {
    name = "Gender"
    type = "string"
  }

  column {
    name = "HireDate"
    type = "date"
  }

  column {
    name = "JobTitle"
    type = "string"
  }

  column {
    name = "LoginID"
    type = "decimal(38,0)"
  }

  column {
    name = "MaritalStatus"
    type = "string"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "NationalIDNumber"
    type = "string"
  }

  column {
    name = "OrganizationLevel"
    type = "string"
  }

  column {
    name = "OrganizationNode"
    type = "string"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }

  column {
    name = "SalariedFlag"
    type = "string"
  }

  column {
    name = "SickLeaveHours"
    type = "decimal(38,0)"
  }

  column {
    name = "VacationHours"
    type = "decimal(38,0)"
  }
}

// -- -- Table EMPLOYEEDEPARTMENTHISTORY
resource "databricks_sql_table" "empoyee_department_history" {
  catalog_name = databricks_schema.humanresources.catalog_name
  schema_name  = databricks_schema.humanresources.name
  name         = "employeedepartmenthistory"
  table_type   = "MANAGED"
  comment      = "Employee department transfers."

  column {
    name = "BusinessEntityID"
    type = "decimal(38,0)"
  }

  column {
    name = "DepartmentID"
    type = "decimal(38,0)"
  }

  column {
    name = "EndDate"
    type = "date"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ShiftID"
    type = "decimal(38,0)"
  }

  column {
    name = "StartDate"
    type = "date"
  }
}

// -- -- Table JOBCANDIDATE
resource "databricks_sql_table" "job_candidate" {
  catalog_name = databricks_schema.humanresources.catalog_name
  schema_name  = databricks_schema.humanresources.name
  name         = "jobcandidate"
  table_type   = "MANAGED"
  comment      = "Résumés submitted to Human Resources by job applicants."

  column {
    name = "BusinessEntityID"
    type = "decimal(38,0)"
  }

  column {
    name = "JobCandidateID"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "Resume"
    type = "string"
  }
}

// -- -- Table SHIFT
resource "databricks_sql_table" "shift" {
  catalog_name = databricks_schema.humanresources.catalog_name
  schema_name  = databricks_schema.humanresources.name
  name         = "shift"
  table_type   = "MANAGED"
  comment      = "Work shift lookup table."

  column {
    name = "EndTime"
    type = "timestamp"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }


  column {
    name = "Name"
    type = "string"
  }

  column {
    name = "ShiftID"
    type = "decimal(38,0)"
  }

  column {
    name = "StartTime"
    type = "timestamp"
  }
}

// -- Schema PERSON
resource "databricks_schema" "person" {
  catalog_name = databricks_catalog.testing.id
  name         = "person"
}

// -- -- Table ADDRESS
resource "databricks_sql_table" "address" {
  catalog_name = databricks_schema.person.catalog_name
  schema_name  = databricks_schema.person.name
  name         = "address"
  table_type   = "MANAGED"
  comment      = "Street address information for customers, employees, and vendors."

  column {
    name = "AddressID"
    type = "decimal(38,0)"
  }

  column {
    name = "addressline1"
    type = "string"
    // TODO masking_policy = snowflake_masking_policy.pserson_pii.qualified_name
  }

  column {
    name = "addressline2"
    type = "string"
    // TODO masking_policy = snowflake_masking_policy.pserson_pii.qualified_name
  }

  column {
    name = "city"
    type = "string"
    // TODO masking_policy = snowflake_masking_policy.pserson_pii.qualified_name
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "postalcode"
    type = "string"
    // TODO masking_policy = snowflake_masking_policy.pserson_pii.qualified_name
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }

  column {
    name = "SpatialLocation"
    type = "string"
  }

  column {
    name = "StateProvinceID"
    type = "decimal(38,0)"
  }
}

// -- -- Table ADDRESSTYPE
resource "databricks_sql_table" "address_type" {
  catalog_name = databricks_schema.person.catalog_name
  schema_name  = databricks_schema.person.name
  name         = "addresstype"
  table_type   = "MANAGED"
  comment      = "Types of addresses stored in the Address table."

  column {
    name = "AddressTypeID"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "Name"
    type = "string"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }
}

// -- -- Table BUSINESSENTITY
resource "databricks_sql_table" "business_entity" {
  catalog_name = databricks_schema.person.catalog_name
  schema_name  = databricks_schema.person.name
  name         = "businessentity"
  table_type   = "MANAGED"
  comment      = "Source of the ID that connects vendors, customers, and employees with address and contact information."

  column {
    name = "BusinessEntityID"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }
}

// -- -- Table BUSINESSENTITYADDRESS
resource "databricks_sql_table" "business_entity_address" {
  catalog_name = databricks_schema.person.catalog_name
  schema_name  = databricks_schema.person.name
  name         = "businessentityaddress"
  table_type   = "MANAGED"
  comment      = "Cross-reference table mapping customers, vendors, and employees to their addresses."

  column {
    name = "AddressID"
    type = "decimal(38,0)"
  }

  column {
    name = "AddressTypeID"
    type = "decimal(38,0)"
  }

  column {
    name = "BusinessEntityID"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }
}

// -- -- Table BUSINESSENTITYCONTACT
resource "databricks_sql_table" "business_entity_contact" {
  catalog_name = databricks_schema.person.catalog_name
  schema_name  = databricks_schema.person.name
  name         = "businessentitycontact"
  table_type   = "MANAGED"
  comment      = "Cross-reference table mapping stores, vendors, and employees to people"

  column {
    name = "BusinessEntityID"
    type = "decimal(38,0)"
  }

  column {
    name = "ContactTypeID"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "PersonID"
    type = "decimal(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }
}

// -- -- Table CONTACTTYPE
resource "databricks_sql_table" "contact_type" {
  catalog_name = databricks_schema.person.catalog_name
  schema_name  = databricks_schema.person.name
  name         = "contacttype"
  table_type   = "MANAGED"
  comment      = "Lookup table containing the types of business entity contacts."

  column {
    name = "ContactTypeID"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "Name"
    type = "string"
  }
}

// -- -- Table COUNTRYREGION
resource "databricks_sql_table" "country_region" {
  catalog_name = databricks_schema.person.catalog_name
  schema_name  = databricks_schema.person.name
  name         = "countryregion"
  table_type   = "MANAGED"
  comment      = "Lookup table containing the ISO standard codes for countries and regions."

  column {
    name = "CountryRegionCode"
    type = "varchar(16)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "Name"
    type = "string"
  }
}

// -- -- Table EMAILADDRESS
resource "databricks_sql_table" "email_address" {
  catalog_name = databricks_schema.person.catalog_name
  schema_name  = databricks_schema.person.name
  name         = "emailaddress"
  table_type   = "MANAGED"
  comment      = "Where to send a person email."

  column {
    name = "BusinessEntityID"
    type = "decimal(38,0)"
  }

  column {
    name = "emailaddress"
    type = "string"
    // TODO masking_policy = snowflake_masking_policy.pserson_pii.qualified_name
  }

  column {
    name = "EmailAddressID"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }
}

// -- -- Table PERSONPHONE
resource "databricks_sql_table" "person_phone" {
  catalog_name = databricks_schema.person.catalog_name
  schema_name  = databricks_schema.person.name
  name         = "personphone"
  table_type   = "MANAGED"
  comment      = "Telephone number and type of a person."

  column {
    name = "BusinessEntityID"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "phonenumber"
    type = "string"
    // TODO MASKING POLICY
    #     masking_policy = snowflake_masking_policy.pserson_pii.qualified_name
  }

  column {
    name = "PhoneNumberTypeID"
    type = "decimal(38,0)"
  }
}

// -- -- Table PHONENUMBERTYPE
resource "databricks_sql_table" "phone_number_type" {
  catalog_name = databricks_schema.person.catalog_name
  schema_name  = databricks_schema.person.name
  name         = "phonenumbertype"
  table_type   = "MANAGED"
  comment      = "Type of phone number of a person."

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "Name"
    type = "string"
  }

  column {
    name = "PhoneNumberTypeID"
    type = "decimal(38,0)"
  }
}

// -- -- Table STATEPROVINCE
resource "databricks_sql_table" "state_province" {
  catalog_name = databricks_schema.person.catalog_name
  schema_name  = databricks_schema.person.name
  name         = "stateprovince"
  table_type   = "MANAGED"
  comment      = "State and province lookup table."

  column {
    name = "CountryRegionCode"
    type = "decimal(38,0)"
  }

  column {
    name = "IsOnlyStateProvinceFlag"
    type = "boolean"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "Name"
    type = "string"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }

  column {
    name = "StateProvinceID"
    type = "decimal(38,0)"
  }

  column {
    name = "TerritoryID"
    type = "decimal(38,0)"
  }
}
