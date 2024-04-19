// Catalog - MASTER DATA
resource "databricks_catalog" "master_catalog" {
  name          = "master_catalog_tf_5"
  comment       = "Testing out catalog for TF"
  owner         = var.master_owner_group_name
  force_destroy = true
}

data "databricks_current_user" "me" {

}

# // -- Schema DBO
resource "databricks_schema" "dbo" {
  catalog_name = databricks_catalog.master_catalog.id
  name         = "dbo"
}

// -- -- Table AWSBUILDVERSION
resource "databricks_sql_table" "awsbuildversion" {
  catalog_name = databricks_schema.dbo.catalog_name
  schema_name  = databricks_schema.dbo.name
  table_type   = "MANAGED"
  name         = "awsbuildversion"
  comment      = "Current version number of the AdventureWorks 2014 sample database."

  column {
    name = "DatabaseVersion"
    type = "string"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "SystemInformationID"
    type = "decimal(38,0)"
  }

  column {
    name = "VersionDate"
    type = "timestamp_ntz"
  }
}

// -- -- Table DATABASELOG
resource "databricks_sql_table" "databaselog" {
  catalog_name = databricks_schema.dbo.catalog_name
  schema_name  = databricks_schema.dbo.name
  name         = "databaselog"
  table_type   = "MANAGED"
  comment      = "Audit table tracking all DDL changes made to the AdventureWorks database. Data is captured by the database trigger ddlDatabaseTriggerLog."

  column {
    name = "DatabaseLogID"
    type = "decimal(38,0)"
  }

  column {
    name = "DatabaseUser"
    type = "string"
  }

  column {
    name = "Event"
    type = "string"
  }

  column {
    name = "Object"
    type = "string"
  }

  column {
    name = "PostTime"
    type = "timestamp_ntz"
  }

  column {
    name = "Schema"
    type = "string"
  }

  column {
    name = "TSQL"
    type = "string"
  }

  column {
    name = "XmlEvent"
    type = "string"
  }
}

// -- Schema HUMANRESOURCES
resource "databricks_schema" "humanresources" {
  catalog_name = databricks_catalog.master_catalog.id
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
  catalog_name = databricks_catalog.master_catalog.id
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

// -- Schema PRODUCTION
resource "databricks_schema" "production" {
  catalog_name = databricks_catalog.master_catalog.id
  name         = "production"
}

// -- -- Table BILLOFMATERIALS
resource "databricks_sql_table" "bill_of_materials" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "bill_of_materials"
  table_type   = "MANAGED"
  comment      = "Items required to make bicycles and bicycle subassemblies. It identifies the heirarchical relationship between a parent product and its components."

  column {
    name = "BillOfMaterialsID"
    type = "decimal(38,0)"
  }

  column {
    name = "BOMLevel"
    type = "decimal(38,0)"
  }

  column {
    name = "ComponentID"
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
    name = "PerAssemblyQty"
    type = "decimal(38,0)"
  }

  column {
    name = "ProductAssemblyID"
    type = "decimal(38,0)"
  }

  column {
    name = "StartDate"
    type = "date"
  }

  column {
    name = "UnitMeasureCode"
    type = "varchar(16)"
  }
}

// -- -- Table CULTURE
resource "databricks_sql_table" "culture" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "culture"
  table_type   = "MANAGED"
  comment      = "Lookup table containing the languages in which some AdventureWorks data is stored."

  column {
    name = "CultureID"
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

// -- -- Table DOCUMENT
resource "databricks_sql_table" "document" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "document"
  table_type   = "MANAGED"
  comment      = "Product maintenance documents."

  column {
    name = "ChangeNumber"
    type = "decimal(38,0)"
  }

  column {
    name = "Document"
    type = "string"
  }

  column {
    name = "DocumentLevel"
    type = "string"
  }

  column {
    name = "DocumentNode"
    type = "string"
  }

  column {
    name = "DocumentSummary"
    type = "string"
  }

  column {
    name = "FileExtension"
    type = "string"
  }

  column {
    name = "FileName"
    type = "string"
  }

  column {
    name = "FolderFlag"
    type = "string"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "Owner"
    type = "string"
  }

  column {
    name = "Revision"
    type = "string"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }

  column {
    name = "Status"
    type = "string"
  }

  column {
    name = "Title"
    type = "varchar(32)"
  }
}

// -- -- Table ILLUSTRATION
resource "databricks_sql_table" "illustration" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "illustration"
  table_type   = "MANAGED"
  comment      = "Bicycle assembly diagrams."

  column {
    name = "Diagram"
    type = "string"
  }

  column {
    name = "IllustrationID"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }
}

// -- -- Table LOCATION
resource "databricks_sql_table" "location" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "location"
  table_type   = "MANAGED"
  comment      = "Product inventory and manufacturing locations."

  column {
    name = "Availability"
    type = "string"
  }

  column {
    name = "CostRate"
    type = "decimal(38,0)"
  }

  column {
    name = "LocationID"
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

// -- -- Table PRODUCT
resource "databricks_sql_table" "product" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "product"
  table_type   = "MANAGED"
  comment      = "Products sold or used in the manfacturing of sold products."

  column {
    name = "Class"
    type = "string"
  }

  column {
    name = "Color"
    type = "string"
  }

  column {
    name = "DaysToManufacture"
    type = "decimal(38,0)"
  }

  column {
    name = "DiscontinuedDate"
    type = "date"
  }

  column {
    name = "FinishedGoodsFlag"
    type = "boolean"
  }

  column {
    name = "ListPrice"
    type = "decimal(38,0)"
  }

  column {
    name = "MakeFlag"
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
    name = "ProductID"
    type = "decimal(38,0)"
  }

  column {
    name = "ProductLine"
    type = "decimal(38,0)"
  }

  column {
    name = "ProductModelID"
    type = "decimal(38,0)"
  }

  column {
    name = "ProductNumber"
    type = "decimal(38,0)"
  }

  column {
    name = "ProductSubcategoryID"
    type = "decimal(38,0)"
  }

  column {
    name = "ReorderPoint"
    type = "decimal(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }

  column {
    name = "SafetyStockLevel"
    type = "decimal(38,0)"
  }

  column {
    name = "SellEndDate"
    type = "date"
  }

  column {
    name = "SellStartDate"
    type = "date"
  }

  column {
    name = "Size"
    type = "decimal(38,0)"
  }

  column {
    name = "SizeUnitMeasureCode"
    type = "varchar(16)"
  }

  column {
    name = "StandardCost"
    type = "decimal(38,0)"
  }

  column {
    name = "Style"
    type = "string"
  }

  column {
    name = "Weight"
    type = "decimal(38,0)"
  }

  column {
    name = "WeightUnitMeasureCode"
    type = "varchar(16)"
  }
}

// -- -- Table PRODUCTCATEGORY
resource "databricks_sql_table" "product_category" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "productcategory"
  table_type   = "MANAGED"
  comment      = "High-level product categorization."

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "Name"
    type = "string"
  }

  column {
    name = "ProductCategoryID"
    type = "decimal(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }
}

// -- -- Table PRODUCTCOSTHISTORY
resource "databricks_sql_table" "product_cost_history" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  table_type   = "MANAGED"
  name         = "productcosthistory"

  column {
    name = "EndDate"
    type = "date"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ProductID"
    type = "decimal(38,0)"
  }

  column {
    name = "StandardCost"
    type = "decimal(38,0)"
  }

  column {
    name = "StartDate"
    type = "date"
  }
}

// -- -- Table PRODUCTDESCRIPTION
resource "databricks_sql_table" "product_description" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "productdescription"
  table_type   = "MANAGED"
  comment      = "Product descriptions in several languages."

  column {
    name = "Description"
    type = "string"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ProductDescriptionID"
    type = "decimal(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }
}

// -- -- Table PRODUCTDOCUMENT
resource "databricks_sql_table" "product_document" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "productdocument"
  table_type   = "MANAGED"
  comment      = "Cross-reference table mapping products to related product documents."

  column {
    name = "DocumentNode"
    type = "string"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ProductID"
    type = "decimal(38,0)"
  }
}

// -- -- Table PRODUCTINVENTORY
resource "databricks_sql_table" "product_inventory" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "productinventory"
  table_type   = "MANAGED"
  comment      = "Product inventory information."

  column {
    name = "Bin"
    type = "string"
  }

  column {
    name = "LocationID"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ProductID"
    type = "decimal(38,0)"
  }

  column {
    name = "Quantity"
    type = "decimal(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }

  column {
    name = "Shelf"
    type = "string"
  }
}

// -- -- Table PRODUCTLISTPRICEHISTORY
resource "databricks_sql_table" "product_list_price_history" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "productlistpricehistory"
  table_type   = "MANAGED"
  comment      = "Changes in the list price of a product over time."

  column {
    name = "EndDate"
    type = "date"
  }

  column {
    name = "ListPrice"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ProductID"
    type = "decimal(38,0)"
  }

  column {
    name = "StartDate"
    type = "date"
  }
}

// -- -- Table PRODUCTMODEL
resource "databricks_sql_table" "product_model" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "productmodel"
  table_type   = "MANAGED"
  comment      = "Product model classification."

  column {
    name = "CatalogDescription"
    type = "string"
  }

  column {
    name = "Instructions"
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

  column {
    name = "ProductModelID"
    type = "decimal(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }
}

// -- -- Table PRODUCTMODELILLUS
resource "databricks_sql_table" "product_model_illus" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "productmodelillus"
  table_type   = "MANAGED"
  column {
    name = "IllustrationID"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ProductModelID"
    type = "decimal(38,0)"
  }
}

// -- -- Table PRODUCTMODELPROD
resource "databricks_sql_table" "product_model_prod" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  table_type   = "MANAGED"
  name         = "productmodelprod"
  comment      = "Cross-reference table mapping product descriptions and the language the description is written in."

  column {
    name = "CultureID"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ProductDescriptionID"
    type = "decimal(38,0)"
  }

  column {
    name = "ProductModelID"
    type = "decimal(38,0)"
  }
}

// -- -- Table PRODUCTPHOTO
resource "databricks_sql_table" "product_photo" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "productphoto"
  table_type   = "MANAGED"
  comment      = "Product images."

  column {
    name = "LargePhoto"
    type = "string"
  }

  column {
    name = "LargePhotoFileName"
    type = "string"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ProductPhotoID"
    type = "decimal(38,0)"
  }

  column {
    name = "ThumbNailPhoto"
    type = "string"
  }

  column {
    name = "ThumbnailPhotoFileName"
    type = "string"
  }
}

// -- -- Table PRODUCTPRODUCTPH
resource "databricks_sql_table" "product_product_photo" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "productproductph"
  table_type   = "MANAGED"
  comment      = "Cross-reference table mapping products and product photos."

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "Primary"
    type = "boolean"
  }

  column {
    name = "ProductID"
    type = "decimal(38,0)"
  }

  column {
    name = "ProductPhotoID"
    type = "decimal(38,0)"
  }
}

// -- -- Table PRODUCTREVIEW
resource "databricks_sql_table" "product_review" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "productreview"
  table_type   = "MANAGED"
  comment      = "Customer reviews of products they have purchased."

  column {
    name = "Comments"
    type = "string"
  }

  column {
    name = "EmailAddress"
    type = "string"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ProductID"
    type = "decimal(38,0)"
  }

  column {
    name = "ProductReviewID"
    type = "decimal(38,0)"
  }

  column {
    name = "Rating"
    type = "decimal(4,0)"
  }

  column {
    name = "ReviewDate"
    type = "date"
  }

  column {
    name = "ReviewerName"
    type = "string"
  }
}

// -- -- Table PRODUCTSUBCATEG
resource "databricks_sql_table" "product_sub_category" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "productsubcateg"
  table_type   = "MANAGED"
  comment      = "Product sub-categories."

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "Name"
    type = "string"
  }

  column {
    name = "ProductCategoryID"
    type = "decimal(38,0)"
  }

  column {
    name = "ProductSubcategoryID"
    type = "decimal(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }
}

// -- -- Table SCRAPREASON
resource "databricks_sql_table" "scrap_reason" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "scrapreason"
  table_type   = "MANAGED"
  comment      = "Manufacturing failure reasons lookup table."

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "Name"
    type = "string"
  }

  column {
    name = "ScrapReasonID"
    type = "decimal(38,0)"
  }
}

// -- -- Table TRANSACTIONHISTORY
resource "databricks_sql_table" "transaction_history" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "transactionhistory"
  table_type   = "MANAGED"
  comment      = "Record of each purchase order, sales order, or work order transaction year to date."

  column {
    name = "ActualCost"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ProductID"
    type = "decimal(38,0)"
  }

  column {
    name = "Quantity"
    type = "decimal(38,0)"
  }

  column {
    name = "ReferenceOrderID"
    type = "decimal(38,0)"
  }

  column {
    name = "ReferenceOrderLineID"
    type = "decimal(38,0)"
  }

  column {
    name = "TransactionDate"
    type = "date"
  }

  column {
    name = "TransactionID"
    type = "decimal(38,0)"
  }

  column {
    name = "TransactionType"
    type = "string"
  }
}

// -- -- Table TRANSACTIONHISTORYARCHIVE
resource "databricks_sql_table" "transaction_history_archive" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "transactionhistoryarchive"
  table_type   = "MANAGED"
  comment      = "Transactions for previous years."

  column {
    name = "ActualCost"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ProductID"
    type = "decimal(38,0)"
  }

  column {
    name = "Quantity"
    type = "decimal(38,0)"
  }

  column {
    name = "ReferenceOrderID"
    type = "decimal(38,0)"
  }

  column {
    name = "ReferenceOrderLineID"
    type = "decimal(38,0)"
  }

  column {
    name = "TransactionDate"
    type = "date"
  }

  column {
    name = "TransactionID"
    type = "decimal(38,0)"
  }

  column {
    name = "TransactionType"
    type = "string"
  }
}

// -- -- Table UNITMEASURE
resource "databricks_sql_table" "unit_measure" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "unitmeasure"
  table_type   = "MANAGED"
  comment      = "Unit of measure lookup table."

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "Name"
    type = "string"
  }

  column {
    name = "UnitMeasureCode"
    type = "varchar(16)"
  }
}

// -- -- Table WORKORDER
resource "databricks_sql_table" "work_order" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "workorder"
  table_type   = "MANAGED"
  comment      = "Manufacturing work orders."

  column {
    name = "DueDate"
    type = "date"
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
    name = "OrderQty"
    type = "decimal(38,0)"
  }

  column {
    name = "ProductID"
    type = "decimal(38,0)"
  }

  column {
    name = "ScrappedQty"
    type = "decimal(38,0)"
  }

  column {
    name = "ScrapReasonID"
    type = "decimal(38,0)"
  }

  column {
    name = "StartDate"
    type = "date"
  }

  column {
    name = "StockedQty"
    type = "decimal(38,0)"
  }

  column {
    name = "WorkOrderID"
    type = "decimal(38,0)"
  }
}

// -- -- Table WORKORDERROUTING
resource "databricks_sql_table" "work_order_routing" {
  catalog_name = databricks_schema.production.catalog_name
  schema_name  = databricks_schema.production.name
  name         = "workorderrouting"
  table_type   = "MANAGED"
  comment      = "Work order details."

  column {
    name = "ActualCost"
    type = "decimal(38,0)"
  }

  column {
    name = "ActualEndDate"
    type = "date"
  }

  column {
    name = "ActualResourceHrs"
    type = "decimal(38,0)"
  }

  column {
    name = "ActualStartDate"
    type = "date"
  }

  column {
    name = "LocationID"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "OperationSequence"
    type = "decimal(38,0)"
  }

  column {
    name = "PlannedCost"
    type = "decimal(38,0)"
  }

  column {
    name = "ProductID"
    type = "decimal(38,0)"
  }

  column {
    name = "ScheduledEndDate"
    type = "date"
  }

  column {
    name = "ScheduledStartDate"
    type = "date"
  }

  column {
    name = "WorkOrderID"
    type = "decimal(38,0)"
  }
}

// -- Schema PURCHASING
resource "databricks_schema" "purchasing" {
  catalog_name = databricks_schema.production.catalog_name
  name         = "purchasing"
}

// -- -- Table PRODUCTVENDOR
resource "databricks_sql_table" "product_vendor" {
  catalog_name = databricks_schema.purchasing.catalog_name
  schema_name  = databricks_schema.purchasing.name
  name         = "productvendor"
  table_type   = "MANAGED"
  comment      = "Cross-reference table mapping vendors with the products they supply."

  column {
    name = "AverageLeadTime"
    type = "decimal(38,0)"
  }

  column {
    name = "BusinessEntityID"
    type = "decimal(38,0)"
  }

  column {
    name = "LastReceiptCost"
    type = "decimal(38,0)"
  }

  column {
    name = "LastReceiptDate"
    type = "date"
  }

  column {
    name = "MaxOrderQty"
    type = "decimal(38,0)"
  }

  column {
    name = "MinOrderQty"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "OnOrderQty"
    type = "decimal(38,0)"
  }

  column {
    name = "ProductID"
    type = "decimal(38,0)"
  }

  column {
    name = "StandardPrice"
    type = "decimal(38,0)"
  }

  column {
    name = "UnitMeasureCode"
    type = "varchar(16)"
  }
}

// -- -- Table PURCHASEORDERDET
resource "databricks_sql_table" "purchase_order_det" {
  catalog_name = databricks_schema.purchasing.catalog_name
  schema_name  = databricks_schema.purchasing.name
  name         = "purchaseorderdet"
  table_type   = "MANAGED"
  comment      = "Individual products associated with a specific purchase order. See PurchaseOrderHeader."

  column {
    name = "DueDate"
    type = "date"
  }

  column {
    name = "LineTotal"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "OrderQty"
    type = "decimal(38,0)"
  }

  column {
    name = "ProductID"
    type = "decimal(38,0)"
  }

  column {
    name = "PurchaseOrderDetailID"
    type = "decimal(38,0)"
  }

  column {
    name = "PurchaseOrderID"
    type = "decimal(38,0)"
  }

  column {
    name = "ReceivedQty"
    type = "decimal(38,0)"
  }

  column {
    name = "RejectedQty"
    type = "decimal(38,0)"
  }

  column {
    name = "StockedQty"
    type = "decimal(38,0)"
  }

  column {
    name = "UnitPrice"
    type = "decimal(38,0)"
  }
}

// -- -- Table PURCHASEORDERHEADER
resource "databricks_sql_table" "purchase_order_hea" {
  catalog_name = databricks_schema.purchasing.catalog_name
  schema_name  = databricks_schema.purchasing.name
  name         = "purchaseorderheader"
  table_type   = "MANAGED"
  comment      = "General purchase order information. See PurchaseOrderDetail."

  column {
    name = "EmployeeID"
    type = "decimal(38,0)"
  }

  column {
    name = "Freight"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "OrderDate"
    type = "date"
  }

  column {
    name = "PurchaseOrderID"
    type = "decimal(38,0)"
  }

  column {
    name = "RevisionNumber"
    type = "decimal(38,0)"
  }

  column {
    name = "ShipDate"
    type = "date"
  }

  column {
    name = "ShipMethodID"
    type = "decimal(38,0)"
  }

  column {
    name = "Status"
    type = "string"
  }

  column {
    name = "SubTotal"
    type = "decimal(38,0)"
  }

  column {
    name = "TaxAmt"
    type = "decimal(38,0)"
  }

  column {
    name = "TotalDue"
    type = "decimal(38,0)"
  }

  column {
    name = "VendorID"
    type = "decimal(38,0)"
  }
}

// -- -- Table SHIPMETHOD
resource "databricks_sql_table" "ship_method" {
  catalog_name = databricks_schema.purchasing.catalog_name
  schema_name  = databricks_schema.purchasing.name
  name         = "shipmethod"
  table_type   = "MANAGED"
  comment      = "Shipping company lookup table."

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
    type = "string"
  }

  column {
    name = "ShipBase"
    type = "string"
  }

  column {
    name = "ShipMethodID"
    type = "decimal(38,0)"
  }

  column {
    name = "ShipRate"
    type = "decimal(38,0)"
  }
}

// -- -- Table VENDOR
resource "databricks_sql_table" "vendor" {
  catalog_name = databricks_schema.purchasing.catalog_name
  schema_name  = databricks_schema.purchasing.name
  name         = "vendor"
  table_type   = "MANAGED"
  comment      = "Companies from whom Adventure Works Cycles purchases parts or other goods."

  column {
    name = "AccountNumber"
    type = "string"
  }

  column {
    name = "ActiveFlag"
    type = "boolean"
  }

  column {
    name = "BusinessEntityID"
    type = "decimal(38,0)"
  }

  column {
    name = "CreditRating"
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
    name = "PreferredVendorStatus"
    type = "string"
  }

  column {
    name = "PurchasingWebServiceURL"
    type = "string"
  }
}

// -- Schema SALES
resource "databricks_schema" "sales" {
  catalog_name = databricks_schema.production.catalog_name
  name         = "sales"
}

// -- -- Table COUNTRYREGIONCURRENCY
resource "databricks_sql_table" "country_region_currency" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "countryregioncurrency"
  table_type   = "MANAGED"
  comment      = "Cross-reference table mapping ISO currency codes to a country or region."

  column {
    name = "CountryRegionCode"
    type = "varchar(16)"
  }

  column {
    name = "CurrencyCode"
    type = "varchar(16)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }
}

// -- -- Table CREDITCARD
resource "databricks_sql_table" "credit_card" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "creditcard"
  table_type   = "MANAGED"
  comment      = "Customer credit card information."

  column {
    name = "CardNumber"
    type = "string"
  }

  column {
    name = "CardType"
    type = "string"
  }

  column {
    name = "CreditCardID"
    type = "decimal(38,0)"
  }

  column {
    name = "ExpMonth"
    type = "decimal(12,0)"
  }

  column {
    name = "ExpYear"
    type = "decimal(12,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }
}

// -- -- Table CURRENCY
resource "databricks_sql_table" "currency" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "currency"
  table_type   = "MANAGED"
  comment      = "Lookup table containing standard ISO currencies."

  column {
    name = "CurrencyCode"
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

// -- -- Table CURRENCYRATE
resource "databricks_sql_table" "currency_rate" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "currencyrate"
  table_type   = "MANAGED"
  comment      = "Currency exchange rates."

  column {
    name = "AverageRate"
    type = "decimal(38,0)"
  }

  column {
    name = "CurrencyRateDate"
    type = "date"
  }

  column {
    name = "CurrencyRateID"
    type = "decimal(38,0)"
  }

  column {
    name = "EndOfDayRate"
    type = "decimal(38,0)"
  }

  column {
    name = "FromCurrencyCode"
    type = "varchar(16)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ToCurrencyCode"
    type = "varchar(16)"
  }
}

// -- -- Table CUSTOMER
resource "databricks_sql_table" "customer" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "customer"
  table_type   = "MANAGED"
  comment      = "Current customer information. Also see the Person and Store tables."

  column {
    name = "AccountNumber"
    type = "string"
  }

  column {
    name = "CustomerID"
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

  column {
    name = "StoreID"
    type = "decimal(38,0)"
  }

  column {
    name = "TerritoryID"
    type = "decimal(38,0)"
  }
}

// -- -- View CUSTOMER_EU
resource "databricks_sql_table" "customer_eu" {
  catalog_name    = databricks_schema.sales.catalog_name
  schema_name     = databricks_schema.sales.name
  name            = "customer_eu"
  table_type      = "VIEW"
  view_definition = "SELECT * FROM ${databricks_sql_table.customer.id} WHERE \"TerritoryID\" between 1 and 200"
}

// -- -- Table PERSONCREDITCARD
resource "databricks_sql_table" "person_creditcard" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "personcreditcard"
  table_type   = "MANAGED"
  comment      = "Cross-reference table mapping people to their credit card information in the CreditCard table."

  column {
    name = "BusinessEntityID"
    type = "decimal(38,0)"
  }

  column {
    name = "CreditCardID"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }
}

// -- -- Table SALESORDERDETAIL
resource "databricks_sql_table" "sales_order_detail" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "salesorderdetail"
  table_type   = "MANAGED"
  comment      = "Individual products associated with a specific sales order. See SalesOrderHeader."

  column {
    name = "CarrierTrackingNumber"
    type = "string"
  }

  column {
    name = "LineTotal"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "OrderQty"
    type = "decimal(38,0)"
  }

  column {
    name = "ProductID"
    type = "decimal(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }

  column {
    name = "SalesOrderDetailID"
    type = "decimal(38,0)"
  }

  column {
    name = "SalesOrderID"
    type = "decimal(38,0)"
  }

  column {
    name = "SpecialOfferID"
    type = "decimal(38,0)"
  }

  column {
    name = "UnitPrice"
    type = "decimal(38,0)"
  }

  column {
    name = "UnitPriceDiscount"
    type = "decimal(38,0)"
  }
}

// -- -- Table SALESORDERHEADER
resource "databricks_sql_table" "sales_order_header" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "salesorderheader"
  table_type   = "MANAGED"
  comment      = "General sales order information."

  column {
    name = "AccountNumber"
    type = "string"
  }

  column {
    name = "BillToAddressID"
    type = "decimal(38,0)"
  }

  column {
    name = "Comment"
    type = "string"
  }

  column {
    name = "CreditCardApprovalCode"
    type = "varchar(16)"
  }

  column {
    name = "CreditCardID"
    type = "decimal(38,0)"
  }

  column {
    name = "CurrencyRateID"
    type = "decimal(38,0)"
  }

  column {
    name = "CustomerID"
    type = "decimal(38,0)"
  }

  column {
    name = "DueDate"
    type = "date"
  }

  column {
    name = "Freight"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "OnlineOrderFlag"
    type = "boolean"
  }

  column {
    name = "OrderDate"
    type = "date"
  }

  column {
    name = "PurchaseOrderNumber"
    type = "string"
  }

  column {
    name = "RevisionNumber"
    type = "decimal(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }

  column {
    name = "SalesOrderID"
    type = "decimal(38,0)"
  }

  column {
    name = "SalesOrderNumber"
    type = "string"
  }

  column {
    name = "SalesPersonID"
    type = "decimal(38,0)"
  }

  column {
    name = "ShipDate"
    type = "date"
  }

  column {
    name = "ShipMethodID"
    type = "decimal(38,0)"
  }

  column {
    name = "ShipToAddressID"
    type = "decimal(38,0)"
  }

  column {
    name = "Status"
    type = "string"
  }

  column {
    name = "SubTotal"
    type = "decimal(38,0)"
  }

  column {
    name = "TaxAmt"
    type = "decimal(38,0)"
  }

  column {
    name = "TerritoryID"
    type = "decimal(38,0)"
  }

  column {
    name = "TotalDue"
    type = "decimal(38,0)"
  }
}

// -- -- Table SALESORDERHEADERSALESREASON
resource "databricks_sql_table" "sales_order_header_sales_reason" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "salesorderheadersalesreason"
  table_type   = "MANAGED"
  comment      = "Cross-reference table mapping sales orders to sales reason codes."

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "SalesOrderID"
    type = "decimal(38,0)"
  }

  column {
    name = "SalesReasonID"
    type = "decimal(38,0)"
  }
}

// -- -- Table SALESPERSON
resource "databricks_sql_table" "sales_person" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "salesperson"
  table_type   = "MANAGED"
  comment      = "Sales representative current information."

  column {
    name = "Bonus"
    type = "decimal(38,0)"
  }

  column {
    name = "BusinessEntityID"
    type = "decimal(38,0)"
  }

  column {
    name = "CommissionPct"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "NAME"
    type = "string"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }

  column {
    name = "SalesLastYear"
    type = "decimal(38,0)"
  }

  column {
    name = "SalesQuota"
    type = "decimal(38,0)"
  }

  column {
    name = "SalesYTD"
    type = "decimal(38,0)"
  }

  column {
    name = "TerritoryID"
    type = "decimal(38,0)"
  }
}

// -- -- Table SALESPERSONQUOTAHISTORY
resource "databricks_sql_table" "sales_person_quota_history" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "salespersonquotahistory"
  table_type   = "MANAGED"
  comment      = "Sales performance tracking."

  column {
    name = "BusinessEntityID"
    type = "decimal(38,0)"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "QuotaDate"
    type = "date"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }

  column {
    name = "SalesQuota"
    type = "decimal(38,0)"
  }
}

// -- -- Table SALESREASON
resource "databricks_sql_table" "sales_reason" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "salesreason"
  table_type   = "MANAGED"
  comment      = "Lookup table of customer purchase reasons."

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "Name"
    type = "string"
  }

  column {
    name = "ReasonType"
    type = "string"
  }

  column {
    name = "SalesReasonID"
    type = "decimal(38,0)"
  }
}

// -- -- Table SALESTAXRATE
resource "databricks_sql_table" "sales_tax_rate" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "salestaxrate"
  table_type   = "MANAGED"
  comment      = "Tax rate lookup table."

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
    name = "SalesTaxRateID"
    type = "decimal(38,0)"
  }

  column {
    name = "StateProvinceID"
    type = "decimal(38,0)"
  }

  column {
    name = "TaxRate"
    type = "decimal(38,0)"
  }

  column {
    name = "TaxType"
    type = "string"
  }
}

// -- -- Table SALESTERRITORY
resource "databricks_sql_table" "sales_territory" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "salesterritory"
  table_type   = "MANAGED"
  comment      = "Sales territory lookup table."

  column {
    name = "CostLastYear"
    type = "decimal(38,0)"
  }

  column {
    name = "CostYTD"
    type = "decimal(38,0)"
  }

  column {
    name = "CountryRegionCode"
    type = "varchar(16)"
  }

  column {
    name = "Group"
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

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }

  column {
    name = "SalesLastYear"
    type = "decimal(38,0)"
  }

  column {
    name = "SalesYTD"
    type = "decimal(38,0)"
  }

  column {
    name = "TerritoryID"
    type = "decimal(38,0)"
  }
}

// -- -- Table SALESTERRITORYHISTORY
resource "databricks_sql_table" "sales_territory_history" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "salesterritoryhistory"
  table_type   = "MANAGED"
  comment      = "Sales representative transfers to other sales territories."

  column {
    name = "BusinessEntityID"
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
    name = "ROWGUID"
    type = "decimal(38,0)"
  }

  column {
    name = "StartDate"
    type = "date"
  }

  column {
    name = "TerritoryID"
    type = "decimal(38,0)"
  }
}

// -- -- Table SHOPPINGCARTITEM
resource "databricks_sql_table" "shopping_cart_item" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "shoppingcartitem"
  table_type   = "MANAGED"
  comment      = "Contains online customer orders until the order is submitted or cancelled."

  column {
    name = "DateCreated"
    type = "timestamp_ntz"
  }

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ProductID"
    type = "decimal(38,0)"
  }

  column {
    name = "Quantity"
    type = "decimal(38,0)"
  }


  column {
    name = "ShoppingCartID"
    type = "decimal(38,0)"
  }

  column {
    name = "ShoppingCartItemID"
    type = "decimal(38,0)"
  }
}

// -- -- Table SPECIALOFFER
resource "databricks_sql_table" "special_offer" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "specialoffer"
  table_type   = "MANAGED"
  comment      = "Sale discounts lookup table."

  column {
    name = "Category"
    type = "string"
  }

  column {
    name = "Description"
    type = "string"
  }

  column {
    name = "DiscountPct"
    type = "decimal(38,0)"
  }

  column {
    name = "EndDate"
    type = "date"
  }

  column {
    name = "MaxQty"
    type = "decimal(38,0)"
  }

  column {
    name = "MinQty"
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

  column {
    name = "SpecialOfferID"
    type = "decimal(38,0)"
  }

  column {
    name = "StartDate"
    type = "date"
  }

  column {
    name = "Type"
    type = "string"
  }
}

// -- -- Table SPECIALOFFERPRODUCT
resource "databricks_sql_table" "special_offer_product" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "specialofferproduct"
  table_type   = "MANAGED"
  comment      = "Cross-reference table mapping products to special offer discounts."

  column {
    name = "ModifiedDate"
    type = "timestamp_ntz"
  }

  column {
    name = "ProductID"
    type = "decimal(38,0)"
  }

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }

  column {
    name = "SpecialOfferID"
    type = "decimal(38,0)"
  }
}

// -- -- Table STORE
resource "databricks_sql_table" "store" {
  catalog_name = databricks_schema.sales.catalog_name
  schema_name  = databricks_schema.sales.name
  name         = "store"
  table_type   = "MANAGED"
  comment      = "Customers (resellers) of Adventure Works products."

  column {
    name = "BusinessEntityID"
    type = "decimal(38,0)"
  }

  column {
    name = "Demographics"
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

  column {
    name = "ROWGUID"
    type = "decimal(38,0)"
  }

  column {
    name = "SalesPersonID"
    type = "decimal(38,0)"
  }
}
