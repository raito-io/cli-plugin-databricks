package repo

import "github.com/databricks/databricks-sdk-go/service/catalog"

type RepositoryCredentials struct {
	Username string
	Password string

	ClientId     string
	ClientSecret string
}

type MetastoreAssignment struct {
	WorkspaceIds []int `json:"workspace_ids,omitempty"`
}

type Workspace struct {
	WorkspaceId     int    `json:"workspace_id"`
	WorkspaceName   string `json:"workspace_name"`
	WorkspaceStatus string `json:"workspace_status"`
	DeploymentName  string `json:"deployment_name"`
}

type DatabricksUsersFilter struct {
	Username *string
}

type DatabricksGroupsFilter struct {
	Groupname *string
}

// FunctionInfo Temporarily defined until bug in SDK is fixed
type FunctionInfo struct {
	// Name of parent catalog.
	CatalogName string `json:"catalog_name,omitempty"`
	// User-provided free-form text description.
	Comment string `json:"comment,omitempty"`
	// Time at which this function was created, in epoch milliseconds.
	CreatedAt int64 `json:"created_at,omitempty"`
	// Username of function creator.
	CreatedBy string `json:"created_by,omitempty"`
	// Scalar function return data type.
	DataType catalog.ColumnTypeName `json:"data_type,omitempty"`
	// External function language.
	ExternalLanguage string `json:"external_language,omitempty"`
	// External function name.
	ExternalName string `json:"external_name,omitempty"`
	// Pretty printed function data type.
	FullDataType string `json:"full_data_type,omitempty"`
	// Full name of function, in form of
	// __catalog_name__.__schema_name__.__function__name__
	FullName string `json:"full_name,omitempty"`
	// Id of Function, relative to parent schema.
	FunctionId string `json:"function_id,omitempty"`
	// The array of __FunctionParameterInfo__ definitions of the function's
	// parameters.
	InputParams []catalog.FunctionParameterInfo `json:"input_params,omitempty"`
	// Whether the function is deterministic.
	IsDeterministic bool `json:"is_deterministic,omitempty"`
	// Function null call.
	IsNullCall bool `json:"is_null_call,omitempty"`
	// Unique identifier of parent metastore.
	MetastoreId string `json:"metastore_id,omitempty"`
	// Name of function, relative to parent schema.
	Name string `json:"name,omitempty"`
	// Username of current owner of function.
	Owner string `json:"owner,omitempty"`
	// Function parameter style. **S** is the value for SQL.
	ParameterStyle catalog.FunctionInfoParameterStyle `json:"parameter_style,omitempty"`
	// A map of key-value properties attached to the securable.
	// INVALID Properties map[string]string `json:"properties,omitempty"`
	// Table function return parameters.
	ReturnParams []catalog.FunctionParameterInfo `json:"return_params,omitempty"`
	// Function language. When **EXTERNAL** is used, the language of the routine
	// function should be specified in the __external_language__ field, and the
	// __return_params__ of the function cannot be used (as **TABLE** return
	// type is not supported), and the __sql_data_access__ field must be
	// **NO_SQL**.
	RoutineBody catalog.FunctionInfoRoutineBody `json:"routine_body,omitempty"`
	// Function body.
	RoutineDefinition string `json:"routine_definition,omitempty"`
	// Function dependencies.
	RoutineDependencies []catalog.Dependency `json:"routine_dependencies,omitempty"`
	// Name of parent schema relative to its parent catalog.
	SchemaName string `json:"schema_name,omitempty"`
	// Function security type.
	SecurityType catalog.FunctionInfoSecurityType `json:"security_type,omitempty"`
	// Specific name of the function; Reserved for future use.
	SpecificName string `json:"specific_name,omitempty"`
	// Function SQL data access.
	SqlDataAccess catalog.FunctionInfoSqlDataAccess `json:"sql_data_access,omitempty"`
	// List of schemes whose objects can be referenced without qualification.
	SqlPath string `json:"sql_path,omitempty"`
	// Time at which this function was created, in epoch milliseconds.
	UpdatedAt int64 `json:"updated_at,omitempty"`
	// Username of user who last modified function.
	UpdatedBy string `json:"updated_by,omitempty"`

	ForceSendFields []string `json:"-"`
}

type FunctionsInfo struct {
	Functions []FunctionInfo `json:"functions,omitempty"`
}

type ColumnInformation struct {
	Name string
	Type string
	Mask *string
}
