package databricks

import (
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/raito-io/cli/base/access_provider"
	ds "github.com/raito-io/cli/base/data_source"

	"cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/databricks/masks"
)

var databricks_metadata = ds.MetaData{
	Type:                  "databricks",
	SupportsApInheritance: false,
	SupportedFeatures:     []string{ds.ColumnMasking, ds.RowFiltering},
	DataObjectTypes: []*ds.DataObjectType{
		{
			// Account
			Name:        ds.Datasource,
			Type:        ds.Datasource,
			Permissions: []*ds.DataObjectTypePermission{},
			Children:    []string{constants.MetastoreType, constants.WorkspaceType},
		},
		{
			Name: constants.WorkspaceType,
			Type: constants.WorkspaceType,
			Permissions: []*ds.DataObjectTypePermission{
				{
					// Defined by Raito
					Permission:  "USER",
					Description: "Assigned to workspace with role USER",
				},
				{
					// Defined by Raito
					Permission:  "ADMIN",
					Description: "Assigned to workspace with role ADMIN",
				},
			},
		},
		{
			Name: constants.MetastoreType,
			Type: constants.MetastoreType,
			Permissions: []*ds.DataObjectTypePermission{
				&CreateCatalogPermission,
				&CreateCleanRoomPermission,
				&CreateConnectionPermission,
				&CreateExternalLocationPermission,
				&CreateProviderPermission,
				&CreateRecipientPermission,
				&CreateSharePermission,
				&CreateStorageCredentialPermission,
				&SetSharePermissionPermission,
				&UseMarketplaceAssetsPermission,
				&UseProviderPermission,
				&UseRecipientPermission,
				&UseSharePermission,
			},
			Children: []string{"catalog"},
		},
		{
			Name: constants.CatalogType,
			Type: constants.CatalogType,
			Permissions: []*ds.DataObjectTypePermission{
				&AllPrivilegesPermission,
				// Permissions on Catalog
				&ApplyTagPermission,
				&BrowsePermission,
				&CreateSchemaPermission,
				&UseCatalogPermission,

				// Permissions on child objects
				&CreateFunctionPermission,
				&CreateTablePermission,
				&CreateMaterializedViewPermission,
				&CreateModelPermission,
				&CreateVolumePermission,
				&ExternalUseSchemaPermission,
				&ReadVolumePermission,
				&RefreshPermission,
				&WriteVolumePermission,
				&ExecutePermission,
				&ModifyPermission,
				&SelectPermission,
				&UseSchemaPermission,
			},
			Children: []string{ds.Schema},
		},
		{
			Name: ds.Schema,
			Type: ds.Schema,
			Permissions: []*ds.DataObjectTypePermission{
				&AllPrivilegesPermission,
				// Permissions on Schema
				&ApplyTagPermission,
				&CreateFunctionPermission,
				&CreateTablePermission,
				&CreateModelPermission,
				&CreateVolumePermission,
				&CreateMaterializedViewPermission,
				&ExternalUseSchemaPermission,
				&UseSchemaPermission,

				// Permissions on child objects
				&ExecutePermission,
				&ModifyPermission,
				&ReadVolumePermission,
				&RefreshPermission,
				&SelectPermission,
				&WriteVolumePermission,
			},
			Children: []string{ds.Table, constants.MaterializedViewType, ds.View, constants.FunctionType},
		},
		{
			Name: constants.FunctionType,
			Type: constants.FunctionType,
			Permissions: []*ds.DataObjectTypePermission{
				&AllPrivilegesPermission,
				// &ApplyTagPermission, // Only for models which is not supported at the moment
				&ExecutePermission,
			},
		},
		{
			Name: ds.Table,
			Type: ds.Table,
			Permissions: []*ds.DataObjectTypePermission{
				&AllPrivilegesPermission,
				&ApplyTagPermission,
				&ModifyPermission,
				&SelectPermission,
			},
			Actions: []*ds.DataObjectTypeAction{
				{
					Action:        "SELECT",
					GlobalActions: []string{ds.Read},
				},
				{
					Action:        "UPDATE",
					GlobalActions: []string{ds.Write},
				},
				{
					Action:        "INSERT",
					GlobalActions: []string{ds.Write},
				},
				{
					Action:        "MERGE",
					GlobalActions: []string{ds.Write},
				},
				{
					Action:        "DELETE",
					GlobalActions: []string{ds.Write},
				},
				{
					Action:        "COPY",
					GlobalActions: []string{ds.Write},
				},
			},
			Children: []string{ds.Column},
		},
		{
			Name:  constants.MaterializedViewType,
			Label: "Materialized View",
			Type:  ds.View,
			Permissions: []*ds.DataObjectTypePermission{
				&AllPrivilegesPermission,
				&ApplyTagPermission,
				&RefreshPermission,
				&SelectPermission,
			},
			Actions: []*ds.DataObjectTypeAction{
				{
					Action:        "SELECT",
					GlobalActions: []string{ds.Read},
				},
				{
					Action:        "REFRESH",
					GlobalActions: []string{ds.Write},
				},
			},
			Children: []string{ds.Column},
		},
		{
			Name: ds.View,
			Type: ds.View,
			Permissions: []*ds.DataObjectTypePermission{
				&AllPrivilegesPermission,
				&ApplyTagPermission,
				&SelectPermission,
			},
			Actions: []*ds.DataObjectTypeAction{
				{
					Action:        "SELECT",
					GlobalActions: []string{ds.Read},
				},
			},
			Children: []string{ds.Column},
		},

		{
			Name:        ds.Column,
			Type:        ds.Column,
			Permissions: []*ds.DataObjectTypePermission{},
			Actions:     []*ds.DataObjectTypeAction{},
			Children:    nil,
		},
	},
	UsageMetaInfo: &ds.UsageMetaInput{
		DefaultLevel: ds.Table,
		Levels: []*ds.UsageMetaInputDetail{
			{
				Name:            ds.Table,
				DataObjectTypes: []string{ds.Table, ds.View, constants.MaterializedViewType},
			},
		},
	},
	AccessProviderTypes: []*ds.AccessProviderType{
		{
			Type:                          access_provider.AclSet,
			Label:                         "Permission Assignment",
			IsNamedEntity:                 false,
			CanBeCreated:                  true,
			CanBeAssumed:                  false,
			CanAssumeMultiple:             false,
			AllowedWhoAccessProviderTypes: []string{access_provider.AclSet},
		},
	},
	MaskingMetadata: &ds.MaskingMetadata{
		MaskTypes: []*ds.MaskingType{
			{
				DisplayName: "Default mask",
				ExternalId:  masks.DefaultMaskId,
				Description: "Replace the data with a default value.",
			},
			{
				DisplayName: "Hash (sha256)",
				ExternalId:  masks.SHA256MaskId,
				Description: "Replace the data with a hash of the data.",
				DataTypes: []string{
					masks.DataTypeString.String(),
					masks.DataTypeBinary.String(),
				},
			},
		},
		DefaultMaskExternalName: masks.DefaultMaskId,
		ApplicableTypes:         []string{ds.Table, ds.View, constants.MaterializedViewType},
	},
	FilterMetadata: &ds.FilterMetadata{
		ApplicableTypes: []string{ds.Table, ds.View, constants.MaterializedViewType},
	},
}

// Permissions
// AllPrivilegesPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#all-privileges
var AllPrivilegesPermission = ds.DataObjectTypePermission{
	Permission:      "ALL PRIVILEGES",
	Description:     "Used to grant or revoke all privileges applicable to the securable object and its child objects without explicitly specifying them.",
	CannotBeGranted: false,
}

// ApplyTagPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#apply-tag
var ApplyTagPermission = ds.DataObjectTypePermission{
	Permission:             "APPLY TAG",
	Description:            "Allows a user to add and edit tags on an object. Granting APPLY TAG to a table or view also enables column tagging. Granting APPLY TAG to a registered model also enables model version tagging.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// BrowsePermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#browse
var BrowsePermission = ds.DataObjectTypePermission{
	Permission:             "BROWSE",
	GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
	Description:            "Allows a user to view an object’s metadata using Catalog Explorer, the schema browser, search results, the lineage graph, information_schema, and the REST API.",
	UsageGlobalPermissions: []string{ds.Read},
	CannotBeGranted:        false,
}

// CreateCatalogPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-catalog
var CreateCatalogPermission = ds.DataObjectTypePermission{
	Permission:             "CREATE CATALOG",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create a catalog in a Unity Catalog metastore. To create a foreign catalog, you must also have the CREATE FOREIGN CATALOG privilege on the connection that contains the foreign catalog or on the metastore.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateCleanRoomPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-clean-room
var CreateCleanRoomPermission = ds.DataObjectTypePermission{
	Permission:             "CREATE CLEAN ROOM",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create a clean room for securely collaborating on projects with other organizations without sharing underlying data.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateConnectionPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-connection
var CreateConnectionPermission = ds.DataObjectTypePermission{
	Permission:             "CREATE CONNECTION",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create a connection to an external database in a Lakehouse Federation scenario.\n\n",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateExternalLocationPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-external-location
var CreateExternalLocationPermission = ds.DataObjectTypePermission{
	Permission:             "CREATE EXTERNAL LOCATION",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "To create an external location, the user must have this privilege on both the metastore and the storage credential that is being referenced in the external location.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateExternalTablePermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-external-table
var CreateExternalTablePermission = ds.DataObjectTypePermission{
	Permission:             "CREATE EXTERNAL TABLE",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create external tables directly in your cloud tenant using an external location or storage credential. Databricks recommends granting this privilege on an external location rather than storage credential (since it’s scoped to a path, it allows more control over where users can create external tables in your cloud tenant).",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateExternalVolumePermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-external-volume
var CreateExternalVolumePermission = ds.DataObjectTypePermission{
	Permission:             "CREATE EXTERNAL VOLUME",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create external volumes using an external location.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateForeignCatalogPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-foreign-catalog
var CreateForeignCatalogPermission = ds.DataObjectTypePermission{
	Permission:             "CREATE FOREIGN CATALOG",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create a function in the schema. Since privileges are inherited, CREATE FUNCTION can also be granted on a catalog, which allows a user to create a function in any existing or future schema in the catalog.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateFunctionPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-function
var CreateFunctionPermission = ds.DataObjectTypePermission{
	Permission:             "CREATE FUNCTION",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create a function in the schema. Since privileges are inherited, CREATE FUNCTION can also be granted on a catalog, which allows a user to create a function in any existing or future schema in the catalog.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateModelPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-model
var CreateModelPermission = ds.DataObjectTypePermission{
	Permission:             "CREATE MODEL",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create an MLflow registered model (which is a type of FUNCTION) in the schema. Since privileges are inherited, CREATE MODEL can also be granted on a catalog, which allows a user to create a registered model in any existing or future schema in the catalog.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateManagedStoragePermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-managed-storage
var CreateManagedStoragePermission = ds.DataObjectTypePermission{
	Permission:             "CREATE MANAGED STORAGE",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to specify a location for storing managed tables at the catalog or schema level, overriding the default root storage for the metastore.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateSchemaPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-schema
var CreateSchemaPermission = ds.DataObjectTypePermission{
	Permission:             "CREATE SCHEMA",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create a schema in a catalog. The user also requires the USE CATALOG privilege on the catalog.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateStorageCredentialPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-storage-credential
var CreateStorageCredentialPermission = ds.DataObjectTypePermission{
	Permission:             "CREATE STORAGE CREDENTIAL",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create a storage credential in a Unity Catalog metastore.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateTablePermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-table
var CreateTablePermission = ds.DataObjectTypePermission{
	Permission:             "CREATE TABLE",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create a table or view in the schema. Since privileges are inherited, CREATE TABLE can also be granted on a catalog, which allows a user to create a table or view in any existing or future schema in the catalog.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateMaterializedViewPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-materialized-view
var CreateMaterializedViewPermission = ds.DataObjectTypePermission{
	Permission:             "CREATE MATERIALIZED VIEW",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create a materialized view in the schema. Since privileges are inherited, CREATE MATERIALIZED VIEW can also be granted on a catalog, which allows a user to create a table or view in any existing or future schema in the catalog.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateVolumePermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-volume
var CreateVolumePermission = ds.DataObjectTypePermission{
	Permission:             "CREATE VOLUME",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create a volume in the schema. Since privileges are inherited, CREATE VOLUME can also be granted on a catalog, which allows a user to create a volume in any existing or future schema in the catalog.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// ExecutePermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#execute
var ExecutePermission = ds.DataObjectTypePermission{
	Permission:             "EXECUTE",
	GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
	Description:            "Allows a user to invoke a user defined function or load a model for inference, if the user also has USE CATALOG on its parent catalog and USE SCHEMA on its parent schema. For functions, EXECUTE grants the ability to view the function definition and metadata. For registered models, EXECUTE grants the ability to view metadata for all versions of the registered model, and to download model files.",
	UsageGlobalPermissions: []string{ds.Read},
	CannotBeGranted:        false,
}

// ExecuteCleanRoomTaskPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#execute-clean-room-task
var ExecuteCleanRoomTaskPermission = ds.DataObjectTypePermission{
	Permission:             "EXECUTE CLEAN ROOM TASK",
	GlobalPermissions:      ds.WriteGlobalPermission().StringValues(),
	Description:            "Allows a user to run tasks (notebooks) in a clean room. Also enables the user to view clean room details.\n\n",
	UsageGlobalPermissions: []string{ds.Write},
	CannotBeGranted:        false,
}

// ExternalUseSchemaPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#external-use-schema
var ExternalUseSchemaPermission = ds.DataObjectTypePermission{
	Permission:             "EXTERNAL USE SCHEMA",
	GlobalPermissions:      nil,
	Description:            "Allows a user to be granted a temporary credential to access Unity Catalog tables from an external processing engine using the Unity Catalog open APIs or Iceberg REST APIs. Only the catalog owner can grant this privilege.",
	UsageGlobalPermissions: nil,
	CannotBeGranted:        true,
}

// ManageAllowlistPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#manage-allowlist
var ManageAllowlistPermission = ds.DataObjectTypePermission{
	Permission:             "MANAGE ALLOWLIST",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to add or modify paths for init scripts, JARs, and Maven coordinates in the allowlist that governs Unity Catalog-enabled clusters with shared access mode.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// ModifyPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#modify
var ModifyPermission = ds.DataObjectTypePermission{
	Permission:             "MODIFY",
	GlobalPermissions:      ds.WriteGlobalPermission().StringValues(),
	Description:            "Allows a user to add, update, and delete data to or from the table if the user also has SELECT on the table as well as USE CATALOG on its parent catalog and USE SCHEMA on its parent schema.",
	UsageGlobalPermissions: []string{ds.Write},
	CannotBeGranted:        false,
}

// ModifyCleanRoomPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#modify-clean-room
var ModifyCleanRoomPermission = ds.DataObjectTypePermission{
	Permission:             "MODIFY CLEAN ROOM",
	GlobalPermissions:      ds.WriteGlobalPermission().StringValues(),
	Description:            "Allows a user to update a clean room, including adding and removing data assets, adding and removing notebooks, and updating comments. Also enables the user to view clean room details.",
	UsageGlobalPermissions: []string{ds.Write},
	CannotBeGranted:        false,
}

// ReadFilePermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#read-file
var ReadFilePermission = ds.DataObjectTypePermission{
	Permission:             "READ FILE",
	GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
	Description:            "Allows a user to read files directly from your cloud object storage. Databricks recommends granting this privilege on volumes and granting on external locations for limited use cases.",
	UsageGlobalPermissions: []string{ds.Read},
	CannotBeGranted:        false,
}

// ReadVolumePermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#read-volume
var ReadVolumePermission = ds.DataObjectTypePermission{
	Permission:             "READ VOLUME",
	GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
	Description:            "Allows a user to read files and directories stored inside a volume if the user also has USE CATALOG on its parent catalog and USE SCHEMA on its parent schema.",
	UsageGlobalPermissions: []string{ds.Read},
	CannotBeGranted:        false,
}

// RefreshPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#refresh
var RefreshPermission = ds.DataObjectTypePermission{
	Permission:             "REFRESH",
	GlobalPermissions:      ds.WriteGlobalPermission().StringValues(),
	Description:            "Allows a user to refresh a materialized view if the user also has USE CATALOG on its parent catalog and USE SCHEMA on its parent schema.",
	UsageGlobalPermissions: []string{ds.Write},
	CannotBeGranted:        false,
}

// SelectPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#select
var SelectPermission = ds.DataObjectTypePermission{
	Permission:             "SELECT",
	GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
	Description:            "If applied to a table or view, allows a user to select from the table or view, if the user also has USE CATALOG on its parent catalog and USE SCHEMA on its parent schema. If applied to a share, allows a recipient to select from the share.",
	UsageGlobalPermissions: []string{ds.Read},
	CannotBeGranted:        false,
}

// UseCatalogPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#use-catalog
var UseCatalogPermission = ds.DataObjectTypePermission{
	Permission:        "USE CATALOG",
	GlobalPermissions: ds.ReadGlobalPermission().StringValues(),
	Description:       "This privilege does not grant access to the catalog itself, but is needed for a user to interact with any object within the catalog.",
	CannotBeGranted:   true,
}

// UseConnectionPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#use-connection
var UseConnectionPermission = ds.DataObjectTypePermission{
	Permission:        "USE CONNECTION",
	GlobalPermissions: ds.ReadGlobalPermission().StringValues(),
	Description:       "Allows a user to list and view details about connections to an external database in a Lakehouse Federation scenario. To create foreign catalogs for a connection, you must have CREATE FOREIGN CATALOG on the connection or ownership of the connection.",
	CannotBeGranted:   false,
}

// UseSchemaPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#use-schema
var UseSchemaPermission = ds.DataObjectTypePermission{
	Permission:        "USE SCHEMA",
	GlobalPermissions: ds.ReadGlobalPermission().StringValues(),
	Description:       "This privilege does not grant access to the schema itself, but is needed for a user to interact with any object within the schema.",
	CannotBeGranted:   true,
}

// WriteFilesPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#write-files
var WriteFilesPermission = ds.DataObjectTypePermission{
	Permission:             "WRITE FILES",
	GlobalPermissions:      ds.WriteGlobalPermission().StringValues(),
	Description:            "Allows a user to write files directly into your cloud object storage. Databricks recommends granting this privilege on volumes.",
	UsageGlobalPermissions: []string{ds.Write},
	CannotBeGranted:        false,
}

// WriteVolumePermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#write-volume
var WriteVolumePermission = ds.DataObjectTypePermission{
	Permission:             "WRITE VOLUME",
	GlobalPermissions:      ds.WriteGlobalPermission().StringValues(),
	Description:            "Allows a user to write files and directories to a volume if the user also has USE CATALOG on its parent catalog and USE SCHEMA on its parent schema.",
	UsageGlobalPermissions: []string{ds.Write},
	CannotBeGranted:        false,
}

// CreateProviderPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-provider
var CreateProviderPermission = ds.DataObjectTypePermission{
	Permission:             "CREATE PROVIDER",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create a Delta Sharing provider object in the metastore. A provider identifies an organization or group of users that have shared data using Delta Sharing. Provider creation is performed by a user in the recipient’s Databricks account.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateRecipientPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-recipient
var CreateRecipientPermission = ds.DataObjectTypePermission{
	Permission:             "CREATE RECIPIENT",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create a Delta Sharing recipient object in the metastore. A recipient identifies an organization or group of users that can have data shared with them using Delta Sharing. Recipient creation is performed by a user in the provider’s Databricks account.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// CreateSharePermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#create-share
var CreateSharePermission = ds.DataObjectTypePermission{
	Permission:             "CREATE SHARE",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "Allows a user to create a share in the metastore. A share is a logical grouping for the tables you intend to share using Delta Sharing/",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// SetSharePermissionPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#set-share-permission
var SetSharePermissionPermission = ds.DataObjectTypePermission{
	Permission:             "SET SHARE PERMISSION",
	GlobalPermissions:      ds.AdminGlobalPermission().StringValues(),
	Description:            "In Delta Sharing, this privilege, combined with USE SHARE and USE RECIPIENT (or recipient ownership), gives a provider user the ability to grant a recipient access to a share. Combined with USE SHARE, it gives the ability to transfer ownership of a share to another user, group, or service principal.",
	UsageGlobalPermissions: []string{ds.Admin},
	CannotBeGranted:        false,
}

// UseMarketplaceAssetsPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#use-marketplace-assets
var UseMarketplaceAssetsPermission = ds.DataObjectTypePermission{
	Permission:      "USE MARKETPLACE ASSETS",
	Description:     "In Databricks Marketplace, this privilege gives a user the ability to get instant access or request access for data products shared in a Marketplace listing. It also allows a user to access the read-only catalog that is created when a provider shares a data product. Without this privilege, the user would require the CREATE CATALOG and USE PROVIDER privileges or the metastore admin role. This enables you to limit the number of users with those powerful permissions.",
	CannotBeGranted: false,
}

// UseProviderPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#use-provider
var UseProviderPermission = ds.DataObjectTypePermission{
	Permission:      "USE PROVIDER",
	Description:     "In Delta Sharing, gives a recipient user read-only access to all providers in a recipient metastore and their shares. Combined with the CREATE CATALOG privilege, this privilege allows a recipient user who is not a metastore admin to mount a share as a catalog. This enables you to limit the number of users with the powerful metastore admin role.",
	CannotBeGranted: false,
}

// UseRecipientPermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#use-recipient
var UseRecipientPermission = ds.DataObjectTypePermission{
	Permission:      "USE RECIPIENT",
	Description:     "In Delta Sharing, gives a provider user read-only access to all recipients in a provider metastore and their shares. This allows a provider user who is not a metastore admin to view recipient details, recipient authentication status, and the list of shares that the provider has shared with the recipient.",
	CannotBeGranted: false,
}

// UseSharePermission as defined on https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#use-share
var UseSharePermission = ds.DataObjectTypePermission{
	Permission:      "USE SHARE",
	Description:     "In Delta Sharing, gives a provider user read-only access to all shares defined in a provider metastore. This allows a provider user who is not a metastore admin to list shares and list the assets (tables and notebooks) in a share, along with the share’s recipients.",
	CannotBeGranted: false,
}

var TableTypeMap = map[catalog.TableType]string{
	catalog.TableTypeExternal: ds.Table,
	// catalog.TableTypeExternalShallowClone: "", //NOT SUPPORTED YET
	// catalog.TableTypeForeign: "", // NOT SUPPORTED YET
	catalog.TableTypeManaged: ds.Table,
	// catalog.TableTypeManagedShallowClone: "", // NOT SUPPORTED YET
	catalog.TableTypeMaterializedView: constants.MaterializedViewType,
	catalog.TableTypeStreamingTable:   ds.Table,
	catalog.TableTypeView:             ds.View,
}
