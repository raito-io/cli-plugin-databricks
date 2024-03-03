package databricks

import (
	"github.com/raito-io/cli/base/access_provider"
	ds "github.com/raito-io/cli/base/data_source"

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
			Children:    []string{metastoreType, workspaceType},
		},
		{
			Name: workspaceType,
			Type: workspaceType,
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
			Name: metastoreType,
			Type: metastoreType,
			Permissions: []*ds.DataObjectTypePermission{
				{
					Permission:        "CREATE CATALOG",
					Description:       "Allows a user to create a catalog in a Unity Catalog metastore.",
					GlobalPermissions: ds.AdminGlobalPermission().StringValues(),
				},
				{
					Permission:        "CREATE EXTERNAL LOCATION",
					Description:       "When applied to a storage credential, allows a user to create an external location using the storage credential. This privilege can also be granted to a user on the metastore to allow them to create an external location.",
					GlobalPermissions: ds.AdminGlobalPermission().StringValues(),
				},
				{
					Permission:        "CREATE RECIPIENT",
					Description:       "Allows a user to create a Delta Sharing recipient object in the metastore.",
					GlobalPermissions: ds.AdminGlobalPermission().StringValues(),
				},
				{
					Permission:        "CREATE SHARE",
					Description:       "Allows a user to create a share in the metastore. A share is a logical grouping for the tables you intend to share using Delta Sharing.",
					GlobalPermissions: ds.AdminGlobalPermission().StringValues(),
				},
				{
					Permission:        "CREATE PROVIDER",
					Description:       "Allows a user to create a Delta Sharing provider object in the metastore.",
					GlobalPermissions: ds.AdminGlobalPermission().StringValues(),
				},
				{
					Permission:  "USE PROVIDER",
					Description: "In Delta Sharing, gives a recipient user read-only access to all providers in a recipient metastore and their shares. Combined with the CREATE CATALOG privilege, this privilege allows a recipient user who is not a metastore admin to mount a share as a catalog. This enables you to limit the number of users with the powerful metastore admin role.",
				},
				{
					Permission:  "USE SHARE",
					Description: "In Delta Sharing, gives a provider user read-only access to all shares defined in a provider metastore. This allows a provider user who is not a metastore admin to list shares and list the assets (tables and notebooks) in a share, along with the share’s recipients.",
				},
				{
					Permission:  "USE RECIPIENT",
					Description: "In Delta Sharing, gives a provider user read-only access to all recipients in a provider metastore and their shares. This allows a provider user who is not a metastore admin to view recipient details, recipient authentication status, and the list of shares that the provider has shared with the recipient.",
				},
				{
					Permission:  "SET SHARE PERMISSION",
					Description: "In Delta Sharing, this permission, combined with USE SHARE and USE RECIPIENT (or recipient ownership), gives a provider user the ability to grant a recipient access to a share. Combined with USE SHARE, it gives the ability to transfer ownership of a share to another user, group, or service principal.",
				},
			},
			Children: []string{"catalog"},
		},
		{
			Name: catalogType,
			Type: catalogType,
			Permissions: []*ds.DataObjectTypePermission{
				{
					Permission:        "USE CATALOG",
					Description:       "Required, but not sufficient to reference any objects in a catalog. The principal also needs to have privileges on the individual securable objects.",
					GlobalPermissions: ds.ReadGlobalPermission().StringValues(),
					CannotBeGranted:   true,
				},
				{
					Permission:        "USE SCHEMA",
					Description:       "Required, but not sufficient to reference any objects in a schema. The principal also needs to have privileges on the individual securable objects.",
					GlobalPermissions: ds.ReadGlobalPermission().StringValues(),
					CannotBeGranted:   true,
				},
				{
					Permission:  "EXECUTE",
					Description: "Invoke a user defined function. The user also requires the USE CATALOG privilege on the catalog and the USE SCHEMA privilege on the schema.",
				},
				{
					Permission:             "MODIFY",
					Description:            "COPY INTO, UPDATE DELETE, INSERT, or MERGE INTO the table.",
					GlobalPermissions:      ds.WriteGlobalPermission().StringValues(),
					UsageGlobalPermissions: []string{ds.Write},
				},
				{
					Permission:  "REFRESH",
					Description: "Gives ability to refresh a materialized view",
				},
				{
					Permission:             "SELECT",
					Description:            "Query a table or view, invoke a user defined or anonymous function, or select ANY FILE. The user needs SELECT on the table, view, or function, as well as USE CATALOG on the object’s catalog and USE SCHEMA on the object’s schema.",
					GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
					UsageGlobalPermissions: []string{ds.Read},
				},
				{
					Permission:        "CREATE FUNCTION",
					Description:       "Create a function in a schema. The user also requires the USE CATALOG privilege on the catalog and the USE SCHEMA privilege on the schema.",
					GlobalPermissions: ds.WriteGlobalPermission().StringValues(),
				},
				{
					Permission:  "CREATE MATERIALIZED VIEW",
					Description: "Gives ability to create a materialized view",
				},
				{
					Permission:        "CREATE SCHEMA",
					Description:       "Create a schema in a catalog. The user also requires the USE CATALOG privilege on the catalog.",
					GlobalPermissions: ds.WriteGlobalPermission().StringValues(),
				},
				{
					Permission:        "CREATE TABLE",
					Description:       "Create a table or view in a schema. The user also requires the USE CATALOG privilege on the catalog and the USE SCHEMA privilege on the schema. To create an external table, the user also requires the CREATE EXTERNAL TABLE privilege on the external location and storage credential.",
					GlobalPermissions: ds.WriteGlobalPermission().StringValues(),
				},
			},
			Children: []string{ds.Schema},
		},
		{
			Name: ds.Schema,
			Type: ds.Schema,
			Permissions: []*ds.DataObjectTypePermission{
				{
					Permission:        "USE SCHEMA",
					Description:       "Required, but not sufficient to reference any objects in a schema. The principal also needs to have privileges on the individual securable objects.",
					GlobalPermissions: ds.ReadGlobalPermission().StringValues(),
					CannotBeGranted:   true,
				},
				{
					Permission:  "EXECUTE",
					Description: "Invoke a user defined function. The user also requires the USE CATALOG privilege on the catalog and the USE SCHEMA privilege on the schema.",
				},
				{
					Permission:             "MODIFY",
					Description:            "COPY INTO, UPDATE DELETE, INSERT, or MERGE INTO the table.",
					GlobalPermissions:      ds.WriteGlobalPermission().StringValues(),
					UsageGlobalPermissions: []string{ds.Write},
				},
				{
					Permission:  "REFRESH",
					Description: "Gives ability to refresh a materialized view",
				},
				{
					Permission:             "SELECT",
					Description:            "Query a table or view, invoke a user defined or anonymous function, or select ANY FILE. The user needs SELECT on the table, view, or function, as well as USE CATALOG on the object’s catalog and USE SCHEMA on the object’s schema.",
					GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
					UsageGlobalPermissions: []string{ds.Read},
				},
				{
					Permission:        "CREATE FUNCTION",
					Description:       "Create a function in a schema. The user also requires the USE CATALOG privilege on the catalog and the USE SCHEMA privilege on the schema.",
					GlobalPermissions: ds.WriteGlobalPermission().StringValues(),
				},
				{
					Permission:  "CREATE MATERIALIZED VIEW",
					Description: "Gives ability to create a materialized view",
				},
				{
					Permission:        "CREATE TABLE",
					Description:       "Create a table or view in a schema. The user also requires the USE CATALOG privilege on the catalog and the USE SCHEMA privilege on the schema. To create an external table, the user also requires the CREATE EXTERNAL TABLE privilege on the external location and storage credential.",
					GlobalPermissions: ds.WriteGlobalPermission().StringValues(),
				},
			},
			Children: []string{ds.Table, functionType},
		},
		{
			Name: functionType,
			Type: functionType,
			Permissions: []*ds.DataObjectTypePermission{
				{
					Permission:  "EXECUTE",
					Description: "Invoke a user defined function. The user also requires the USE CATALOG privilege on the catalog and the USE SCHEMA privilege on the schema.",
				},
			},
		},
		{
			Name: ds.Table,
			Type: ds.Table,
			Permissions: []*ds.DataObjectTypePermission{
				{
					Permission:             "SELECT",
					UsageGlobalPermissions: []string{ds.Read},
					Description:            "Query a table or view, invoke a user defined or anonymous function, or select ANY FILE. The user needs SELECT on the table, view, or function, as well as USE CATALOG on the object’s catalog and USE SCHEMA on the object’s schema.",
					GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
				},
				{
					Permission:             "MODIFY",
					Description:            "COPY INTO, UPDATE DELETE, INSERT, or MERGE INTO the table.",
					GlobalPermissions:      ds.WriteGlobalPermission().StringValues(),
					UsageGlobalPermissions: []string{ds.Write},
				},
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
			Name: ds.View,
			Type: ds.View,
			Permissions: []*ds.DataObjectTypePermission{
				{
					Permission:             "SELECT",
					Description:            "Query a table or view, invoke a user defined or anonymous function, or select ANY FILE. The user needs SELECT on the table, view, or function, as well as USE CATALOG on the object’s catalog and USE SCHEMA on the object’s schema.",
					GlobalPermissions:      ds.ReadGlobalPermission().StringValues(),
					UsageGlobalPermissions: []string{ds.Read},
				},
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
				DataObjectTypes: []string{ds.Table, ds.View},
			},
		},
	},
	AccessProviderTypes: []*ds.AccessProviderType{
		{
			Type:                          access_provider.AclSet,
			Label:                         "Permission assignments",
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
	},
}
