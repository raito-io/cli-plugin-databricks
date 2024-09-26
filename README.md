<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/raito-io/raito-io.github.io/raw/master/assets/images/logo-vertical-dark%402x.png">
    <img height="250px" src="https://github.com/raito-io/raito-io.github.io/raw/master/assets/images/logo-vertical%402x.png">
  </picture>
</h1>

<h4 align="center">
  Databricks Unity Catalog plugin for the Raito CLI
</h4>

<p align="center">
    <a href="/LICENSE.md" target="_blank"><img src="https://img.shields.io/badge/license-Apache%202-brightgreen.svg" alt="Software License" /></a>
<a href="https://github.com/raito-io/cli-plugin-snowflake/actions/workflows/build.yml" target="_blank"><img src="https://img.shields.io/github/actions/workflow/status/raito-io/cli-plugin-databricks/build.yml?branch=main" alt="Build status" /></a>
    <a href="https://codecov.io/gh/raito-io/cli-plugin-databricks" target="_blank"><img src="https://img.shields.io/codecov/c/github/raito-io/cli-plugin-databricks" alt="Code Coverage" /></a>
</p>

<hr/>

# Raito CLI Plugin - Databricks Unity Catalog

**Note: This repository is still in an early stage of development.
At this point, no contributions are accepted to the project yet.**

This Raito CLI plugin implements the integration with Databricks. It can
 - Synchronize the users, groups and service principals in a databricks account to an identity store in Raito Cloud.
 - Synchronize the Databricks Unity Catalog meta data (data structure, known permissions, ...) to a data source in Raito Cloud.
 - Synchronize the access providers from Raito Cloud into Databricks Unity Catalog grants.
 - Synchronize the data usage information to Raito Cloud.

## Prerequisites
To use this plugin, you will need

1. The Raito CLI to be correctly installed. You can check out our [documentation](http://docs.raito.io/docs/cli/installation) for help on this.
2. A Raito Cloud account to synchronize your Databricks account with. If you don't have this yet, visit our webpage at (https://raito.io) and request a trial account.
3. An admin user on your databricks account with admin access on all workspaces or, a service principle that is owner of all databricks metastores and assigned as admin to the account and worspaces
4. Within the databricks account and all workspaces, Unity Catalog should be enabled

A full example on how to start using Raito Cloud can be found as a [guide in our documentation](http://docs.raito.io/docs/guide/cloud).

## Usage
To use the plugin, add the following snippet to your Raito CLI configuration file (`raito.yml`, by default) under the `targets` section:

```yaml
 - name: databricks
   connector-name: raito-io/cli-plugin-databricks
   data-source-id: <<Databricks datasource ID>>   
   identity-store-id: <<Databricks identitystore ID>>

   databricks-account-id: <<Databricks account ID>>
   databricks-platform: <<Databricks platform>>
   
   # SQL Warehouses (required for tag support, row-level filtering, and column masking)
   databricks-sql-warehouses:
     - workspace: <<databricks workspace ID>>
       warehouse: <<databricks SQL Warehouse ID>>
     - workspace: <<databricks workspace ID>>
       warehouse: <<databricks SQL Warehouse ID>>
   
   # Native authentication
   databricks-client-id: <<Databricks client ID>>
   databricks-client-secret: <<Databricks client secret>>
   databricks-user: <<Databricks user email address>>
   databricks-password: <<Databricks user password>>
   databricks-token: <<Databricks Personal Access Token>>
   
   # Azure authentication
   databricks-azure-resource-id: <<Azure resource ID>>
   databricks-azure-use-msi: <<Azure use MSI>>
   databricks-azure-client-id: <<Azure client ID>>
   databricks-azure-client-secret: <<Azure client secret>>
   databricks-azure-tenant-id: <<Azure tenant ID>>
   databricks-azure-environment: <<Azure environment>>
   
   # GCP authentication
   databricks-google-credentials: <<GCP credential file>>
   databricks-google-service-account: <<GCP service account>>

```

Next, replace the values of the indicated fields with your specific values:
- `<<Databricks datasource ID>>`: the ID of the Data source you created in the Raito Cloud UI.
- `<<Databricks identitystore ID>>`: the ID of the Identity Store you created in the Raito Cloud UI.
- `<<Databricks account ID>>`: the Databricks account ID`
- `<<Databricks platform>>`: The databricks platform that is used. (supported platforms: AWS/GCP/Azure)
- `<<Databricks client ID>>`: if using oauth, the Databricks client ID of an account with admin access to all workspaces
- `<<Databricks client secret>>`: if using oauth, the Databricks client secret of the account specified in `databricks-client-id`
- `<<Databricks user email address>>`: if using basic auth, the email address of an admin user in your databricks account with admin access to all workspaces
- `<<Databricks user password>>`: if using basic auth, the email password of the `databricks-user` user
- `<<Databricks Personal Access Token>>`: authentication by using a personal access token. The Databricks personal access token (PAT) (AWS, Azure, and GCP) or Azure Active Directory (Azure AD) token (Azure).
- `<<Azure resource ID>>`: If using azure authentication, the Azure Resource Manager ID for the Azure Databricks workspace, which is exchanged for a Databricks host URL.
- `<<Azure use MSI>>`: If using azure authentication, true to use Azure Managed Service Identity passwordless authentication flow for service principals. Requires AzureResourceID to be set.
- `<<Azure client ID>>`: If using azure authentication, the Azure AD service principal's application ID.
- `<<Azure client secret>>`: If using azure authentication, the Azure AD service principal's tenant ID.
- `<<Azure environment>>`: If using azure authentication, the Azure environment type (such as Public, UsGov, China, and Germany) for a specific set of API endpoints. Defaults to PUBLIC.
- `<<GCP credential file>>`: If using GCP authentication, GCP Service Account Credentials JSON or the location of these credentials on the local filesystem.
- `<<GCP service account>>`: If using GCP authentication, the Google Cloud Platform (GCP) service account e-mail used for impersonation in the Default Application Credentials Flow that does not require a password.


You will also need to configure the Raito CLI further to connect to your Raito Cloud account, if that's not set up yet.
A full guide on how to configure the Raito CLI can be found on (http://docs.raito.io/docs/cli/configuration).

## Trying it out

As a first step, you can check if the CLI finds this plugin correctly. In a command-line terminal, execute the following command:
```bash
$> raito info raito-io/cli-plugin-databricks
```

This will download the latest version of the plugin (if you don't have it yet) and output the name and version of the plugin, together with all the plugin-specific parameters to configure it.

When you are ready to try out the synchronization for the first time, execute:
```bash
$> raito run
```
This will take the configuration from the `raito.yml` file (in the current working directory) and start a single synchronization.

Note: if you have multiple targets configured in your configuration file, you can run only this target by adding `--only-targets databricks` at the end of the command.

## Configuration
The following configuration parameters are available

| Configuration name                  | Description                                                                                                                                                           | Mandatory | Default value |
|-------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|---------------|
| `databricks-account-id`             | The Databricks account to connect to.                                                                                                                                 | True      |               |
| `databricks-platform`               | The Databricks platform to connect to (AWS/GCP/Azure).                                                                                                                | True      |               |
| `databricks-client-id`              | The (oauth) client ID to use when authenticating against the Databricks account.                                                                                      | False     |               |
| `databricks-client-secret `         | The (oauth) client Secret to use when authentic against the Databricks account.                                                                                       | False     |               |
| `databricks-user`                   | The username to authenticate against the Databricks account.                                                                                                          | False     |               |
| `databricks-password`               | The password to authenticate against the Databricks account.                                                                                                          | False     |               |
| `databricks-token`                  | The Databricks personal access token (PAT) (AWS, Azure, and GCP) or Azure Active Directory (Azure AD) token (Azure).                                                  | False     |               |
| `databricks-azure-use-msi `         | `true` to use Azure Managed Service Identity passwordless authentication flow for service principals. Requires AzureResourceID to be set.                             | False     | `false`       |
| `databricks-azure-client-id`        | The Azure AD service principal's client secret.                                                                                                                       | False     |               |
| `databricks-azure-client-secret `   | The Azure AD service principal's application ID.                                                                                                                      | False     |               |
| `databricks-azure-tenant-id`        | The Azure AD service principal's tenant ID.                                                                                                                           | False     |               |
| `databricks-azure-environment`      | The Azure environment type (such as Public, UsGov, China, and Germany) for a specific set of API endpoints.                                                           | False     | `PUBLIC`      |
| `databricks-google-credentials`     | GCP Service Account Credentials JSON or the location of these credentials on the local filesystem.                                                                    | False     |               |
| `databricks-google-service-account` | The Google Cloud Platform (GCP) service account e-mail used for impersonation in the Default Application Credentials Flow that does not require a password.           | False     |               |
| `databricks-data-usage-window`      | The maximum number of days of usage data to retrieve. Maximum is 90 days.                                                                                             | False     | 90            |
| `databricks-sql-warehouses`         | A map of deployment IDs to workspace and warehouse IDs.                                                                                                               | False     | `{}`          |
| `databricks-exclude-workspaces`     | Comma-separated list of workspaces to exclude. If specified, these workspaces will not be handled. Wildcards (*) can be used. Excludes have preference over includes. | False     |               |
| `databricks-include-workspaces`     | Comma-separated list of workspaces to include. If specified, these workspaces will be handled. Wildcards (*) can be used.                                             | False     |               |
| `databricks-exclude-metastores`     | Comma-separated list of metastores to exclude. If specified, these metastores will not be handled. Wildcards (*) can be used. Excludes have preference over includes. | False     |               |
| `databricks-include-metastores`     | Comma-separated list of metastores to include. If specified, these metastores will be handled. Wildcards (*) can be used.                                             | False     |               |
| `databricks-exclude-catalogs`       | Comma-separated list of catalogs to exclude. If specified, these catalogs will not be handled. Wildcards (*) can be used. Excludes have preference over includes.     | False     |               |
| `databricks-include-catalogs`       | Comma-separated list of catalogs to include. If specified, these catalogs will be handled. Wildcards (*) can be used.                                                 | False     |               |
| `databricks-exclude-schemas`        | Comma-separated list of schemas to exclude. If specified, these schemas will not be handled. Wildcards (*) can be used. Excludes have preference over includes.       | False     |               |
| `databricks-include-schemas`        | Comma-separated list of schemas to include. If specified, these schemas will be handled. Wildcards (*) can be used.                                                   | False     |               |
| `databricks-exclude-tables`         | Comma-separated list of tables to exclude. If specified, these tables will not be handled. Wildcards (*) can be used. Excludes have preference over includes.         | False     |               |
| `databricks-include-tables`         | Comma-separated list of tables to include. If specified, these tables will be handled. Wildcards (*) can be used.                                                     | False     |               |


## Supported features

| Feature             | Supported | Remarks                                                                                                          |
|---------------------|-----------|------------------------------------------------------------------------------------------------------------------|
| Row level filtering | ✅         | Databricks feature is still in [Public Preview](https://docs.databricks.com/en/release-notes/release-types.html) |
| Column masking      | ✅         | Databricks feature is still in [Public Preview](https://docs.databricks.com/en/release-notes/release-types.html) |
| Locking             | ❌         | Not supported                                                                                                    |
| Replay              | ✅         | Explicit deletes cannot be replayed                                                                              |
| Usage               | ✅         | Usage SQL parsing is limited to best effort                                                                      |

## Supported data objects
- Metastore
- Workspace
- Catalog
- Schema
- Table
- Column
- Function

## Limitations

It is essential to be aware of these limitations to ensure appropriate usage and manage expectations. The current limitations of the plugin include:

- **Lack of support for data objects**:
Currently, this software only supports data objects defined in the [Supported data objects](#supported-data-objects) section.

- **Limited support for usage**:
The plugin offers support for a subset of SQL statements in the best effort manner. The supported statements include select, insert, merge, update, delete, and copy. However, certain advanced or complex scenarios may not be fully supported.

- **No support for linux 386**:
Currently, the plugin is not supported on linux 386 systems.

- **No support for locking**:
- As databricks is ACL based, access control locking is not possible.

## Access controls
### From Target
#### Unity Catalog Permissions
Unity catalog permissions are imported ad `grant`.
All Unity Catalog permissions that are not set by a Raito managed access control are imported as `grant` in Raito.
A grant will be created for each permission, data object pair. All principals sharing the same permission (and are not set Raito) will be included.

#### Column mask
Column masks are imported as `mask`.
Column masks are imported as non-internalizable because most existing masking policies cannot be correctly interpreted within Raito.

#### Row Filter
Row filters are imported as `filter`.
The same mechanism is used as for column masks. Therefor, row filters are non-internalizable as the `who`-items can not be identified.

## To Target
#### Grants
Grants will be implemented as permissions.
A permission will be grated for each (unpacked) who item, data object pair.

#### Purposes
Purposes will be implemented exactly the same as grants.

#### Masks
Each mask will be exported as masking policy to all schemas associated with the what-items of the mask.
Within each schema a masking policy function is created for each required data type.

#### Filters
Each filter will be exported as row access policy to exactly one table.