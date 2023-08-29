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
    <a href="https://codecov.io/gh/raito-io/cli-plugin-databricks" target="_blank"><img src="https://img.shields.io/codecov/c/github/raito-io/cli-plugin-databricks" alt="Code Coverage" /></a>
</p>

<hr/>

# Raito CLI Plugin - Databricks Unity Catalog

**Note: This repository is still in an early stage of development.
At this point, no contributions are accepted to the project yet.**

This Raito CLI plugin implements the integration with Databricks. It can
 - Synchronize the users in a databricks account to an identity store in Raito Cloud.
 - Synchronize the Databricks Unity Catalog meta data (data structure, known permissions, ...) to a data source in Raito Cloud.
 - Synchronize the access providers from Raito Cloud into Databricks Unity Catalog grants.
 - Synchronize the data usage information to Raito Cloud.

[//]: # ( - Synchronize the data usage information to Raito Cloud.)


## Prerequisites
To use this plugin, you will need

1. The Raito CLI to be correctly installed. You can check out our [documentation](http://docs.raito.io/docs/cli/installation) for help on this.
2. A Raito Cloud account to synchronize your Databricks account with. If you don't have this yet, visit our webpage at (https://raito.io) and request a trial account.
3. An admin user on your databricks account with admin access on all workspaces
4. Within the databricks account and all workspaces, Unity Catalog should be enabled

[//]: # (A full example on how to start using Raito Cloud with Databricks can be found as a [guide in our documentation]&#40;http://docs.raito.io/docs/guide/cloud&#41;.)

## Usage
To use the plugin, add the following snippet to your Raito CLI configuration file (`raito.yml`, by default) under the `targets` section:

```yaml
 - name: databricks
   connector-name: raito-io/cli-plugin-databricks
   data-source-id: <<Databricks datasource ID>>   
   identity-store-id: <<Databricks identitystore ID>>

   databricks-account-id: <<Databricks account ID>>
   databricks-client-id: <<Databricks client ID>>
   databricks-client-secret: <<Databricks client secret>>
   databricks-user: <<Databricks user email address>>
   databricks-password: <<Databricks user password>>
```

Next, replace the values of the indicated fields with your specific values:
- `<<Databricks datasource ID>>`: the ID of the Data source you created in the Raito Cloud UI.
- `<<Databricks identitystore ID>>`: the ID of the Identity Store you created in the Raito Cloud UI.
- `<<Databricks account ID>>`: the Databricks account ID`
- `<<Databricks client ID>>`: if using oauth, the Databricks client ID of an account with admin access to all workspaces
- `<<Databricks client secret>>`: if using oauth, the Databricks client secret of the account specified in `databricks-client-id`
- `<<Databricks user email address>>`: if using basic auth, the email address of an admin user in your databricks account with admin access to all workspaces
- `<<Databricks user password>>`: if using basic auth, the email password of the `databricks-user` user


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

## Limitations

It is essential to be aware of these limitations to ensure appropriate usage and manage expectations. The current limitations of the plugin include:

- **Lack of support for functions, external locations, and shares**:
At present, the plugin does not provide support for functions, external locations, or shares data objects.

- **Limited support for usage**:
The plugin offers support for a subset of SQL statements in a best effort manner. The supported statements include select, insert, merge, update, delete, and copy. However, certain advanced or complex scenarios may not be fully supported.

- **Support for Row and Column masking**:
Currently, the plugin does not support row and column masking.