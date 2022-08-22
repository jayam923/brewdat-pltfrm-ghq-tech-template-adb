# Manual deployment process

The following steps are required to deploy this brewdat-lib to a Azure Databricks Workspace while proper automatic CI/CD process is not in place.

## 0 - Permissions required

- Read access to this repo.
- Admin access to ADB workspace.

## 1 - Setup git access in ADB

On ADB Workspace, configure your Github user token under: *Settings > User settings > Git integration*.

Select 'GitHub' as Git provider, inform your github username and token with 'repo' persmission. 
Instructions for generating a personal access token can be found [here](https://help.github.com/articles/creating-an-access-token-for-command-line-use/).

## 2 - Setup Databricks CLI on local machine

Install Databricks CLI on your local machine. Requirements and instructions can be found [here](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/cli/).

Setup authentication using a Databricks personal access token as described [here](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/cli/#set-up-authentication-using-a-databricks-personal-access-token).

Instructions for generating a Databricks personal access token can be found [here](https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-personal-access-token).


## 3 - Run deployment script

