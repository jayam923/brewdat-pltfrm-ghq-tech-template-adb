# BrewDat Library versioning and distribution

Created: May 13, 2022  
Updated: May 31, 2022

# Motivation

In ESA, we intend to provide data engineers with notebook templates and libraries that they can leverage to accelerate the development process -- extracting data to the Raw Layer, ingesting it into the Bronze Layer, ingesting it into the Silver Layer, and finally creating Gold data products based on that data, all while using best practices for auditing, exception handling, dependency control, Data Lake organization, etc. One important aspect of these templates is that they will evolve over time, meaning newer versions will be release as new features become available (e.g., a new standard library function for running data quality checks, or a bugfix to an existing library function). These new template versions will be made available for ESA users; however, we should not overwrite the user's copy without their consent, as that could cause existing data pipelines to fail. Instead, we plan on maintaining the many template versions in a source control software (GitHub), using releases with semantic versioning to allow multiple versions of the same templates to coexist. The objective is to allow users to upgrade/downgrade their code at their own pace, after proper testing.

# Assumptions

1. All releases must be properly versioned and documented such that anyone can see what changed in each release;
2. BrewDat/ESA platform team is responsible for releasing new versions of the library;
3. Data Engineers are responsible for choosing a library version to work with, as well as upgrade/downgrade it on any of their notebooks after proper testing; 
4. All released versions of the library should be made available to the all Databricks workspaces in ESA through an automated deployment process;

# Data Engineer experience using the library

Every Azure Databricks Workspace in ESA will contain multiple vesions of the library as read-only repositories under Repos/brewdat_library/:

![Screenshot from 2022-05-19 07-57-54.png](img/release_workflow/adb-repos.png)

Each repository includes multiple library artifacts, including: Python modules and scripts, sample notebooks, markdown documents such as this one, etc. Only Workspace Admins can change these repositories.

To start using the library code in a custom notebook, append the path for the library version to sys.path: 

```python
import sys

sys.path.append("/Workspace/Repos/brewdat_framework/v0.1.0")
```

Different notebooks can reference different library versions, even if they are running on the same cluster. 

To use some functionality from the library, you must use Python's import statement informing which class/function you require:

```python
# Import the BrewDat Library class
sys.path.append(f"/Workspace/Repos/brewdat_framework/v0.1.0")
from brewdat.data_engineering.utils import BrewDatLibrary

# Initialize the BrewDat Library
brewdat_library = BrewDatLibrary(spark=spark, dbutils=dbutils)
```

To upgrade the library on existing notebooks, Data Engineers need to change the appended path to point to the new library version. It is good practice to run integration tests before promoting the new code to production.

# Library versioning and release

Library artifacts are versioned in a git repository. All ABI Data Engineers and Data Architects should be able to view it: [https://github.com/BrewDat/brewdat-pltfrm-ghq-tech-template-adb](https://github.com/BrewDat/brewdat-pltfrm-ghq-tech-template-adb)

The workflow for evolving library artifacts follows the Gitflow workflow, as discribed in [here](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow).

![gitflow.png](img/release_workflow/gitflow.png)

For every commit to the main branch, a new [GitHub Release](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository) and tag are created. The release page provides an overview of changes introduced in each release. Through release page, it is easy to compare library versions and have a more in-depth understanding of their differences.

![github-release.png](img/release_workflow/github-release.png)

Release and tags names always reference a library version. Version numbers follow the semantic versioning standard (see: [https://semver.org/spec/v2.0.0.html](https://semver.org/spec/v2.0.0.html)).

## CI/CD pipeline

### Unit tests

Every commit on any branch should trigger the pipeline that runs unit tests. 

Merging new code into develop and master branches should be allowed only when all unit tests ran successfully.

Unit tests should be written as much as possible, in order to get more stable releases.

### Delivery

Every new tag created for a any release should trigger the delivery process. The steps for the delivery pipeline are:

- List all existing ADB workspaces on ESA platform;
- List all existing tags on library repo;
- For every combination of tag and ADB workspace:
    - check if there is already a repo referencing the tag on ADB workspace
        - when not, add a new repo to the ADB workspace and attach it to the tag

Assumptions:
- The pipeline agent must have permissions to list ADB workspaces on every subscription of ESA;
- The pipeline agent must have permissions to interact with every ADB workspace through the Repos API and Workspace API;
- Since the delivery is triggered by the tag creation event, when a new ADB workspace is created, the delivery pipeline must be triggered manually.

## Collaborative library evolution

DE from DLZs can support the evolution of the library by helping to validate unreleased versions or actively submitting pull-requests from private forks.

For testing a unreleased version, the DE can add a repo under his personal folder and set the branch where the feature they want to validate is being developed (it could be either a hotfix, release, feature or develop branch).

## Outcomes

- Integrated to git and managed releases
- CI/CD pipelines
- Allow multiple versions of the library to coexist on ADB workspace
- All library artifacts are available to DE on ADB workspace
- DE can take part on library development
- Unit tests can be integrated into the library development workflow
