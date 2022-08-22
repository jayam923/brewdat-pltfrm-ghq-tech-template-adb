#!/bin/bash
set -e


GIT_REPO_URL="https://github.com/BrewDat/brewdat-pltfrm-ghq-tech-template-adb.git"
REPO_ROOT="/Repos/brewdat_library"

#git clone repository

cd /home/wesley.silva/Projetos/INBEV/deploy_adb_repo/brewdat-pltfrm-ghq-tech-template-adb
git pull
echo 'Start deploy of all git tags'
for tag in `git tag`; do

	echo "Delivering release $tag"
	REPO_PATH="$REPO_ROOT/$tag"

    #Create the top level folder in case 
    databricks workspace mkdirs $REPO_ROOT

    REPO_LIST=`databricks repos list --path-prefix $REPO_PATH`
    if [[ $REPO_LIST == *"\"$REPO_PATH\""* ]];
	then
	    echo "Repo $REPO_PATH already exist on ADB."
	else
	    echo "Creating a new repo on ADB: $REPO_PATH"
   		databricks repos create --url $GIT_REPO_URL --provider gitHub --path $REPO_PATH
	fi

	echo "Setting repo on $REPO_PATH to tag $tag"
	databricks repos update --path $REPO_PATH --tag $tag

done
# Loop for each worskpace
