#! /bin/bash

readonly WHI='\033[0m'
readonly RED='\e[1;31m'
readonly ORA='\e[0;33m'
readonly YEL='\e[1;33m'
readonly GRE='\e[0;32m'

readonly API_KEY_1="xxxApiKey123"
readonly API_KEY_2="xxxApiKey567"

readonly PROJECT_NAME="LOC_Calculator"
current_dir=$(pwd)
readonly PROJECT_DIR=${current_dir%$PROJECT_NAME*}$PROJECT_NAME

echo -e "${YEL} Build the docker-compose stack ${WHI}"
docker compose -f $PROJECT_DIR/test/integration/docker/docker-compose.yml up -d --force-recreate --build

echo -e "${YEL} Copy requirements.txt to python container and install dependencies ${WHI}"
docker cp $PROJECT_DIR/requirements.txt python:/opt/
docker exec python pip install --no-cache-dir -r requirements.txt
docker exec python pip freeze

echo -e "${YEL} Copy python scripts from repository to python container${WHI}"
docker cp $PROJECT_DIR/src/los_script.py python:/opt/
docker cp $PROJECT_DIR/test/TestLOSCalculation.py python:/opt/

echo -e "${YEL} Execute the test_sftp_connection python script ${WHI}"
docker exec python python test_sftp_connection.py

echo -e "${YEL} Execute unittests for RScript ${WHI}"
docker exec python python TestLOSCalculation.py

echo -e "${YEL} Execute the los_script python script ${WHI}"
docker exec python python los_script.py /opt/settings.toml

result=$(docker exec sftp ./check_if_uploaded_file_exists.sh timeframe.csv)
if [[ "$result" -eq 1 ]]; then
    echo -e "${GRE} R script result was uploaded to SFTP-server!"
elif [[ "$result" -eq 0 ]]; then
    echo -e "${RED} R script result was not found in SFTP-server!"
fi

LIST_CONTAINER=( broker-server broker-connection python sftp )
echo -e "${YEL} Clean up containers ${WHI}"
for container in ${LIST_CONTAINER[*]}; do
  docker stop $container
  docker rm $container
  docker image rm $container
done
