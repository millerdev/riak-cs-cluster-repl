#! /bin/bash

set -e

if env | egrep -q "DOCKER_RIAK_CS_DEBUG"; then
  set -x
fi

CLEAN_DOCKER_HOST=$(echo "${DOCKER_HOST}" | cut -d'/' -f3 | cut -d':' -f1)
CLEAN_DOCKER_HOST=${CLEAN_DOCKER_HOST:-localhost}

index_n=$(docker ps | grep "riak-cs" | wc -l)
index_n=$((index_n + 1))
DOCKER_RIAK_CS_CLUSTER_SIZE="$index_n"

index=$(seq -w "$index_n" "10" | head -n 1)
DOCKER_RIAK_CS_AUTOMATIC_CLUSTERING="1"

if [ "${index}" -gt "1" ] ; then
    docker run -e "DOCKER_RIAK_CS_CLUSTER_SIZE=${DOCKER_RIAK_CS_CLUSTER_SIZE}" \
       -e "DOCKER_RIAK_CS_AUTOMATIC_CLUSTERING=${DOCKER_RIAK_CS_AUTOMATIC_CLUSTERING}" \
       -P --name "riak-cs${index}" --link "riak-cs01:seed" \
       -d hectcastro/riak-cs > /dev/null 2>&1
else
    docker run -e "DOCKER_RIAK_CS_CLUSTER_SIZE=${DOCKER_RIAK_CS_CLUSTER_SIZE}" \
       -e "DOCKER_RIAK_CS_AUTOMATIC_CLUSTERING=${DOCKER_RIAK_CS_AUTOMATIC_CLUSTERING}" \
       -P --name "riak-cs${index}" -d hectcastro/riak-cs > /dev/null 2>&1
fi

CONTAINER_ID=$(docker ps | egrep "riak-cs${index}" | cut -d" " -f1)
CONTAINER_PORT=$(docker port "${CONTAINER_ID}" 8080 | cut -d ":" -f2)
CONFIG_PATH="config.json"

until curl -s "http://${CLEAN_DOCKER_HOST}:${CONTAINER_PORT}/riak-cs/ping" | egrep "OK" > /dev/null 2>&1;
do
    sleep 3
done

echo "  Successfully brought up [riak-cs${index}]"

if [ "${index}" -eq "01" ] ; then
    echo "{\"url\": \"http://${CLEAN_DOCKER_HOST}:${CONTAINER_PORT}\"," > $CONFIG_PATH

    for field in admin_key admin_secret ; do
      until ./ssh_command.sh egrep "${field}" /etc/riak-cs/app.config | cut -d'"' -f2 | egrep -v "admin" > /dev/null 2>&1
      do
        sleep 1
      done
      value=$(./ssh_command.sh egrep "${field}" /etc/riak-cs/app.config | cut -d'"' -f2)
      echo "\"${field}\": \"${value}\"," >> $CONFIG_PATH
    done

    config=$(cat "${CONFIG_PATH}")
    config=${config%$","}
    echo "$config}" > $CONFIG_PATH
else
    echo "  Waiting for cluster to stabalize"
    until ./ssh_command.sh riak-admin member-status | egrep "valid" | wc -l | egrep -q "${DOCKER_RIAK_CS_CLUSTER_SIZE}"
    do
      sleep 2
    done
fi
