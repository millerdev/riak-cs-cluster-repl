#! /bin/bash

docker ps -a | egrep "hectcastro/riak-cs" | cut -d" " -f1 | xargs docker rm -fv > /dev/null 2>&1
echo "Stopped the cluster and cleared all of the running containers."
