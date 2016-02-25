#! /bin/bash

set -e

if env | egrep -q "DOCKER_RIAK_CS_DEBUG"; then
  set -x
fi

CLEAN_DOCKER_HOST=$(echo "${DOCKER_HOST}" | cut -d'/' -f3 | cut -d':' -f1)
CLEAN_DOCKER_HOST=${CLEAN_DOCKER_HOST:-localhost}

INSECURE_KEY_FILE=.insecure_key
SSH_KEY_URL="https://github.com/phusion/baseimage-docker/raw/master/image/services/sshd/keys/insecure_key"
CS01_PORT=$(docker port riak-cs01 22 | cut -d':' -f2)

# Download insecure ssh key of phusion/baseimage-docker
if [ ! -f $INSECURE_KEY_FILE ]; then
  echo
  echo "  Downloading insecure SSH key..."

  if which curl > /dev/null; then
    curl -o .insecure_key -fSL $SSH_KEY_URL > /dev/null 2>&1
  elif which wget > /dev/null; then
    wget -O .insecure_key $SSH_KEY_URL > /dev/null 2>&1
  else
    echo "curl or wget is required to download insecure SSH key. Check"
    echo "the README to get more information about how to download it."
  fi
fi

if [ -f $INSECURE_KEY_FILE ]; then
  # SSH requires some constraints on private key permissions, force it!
  chmod 600 .insecure_key

  ssh -i "${INSECURE_KEY_FILE}" -o "LogLevel=quiet" -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" \
    -p "${CS01_PORT}" "root@${CLEAN_DOCKER_HOST}" "$@"
fi
