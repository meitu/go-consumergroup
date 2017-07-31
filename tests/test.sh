#!/bin/bash

docker_path="`pwd`/docker"
integration_path="`pwd`/integration"
unittest_path="`pwd`/.."

cd $docker_path && sh setup.sh
# run unittest
cd $unittest_path && go test .
# run intergration test
cd $integration_path && go test .
cd $docker_path && sh teardown.sh
