#!/bin/bash
# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
function finish {
  kill -- -$$  2> /dev/null # kill all child processes
}
trap finish EXIT

# ensure the working dir is the script's folder
SCRIPT_DIR=$(realpath $(dirname "$0"))
cd $SCRIPT_DIR

export PROXY_SERVER_PORT=50055

# download test suite
if [ ! -d "cloud-bigtable-clients-test" ]; then
  git clone https://github.com/googleapis/cloud-bigtable-clients-test.git
fi

# start proxy
python test_proxy.py --port $PROXY_SERVER_PORT &

# run tests
pushd cloud-bigtable-clients-test/tests
go test -v -proxy_addr=:$PROXY_SERVER_PORT
