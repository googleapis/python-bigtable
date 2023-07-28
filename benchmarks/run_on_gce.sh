# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)
ROW_SIZE=100

SCRIPT_DIR=$(realpath $(dirname "$0"))
cd $SCRIPT_DIR/..

# build container
export DOCKER_BUILDKIT=1
IMAGE_PATH="gcr.io/$PROJECT_ID/python-bigtable-benchmark"
docker build -t $IMAGE_PATH -f $SCRIPT_DIR/Dockerfile .
docker push $IMAGE_PATH

# deploy to GCE
INSTANCE_NAME="python-bigtable-benchmark-$(date +%s)"
ZONE=us-central1-b
function finish {
  # remove VM on script exit
  gcloud compute instances delete $INSTANCE_NAME --zone=$ZONE --quiet &
  sleep 0.5
}
trap finish EXIT
gcloud compute instances create-with-container $INSTANCE_NAME \
  --container-image=$IMAGE_PATH \
  --machine-type=n2-standard-8 \
  --zone=$ZONE \
  --scopes=cloud-platform \
  --container-restart-policy=never \
  --container-env=ROW_SIZE=$ROW_SIZE \
  --container-env=TEST_DURATION=30

# find container id
echo "waiting for container to start..."
sleep 5
while [[ -z "$CONTAINER_ID" ]]; do
  sleep 2
  CONTAINER_ID=$(gcloud compute instances get-serial-port-output $INSTANCE_NAME --zone $ZONE  2>/dev/null | grep "Starting a container with ID" |  awk '{print $NF}')
done
echo "found container id: $CONTAINER_ID"

# print logs
gcloud beta logging tail "$CONTAINER_ID" --format='value(jsonPayload.message)'
