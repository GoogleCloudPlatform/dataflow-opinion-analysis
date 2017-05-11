# Copyright 2017 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

PROJECT_ID="replace_with_project_id"
DATASET_ID="opinions"
GCS_BUCKET="replace_with_bucket"
INDEXER_CONTROL_TOPIC="indexercommands"
INDEXER_DOCUMENT_TOPIC="documents"

JDBC_SOURCE_URL="jdbc:mysql://<complete_the_url>"
JDBC_SOURCE_USERNAME="replace"
JDBC_SOURCE_PASSWORD="replace"


mvn compile exec:java \
  -Dexec.mainClass=com.google.cloud.dataflow.examples.opinionanalysis.ControlPipeline\
  -Dexec.args="--project=$PROJECT_ID \
    --stagingLocation=gs://$GCS_BUCKET/staging/ \
    --runner=DataflowRunner \
    --tempLocation=gs://$GCS_BUCKET/temp/ \
    --maxNumWorkers=1 \
    --zone=us-central1-f \
    --streaming=true \
    --controlPubsub=true \
    --controlGCS=false \
    --controlPubsubTopic=projects/$PROJECT_ID/topics/$INDEXER_CONTROL_TOPIC \
    --controlGCSPath=gs://$GCS_BUCKET/indexercontrol/*.txt \
    --jobAutoscalingAlgorithm=THROUGHPUT_BASED \
    --jobMaxNumWorkers=100 \
    --sourceJDBC=false \
    --jdbcDriverClassName=com.mysql.jdbc.Driver \
    --jdbcSourceUrl=$JDBC_SOURCE_URL \
    --jdbcSourceUsername=$JDBC_SOURCE_USERNAME \
    --jdbcSourcePassword=$JDBC_SOURCE_PASSWORD \
    --sourcePubsub=false \
    --pubsubTopic=projects/$PROJECT_ID/topics/$INDEXER_DOCUMENT_TOPIC \
    --sourceRecordFile=false \
    --inputFile=gs://$GCS_BUCKET/input/*.txt \
    --bigQueryDataset=$DATASET_ID \
    --writeTruncate=false \
    --processedUrlHistorySec=130000 \
    --wrSocialCountHistoryWindowSec=610000"
