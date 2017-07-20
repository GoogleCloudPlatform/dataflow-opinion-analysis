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

mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipeline \
-Dexec.args="--project=$PROJECT_ID \
--stagingLocation=gs://$GCS_BUCKET/staging/ \
--runner=DataflowRunner \
--filesToStage=./target/examples-opinionanalysis-bundled-0.4.0.jar \
--tempLocation=gs://$GCS_BUCKET/temp/ \
--maxNumWorkers=70 \
--workerMachineType=n1-standard-2 \
--zone=us-central1-f \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--streaming=false \
--sourceRedditBQ=true \
--redditPostsTableName=\"fh-bigquery:reddit_posts.2017_04\" \
--redditCommentsTableName=\"fh-bigquery:reddit_comments.2017_04\" \
--bigQueryDataset=$DATASET_ID \
--writeTruncate=false \
--processedUrlHistorySec=130000 \
--wrSocialCountHistoryWindowSec=610000"
