# Copyright 2021 Google Inc. All Rights Reserved.
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


if [ $# != 5 ]
then
  echo "Invalid number of parameters. Need 5: IndexingType ParsingType ItemIDColumnIdx TextColumnIdx InputFiles"
  exit 1
else
  if [ "$1" != "FULLINDEX" ] && [ "$1" != "TOPSENTIMENTS" ]
  then
    echo "Invalid 1st parameter value. Use one of {FULLINDEX | TOPSENTIMENTS}"
    exit 1
  else
    INDEXINGTYPE=$1
  fi

  if [ "$2" != "DEEP" ] && [ "$2" != "SHALLOW" ] && [ "$2" != "DEPENDENCY" ]
  then
    echo "Invalid 2nd parameter value. Use one of {DEEP | SHALLOW | DEPENDENCY}"
    exit 1
  else
    PARSINGTYPE=$2
  fi

  ITEMIDCOLUMNIDX=$3
  TEXTCOLUMNIDX=$4
  INPUTFILES=$5

fi


mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipeline \
-Dexec.args="\
--project=$PROJECT_ID \
--stagingLocation=gs://$GCS_BUCKET/staging/ \
--runner=DataflowRunner \
--filesToStage=./target/examples-opinionanalysis-bundled-0.7.0.jar \
--tempLocation=gs://$GCS_BUCKET/temp/ \
--maxNumWorkers=10 \
--region=us-central1 \
--streaming=false \
--sourceRecordFile=true \
--inputFile=$INPUTFILES \
--readAsCSV=true \
--recordDelimiters=30 \
--indexAsShorttext=true \
--ratioEnrichWithCNLP=0 \
--textColumnIdx=$TEXTCOLUMNIDX \
--collectionItemIdIdx=$ITEMIDCOLUMNIDX \
--dedupeText=false \
--bigQueryDataset=$DATASET_ID \
--writeTruncate=true \
--processedUrlHistorySec=130000 \
--experiments=enable_execution_details_collection,use_monitoring_state_manager,unsupported_sdk_temporary_override_token=2021-02-13T19:27:16-08:00:ARR1cwqcRETYoL2BHEANlcbwHUdIoSqVQ7Q53rUVBf8VMdexpeAa00rbZ24BQEsyUu11tAVzQ93FoL9PdPMIpX2Q8j34VQXKbrBJrr_JpheFgUQ0el_fuPs2MZk6a-rRKP6-NmX-LbFEBA \
--wrSocialCountHistoryWindowSec=610000"
