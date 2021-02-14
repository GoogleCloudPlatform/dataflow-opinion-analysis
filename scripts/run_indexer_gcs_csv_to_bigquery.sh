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

if [ $# != 6 ]
then
  echo "Invalid number of parameters. Need 6: IndexingType ParsingType ContentType ItemIDColumnIdx TextColumnIdx InputFiles"
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

  if [ "$3" != "ARTICLE" ] && [ "$3" != "SHORTTEXT" ]
  then
    echo "Invalid 3nd parameter value. Use one of {ARTICLE | SHORTTEXT}"
    exit 1
  else
    CONTENTTYPE=$3
  fi

  ITEMIDCOLUMNIDX=$4
  TEXTCOLUMNIDX=$5
  INPUTFILES=$6

fi

EXPERIMENTS="enable_execution_details_collection,use_monitoring_state_manager"
if [ "$UNSUPPORTED_SDK_OVERRIDE_TOKEN" != "" ]
then
  EXPERIMENTS="$EXPERIMENTS,unsupported_sdk_temporary_override_token=$UNSUPPORTED_SDK_OVERRIDE_TOKEN"
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
--indexingType=$INDEXINGTYPE \
--parsingType=$PARSINGTYPE \
--contentType=$CONTENTTYPE \
--ratioEnrichWithCNLP=0 \
--textColumnIdx=$TEXTCOLUMNIDX \
--collectionItemIdIdx=$ITEMIDCOLUMNIDX \
--dedupeText=false \
--bigQueryDataset=$DATASET_ID \
--writeTruncate=true \
--processedUrlHistorySec=130000 \
--experiments=$EXPERIMENTS \
--wrSocialCountHistoryWindowSec=610000"
