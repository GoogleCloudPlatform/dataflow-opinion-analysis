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


if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
	exit 1
fi

if [ "$1" != "mk" ] && [ "$1" != "update" ]
  then
    echo "Invalid parameter value. Use one of {mk | update}"
	exit 1
fi


PROJECT_ID=$(gcloud config list --format 'value(core.project)' 2>/dev/null);
DATASET_ID=${DATASET_ID:-"opinions"}


bq $1 --use_legacy_sql=false --view=\
'WITH 
lastDayOfData AS (
  SELECT CAST (MAX(PublicationTime) AS DATE) AS LastDate FROM `'$PROJECT_ID'.'$DATASET_ID'.webresource` ),
last7days AS (
  SELECT CAST(FORMAT_DATE("%Y%m%d",DATE_SUB(LastDate, INTERVAL 1 DAY)) AS INT64) AS DateId, "T-1" AS TimeMarker, DATE_SUB(LastDate, INTERVAL 1 DAY) AS DateAsDate FROM lastDayOfData UNION ALL
  SELECT CAST(FORMAT_DATE("%Y%m%d",DATE_SUB(LastDate, INTERVAL 2 DAY)) AS INT64) AS DateId, "T-2" AS TimeMarker, DATE_SUB(LastDate, INTERVAL 2 DAY) AS DateAsDate FROM lastDayOfData  UNION ALL
  SELECT CAST(FORMAT_DATE("%Y%m%d",DATE_SUB(LastDate, INTERVAL 3 DAY)) AS INT64) AS DateId, "T-3" AS TimeMarker, DATE_SUB(LastDate, INTERVAL 3 DAY) AS DateAsDate FROM lastDayOfData  UNION ALL
  SELECT CAST(FORMAT_DATE("%Y%m%d",DATE_SUB(LastDate, INTERVAL 4 DAY)) AS INT64) AS DateId, "T-4" AS TimeMarker, DATE_SUB(LastDate, INTERVAL 4 DAY) AS DateAsDate FROM lastDayOfData  UNION ALL
  SELECT CAST(FORMAT_DATE("%Y%m%d",DATE_SUB(LastDate, INTERVAL 5 DAY)) AS INT64) AS DateId, "T-5" AS TimeMarker, DATE_SUB(LastDate, INTERVAL 5 DAY) AS DateAsDate FROM lastDayOfData  UNION ALL
  SELECT CAST(FORMAT_DATE("%Y%m%d",DATE_SUB(LastDate, INTERVAL 6 DAY)) AS INT64) AS DateId, "T-6" AS TimeMarker, DATE_SUB(LastDate, INTERVAL 6 DAY) AS DateAsDate FROM lastDayOfData  UNION ALL
  SELECT CAST(FORMAT_DATE("%Y%m%d",DATE_SUB(LastDate, INTERVAL 7 DAY)) AS INT64) AS DateId, "T-7" AS TimeMarker, DATE_SUB(LastDate, INTERVAL 7 DAY) AS DateAsDate FROM lastDayOfData 
)
SELECT DateId, TimeMarker, DateAsDate FROM last7Days ORDER BY DateID' \
$DATASET_ID.vwlast7days;


bq $1 --use_legacy_sql=false --view=\
'SELECT *
FROM `'$PROJECT_ID'.'$DATASET_ID'.statdomainopinions`
ORDER BY MeanBasic8Ratio ASC' \
$DATASET_ID.vwdomainopinions;


bq $1 --use_legacy_sql=false --view=\
'SELECT 
  s.DocumentDateId AS SnapshotDateId, last7days.TimeMarker, ARRAY_TO_STRING(ARRAY(SELECT tg.Tag FROM UNNEST(s.Tags) AS tg ORDER BY tg.GoodAsTopic LIMIT 3), " & ") AS Topic, s.SentimentTotalScore, 
  (CASE DominantValence WHEN 1 THEN "Positive" WHEN 2 THEN "Negative" WHEN 3 THEN "Ambiguous" WHEN 5 THEN "General" ELSE "Unknown" END) AS DominantValence,
  s.Text,
  s.AnnotatedText,
  s.AnnotatedHtml,
  StAcceptance, StAnger, StAnticipation, StAmbiguous, StDisgust, StFear, StGuilt, StInterest, StJoy, StSadness, StShame, StSurprise, StPositive, StNegative, StSentiment, StProfane, StUnsafe
  ,SUBSTR(wr.Title,0,1000) AS Title
  ,wr.Url
  ,wr.Domain
  ,wr.Author
FROM `'$PROJECT_ID'.'$DATASET_ID'.sentiment` s 
  INNER JOIN `'$PROJECT_ID'.'$DATASET_ID'.vwlast7days` last7days ON s.DocumentDateId = last7days.DateId 
  INNER JOIN `'$PROJECT_ID'.'$DATASET_ID'.webresource` wr ON wr.WebResourceHash = s.MainWebResourceHash
ORDER BY 1, 3' \
$DATASET_ID.vwsentiment4search7d;


bq $1 --use_legacy_sql=false --view=\
'SELECT * FROM `'$PROJECT_ID'.'$DATASET_ID'.statstoryrank`' \
$DATASET_ID.vwstoryrank;


bq $1 --use_legacy_sql=false --view=\
'SELECT * FROM `'$PROJECT_ID'.'$DATASET_ID'.stattoptopic7d`' \
$DATASET_ID.vwtoptopic7d;


bq $1 --use_legacy_sql=false --view=\
'SELECT * FROM `'$PROJECT_ID'.'$DATASET_ID'.stattopstory7d`' \
$DATASET_ID.vwtopstory7d;


bq $1 --use_legacy_sql=false --view=\
'SELECT * FROM `'$PROJECT_ID'.'$DATASET_ID'.stattoptopic7dsentiment`' \
$DATASET_ID.vwtoptopic7dsentiment;
