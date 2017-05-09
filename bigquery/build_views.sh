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
'WITH
s1 AS (
  SELECT wr.Domain, COUNT(*) AS cntWRs
  FROM `'$PROJECT_ID'.'$DATASET_ID'.webresource` wr
  GROUP BY 1 HAVING COUNT(*) > 10
),
s1a AS (
  SELECT 
    Domain, cntWRs,
    RANK() OVER (PARTITION BY Domain ORDER BY cntWRs DESC) AS DomainRank
  FROM s1
),
s2 AS (
  SELECT 
    wr.PublicationTime AS PublicationTime,  
    wr.Domain AS Domain,
    wr.Author AS Author,
    IF(s1a.DomainRank <= 50, 1, 0) AS IsTop50Domain,
    ARRAY(SELECT tg.Tag FROM UNNEST(s.Tags) AS tg WHERE tg.GoodAsTopic = TRUE) AS TopicArray, 
    s.SentimentTotalScore, 
    DominantValence AS Valence,
    StAcceptance, StAnger, StAnticipation, StAmbiguous, StDisgust, StFear, StGuilt, StInterest, StJoy, StSadness, StShame, StSurprise, StPositive, StNegative, StSentiment, StProfane, StUnsafe
  FROM `'$PROJECT_ID'.'$DATASET_ID'.sentiment` s 
    INNER JOIN `'$PROJECT_ID'.'$DATASET_ID'.webresource` wr ON wr.DocumentHash = s.DocumentHash
    INNER JOIN s1a ON s1a.Domain = wr.Domain
),
s3 AS (
  SELECT
    FORMAT_TIMESTAMP("%Y_%U",PublicationTime) AS PubWeek,
    Domain,
    IsTop50Domain,
    ta AS Topic,
    SUM((case when Valence=1 then 1 else 0 end)) as cntPositives,
    SUM((case when Valence=2 then 1 else 0 end)) as cntNegatives,
    SUM((case when Valence=3 then 1 else 0 end)) as cntAmbiguous,
    SUM((case when Valence=5 then 1 else 0 end)) as cntGeneral,
    COUNT(1) AS cntTotalMentions,
    SUM(StAcceptance) AS StAcceptance, 
    SUM(StAnger) AS StAnger, 
    SUM(StAnticipation) AS StAnticipation, 
    SUM(StAmbiguous) AS StAmbiguous, 
    SUM(StDisgust) AS StDisgust, 
    SUM(StFear) AS StFear, 
    SUM(StGuilt) AS StGuilt, 
    SUM(StInterest) AS StInterest, 
    SUM(StJoy) AS StJoy, 
    SUM(StSadness) AS StSadness, 
    SUM(StShame) AS StShame, 
    SUM(StSurprise) AS StSurprise, 
    SUM(StPositive) AS StPositive, 
    SUM(StNegative) AS StNegative, 
    SUM(StSentiment) AS StSentiment, 
    SUM(StProfane) AS StProfane, 
    SUM(StUnsafe) AS StUnsafe,
    GREATEST(SUM(StJoy+StAcceptance+StFear+StSurprise+StSadness+StDisgust+StAnger+StAnticipation),0.01) AS Basic8Score,
    GREATEST(SUM(StJoy+StAcceptance+StSurprise+StAnticipation),0.01) AS Positive4Score,
    GREATEST(SUM(StFear+StSadness+StDisgust+StAnger),0.01) AS Negative4Score
  FROM s2, UNNEST(s2.TopicArray) AS ta
  WHERE ta NOT IN (/*list of excluded topics*/"")
  GROUP BY 1,2,3,4
),
s4 AS (
  SELECT 
    Domain,
    Topic,
    SUM(cntTotalMentions) AS cntTotalMentions
  FROM s3
  GROUP BY 1,2
),
s5 AS (
  SELECT 
    Domain,
    Topic,
    cntTotalMentions,
    RANK() OVER (PARTITION BY Domain ORDER BY cntTotalMentions DESC, LENGTH(Topic) DESC ) AS DomainTopicRank
  FROM s4
),
s6 AS (
  SELECT
    s3.PubWeek,
    s3.Domain,
    s3.IsTop50Domain,
    s3.Topic,
    s5.DomainTopicRank,
    IF(s5.DomainTopicRank <= 20, 1, 0) AS IsTop20DomainTopic,
    cntPositives,
    cntNegatives,
    cntAmbiguous,
    cntGeneral,
    s3.cntTotalMentions,
    s3.Basic8Score,
    (cntPositives - cntNegatives)/GREATEST(s3.cntTotalMentions,1) AS SentimentRatio,
    ROUND(((Positive4Score - Negative4Score) / Basic8Score ),2) AS Basic8Ratio,
    -- 8 basic emotions --
    ROUND(StJoy/Basic8Score,2) AS StJoy,
    ROUND(StAcceptance/Basic8Score,2) AS StAcceptance,
    ROUND(StFear/Basic8Score,2) AS StFear,
    ROUND(StSurprise/Basic8Score,2) AS StSurprise,
    ROUND(StSadness/Basic8Score,2) AS StSadness,
    ROUND(StDisgust/Basic8Score,2) AS StDisgust,
    ROUND(StAnger/Basic8Score,2) AS StAnger,
    ROUND(StAnticipation/Basic8Score,2) AS StAnticipation    
  FROM s3
    INNER JOIN s5 ON s5.Domain = s3.Domain AND s5.Topic = s3.Topic
),
s7 AS (
  SELECT 
    Topic,
    Domain,
    MAX(IsTop50Domain) AS IsTop50Domain,
    MIN(s6.DomainTopicRank) AS DomainTopicRank,
    MAX(IsTop20DomainTopic) AS IsTop20DomainTopic,
    SUM(cntTotalMentions) AS cntTotalMentions,
    ROUND(SUM(Basic8Score),2) AS Basic8Score,

    ROUND(AVG(Basic8Ratio),2) AS MeanBasic8Ratio,
    ROUND(STDDEV_POP(Basic8Ratio),2) AS StdevBasic8Ratio,

    ROUND(AVG(StJoy),2) AS MeanStJoy,
    ROUND(STDDEV_POP(StJoy),2) AS StdevStJoy,

    ROUND(AVG(StAcceptance),2) AS MeanStAcceptance,
    ROUND(STDDEV_POP(StAcceptance),2) AS StdevStAcceptance,

    ROUND(AVG(StFear),2) AS MeanStFear,
    ROUND(STDDEV_POP(StFear),2) AS StdevStFear,

    ROUND(AVG(StSurprise),2) AS MeanStSurprise,
    ROUND(STDDEV_POP(StSurprise),2) AS StdevStSurprise,

    ROUND(AVG(StSadness),2) AS MeanStSadness,
    ROUND(STDDEV_POP(StSadness),2) AS StdevStSadness,

    ROUND(AVG(StDisgust),2) AS MeanStDisgust,
    ROUND(STDDEV_POP(StDisgust),2) AS StdevStDisgust,

    ROUND(AVG(StAnger),2) AS MeanStAnger,
    ROUND(STDDEV_POP(StAnger),2) AS StdevStAnger,

    ROUND(AVG(StAnticipation),2) AS MeanStAnticipation,
    ROUND(STDDEV_POP(StAnticipation),2) AS StdevStAnticipation,
    
    ROUND(AVG(SentimentRatio),2) AS MeanSentimentRatio,
    ROUND(STDDEV_POP(SentimentRatio),2) AS StdevSentimentRatio
       
  FROM s6
  GROUP BY 1,2
  ORDER BY 3 ASC
)
SELECT * 
FROM s7
WHERE s7.Basic8Score >= 30
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
  ,wr.Title
  ,wr.Url
  ,wr.Domain
  ,wr.Author
FROM `'$PROJECT_ID'.'$DATASET_ID'.sentiment` s 
  INNER JOIN `'$PROJECT_ID'.'$DATASET_ID'.vwlast7days` last7days ON s.DocumentDateId = last7days.DateId 
  INNER JOIN `'$PROJECT_ID'.'$DATASET_ID'.webresource` wr ON wr.DocumentHash = s.DocumentHash
ORDER BY 1, 3' \
$DATASET_ID.vwsentiment4search7d;


bq $1 --use_legacy_sql=false --view=\
'WITH
s1 AS (
  SELECT
    d.PublicationDateId,
    d.DocumentHash,
    FORMAT_TIMESTAMP("%Y_%m",d.PublicationTime) AS PubMonth,
    FORMAT_TIMESTAMP("%Y_%U",d.PublicationTime) AS PubWeek,
    wrOrig.Title,
    wrOrig.Url,
    wrOrig.Domain,
    wrOrig.Author
  FROM `'$PROJECT_ID'.'$DATASET_ID'.document` d
    INNER JOIN `'$PROJECT_ID'.'$DATASET_ID'.webresource` wrOrig ON wrOrig.DocumentHash = d.DocumentHash
  WHERE 
    CAST(d.PublicationTime AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY)
),
s2 AS (
  SELECT s1.DocumentHash, MAX(impact.SnapshotDateId) AS LatestStatsDateId
  FROM s1
    INNER JOIN `'$PROJECT_ID'.'$DATASET_ID'.statstoryimpact` impact ON impact.DocumentHash = s1.DocumentHash
  GROUP BY s1.DocumentHash
),
s3 AS (
  SELECT
    s1.DocumentHash,
    s1.PublicationDateId,
    s1.PubMonth,
    s1.PubWeek,
    s1.Title,
    s1.Url,
    s1.Domain,
    s1.Author,
    RANK() OVER (PARTITION BY s1.PubWeek ORDER BY impact.cntFb DESC, impact.cntWRs DESC) AS rankWeekly,
    RANK() OVER (PARTITION BY s1.PubMonth ORDER BY impact.cntFb DESC, impact.cntWRs DESC) AS rankMonthly,
    impact.cntWRs, 
    impact.cntDomains, 
    impact.cntFb, 
    ARRAY_TO_STRING(impact.Domains,",") AS Domains, 
    ARRAY_TO_STRING(impact.Urls,",") AS Urls
  FROM s1
    INNER JOIN s2 ON s2.DocumentHash = s1.DocumentHash
    INNER JOIN `'$PROJECT_ID'.'$DATASET_ID'.statstoryimpact` impact ON impact.DocumentHash = s2.DocumentHash AND impact.SnapshotDateId = s2.LatestStatsDateId
)
SELECT * FROM s3' \
$DATASET_ID.vwstoryimpact;


bq $1 --use_legacy_sql=false --view=\
'WITH 
toptopics1 AS (
  SELECT st.* EXCEPT (SentimentHashes), 
    (cntPositives - cntNegatives)/GREATEST(cntPositives + cntNegatives + cntAmbiguous + cntGeneral,1) AS SentimentRatio,
    RANK() OVER (PARTITION BY st.SnapshotDateId ORDER BY cntOrigPublishers DESC, cntRepostWRs DESC, TagCount DESC, Topic) AS rankPubdomains,
    last7days.TimeMarker
  FROM `'$PROJECT_ID'.'$DATASET_ID'.stattopic` st
    INNER JOIN `'$PROJECT_ID'.'$DATASET_ID'.vwlast7days` last7days ON st.SnapshotDateId = last7days.DateId
  WHERE st.Topic NOT IN (/*list of excluded topics*/"")
  ORDER BY st.SnapshotDateId, rankPubdomains
),
toptopics2 AS ( 
  SELECT toptopics1.*, (CASE WHEN toptopics1.rankPubdomains <= 20 THEN 1 ELSE 0 END) AS IsTop20Topic FROM toptopics1
),
toptopics7d AS (
  SELECT 
    toptopics2.Topic,
    MIN(CASE WHEN IsTop20Topic = 1 THEN TimeMarker ELSE NULL END) AS LatestTimeMarker,
    SUM(IsTop20Topic) AS NumInTop20,
    AVG(1/LN(rankPubdomains+1)) AS AvgTopicDominance,
    (SUM(IsTop20Topic) * AVG(1/LN(rankPubdomains+1))) AS CycleTopicDominance
  FROM toptopics2
  GROUP BY toptopics2.Topic
),
toptopics7dtop20 AS (
  SELECT * FROM toptopics7d ORDER BY CycleTopicDominance DESC LIMIT 20
),
toptopics3 AS (
  SELECT toptopics2.*, (CASE WHEN toptopics7dtop20.Topic IS NOT NULL THEN 1 ELSE 0 END) AS IsDominantTopic7d 
  FROM toptopics2
    LEFT OUTER JOIN toptopics7dtop20 ON toptopics2.Topic = toptopics7dtop20.Topic
)
SELECT * FROM toptopics3' \
$DATASET_ID.vwtoptopic7d;


bq $1 --use_legacy_sql=false --view=\
'WITH
s1 AS (
  SELECT t.SnapshotDateId, reposts AS WebResourceHash, ARRAY_AGG(DISTINCT tags) AS Tags
  FROM `'$PROJECT_ID'.'$DATASET_ID'.vwtoptopic7d` t, UNNEST(t.RepostWebResourceHashes) reposts, UNNEST (t.Tags) tags
  WHERE t.IsTop20Topic = 1
  GROUP BY 1,2
),
s2 AS (
  SELECT s1.SnapshotDateId, s1.WebResourceHash, s1.Tags, si.Title, si.Url, si.cntDomains, si.cntFb,
    RANK() OVER (PARTITION BY s1.SnapshotDateId ORDER BY si.cntFb DESC, si.cntDomains DESC) AS rankSocial
  FROM s1
    INNER JOIN `'$PROJECT_ID'.'$DATASET_ID'.webresource` wr ON wr.WebResourceHash = s1.WebResourceHash
    INNER JOIN `'$PROJECT_ID'.'$DATASET_ID'.vwstoryimpact` si ON si.DocumentHash = wr.DocumentHash
),
s3 AS (
  SELECT SnapshotDateId, WebResourceHash, Title, Url, cntDomains, cntFb, rankSocial, IF(rankSocial <= 10, 1, 0) AS IsTop10Story, STRING_AGG(tags) AS Tags
  FROM s2, UNNEST(s2.Tags) tags
  GROUP BY 1,2,3,4,5,6,7
)
SELECT * FROM s3
ORDER BY SnapshotDateId ASC, rankSocial ASC' \
$DATASET_ID.vwtopstory7d;


bq $1 --use_legacy_sql=false --view=\
'SELECT 
  t.SnapshotDateId, t.TimeMarker, t.Topic, s.SentimentTotalScore, 
  (CASE DominantValence WHEN 1 THEN "Positive" WHEN 2 THEN "Negative" WHEN 3 THEN "Ambiguous" WHEN 5 THEN "General" ELSE "Unknown" END) AS DominantValence,
  s.Text,
  s.AnnotatedText,
  s.AnnotatedHtml,
  StAcceptance, StAnger, StAnticipation, StAmbiguous, StDisgust, StFear, StGuilt, StInterest, StJoy, StSadness, StShame, StSurprise, StPositive, StNegative, StSentiment, StProfane, StUnsafe
  ,wr.Title
  ,wr.Url
  ,wr.Domain
  ,wr.Author
FROM `'$PROJECT_ID'.'$DATASET_ID'.vwtoptopic7d` t
  INNER JOIN `'$PROJECT_ID'.'$DATASET_ID'.stattopic` st ON st.SnapshotDateId = t.SnapshotDateId AND st.Topic = t.Topic,
  UNNEST(st.SentimentHashes) AS sh
  INNER JOIN `'$PROJECT_ID'.'$DATASET_ID'.sentiment` s ON s.SentimentHash = sh
  INNER JOIN `'$PROJECT_ID'.'$DATASET_ID'.webresource` wr ON wr.DocumentHash = s.DocumentHash
WHERE t.IsTop20Topic = 1' \
$DATASET_ID.vwtoptopic7dsentiment;