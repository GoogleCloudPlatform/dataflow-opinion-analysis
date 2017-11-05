/*******************************************************************************
 * Copyright 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.google.cloud.dataflow.examples.opinionanalysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.Instant;

import CS2JNet.System.Collections.LCC.CSList;

/**
 *
 */
public class StatsCalcPipelineUtils {

    public static final String[] allStatsCalcTables = new String[]{ "stattopic", "statstoryimpact", "statdomainopinions", "statstoryrank", "stattoptopic7d", "stattopstory7d", "stattoptopic7dsentiment" };

	/*
	 * Interface for all Query Generators used to generate particular types of stats. 
	 * 
	 */
	public interface StatsQueryGenerator {
		Boolean isDailySnapshots();
		String buildDeleteQuery(String paramsString, String datasetName);
		String buildInsertQuery(String paramsString, String datasetName);
	}

	/*
	 * Query Generators for StatTopic table in BQ. 
	 * 
	 */

	public class StatTopicQueryGenerator implements StatsQueryGenerator {
		
		public Boolean isDailySnapshots(){
			return true;
		}
		
		public String buildDeleteQuery(String paramsString, String datasetName){
			String result = "DELETE FROM "+datasetName+".stattopic WHERE " + paramsString;
			return result;
		}
		
		public String buildInsertQuery(String paramsString, String datasetName) {
			
			String result =	
					
"INSERT INTO "+datasetName+".stattopic (SnapshotDateId, Topic, Tags, TagCount, cntOrigPublishers, cntRepostWRs,\n"+
"  cntPositives, cntNegatives, cntAmbiguous, cntGeneral, SentimentHashes, OrigWebResourceHashes, RepostWebResourceHashes )\n"+
"WITH \n"+
"p AS (\n"+
paramsString+
"),\n"+
"SentimentTags AS (\n"+
"  SELECT p.SnapshotDateId, s.SentimentHash, t.Tag, t.GoodAsTopic, s.Tags AS Tags\n"+
"  FROM p, "+datasetName+".sentiment s, UNNEST(s.Tags) AS t\n"+
"  WHERE\n"+
"    s.DocumentDateId = p.SnapshotDateId AND s.SentimentTotalScore > 0\n"+
"),\n"+
"SentimentTagCombos AS (\n"+
"  SELECT st.SnapshotDateId, st.SentimentHash, st.Tag AS Tag1, stt.Tag AS Tag2 \n"+
"  FROM SentimentTags st, UNNEST(st.Tags) stt\n"+
"  WHERE st.Tag < stt.Tag\n"+
"),\n"+
"CalcStatSentiments AS (\n"+
"  SELECT st.SnapshotDateId, st.Tag, st.GoodAsTopic, d.DocumentHash AS DocumentHash, s.SentimentHash,\n"+
"    wrOrig.WebResourceHash AS OrigWebResourceHash, wrOrig.Domain AS OrigDomain, wrRepost.WebResourceHash AS RepostWebResourceHash,\n"+
"    s.DominantValence AS Valence, d.PublicationTime AS PublicationTime\n"+
"  FROM SentimentTags st\n"+
"    INNER JOIN "+datasetName+".sentiment s ON s.SentimentHash = st.SentimentHash AND s.DocumentDateId = st.SnapshotDateId\n"+
"    INNER JOIN "+datasetName+".document d ON d.DocumentHash = s.DocumentHash AND d.PublicationDateId = st.SnapshotDateId\n"+
"    INNER JOIN "+datasetName+".webresource wrOrig ON wrOrig.WebResourceHash = d.MainWebResourceHash\n"+
"    INNER JOIN "+datasetName+".webresource wrRepost ON wrRepost.DocumentHash = d.DocumentHash\n"+
"),\n"+
"CalcStatTopics AS (\n"+
"  SELECT\n"+
"    c.SnapshotDateId, c.Tag AS Topic, [c.Tag] AS Tags, c.GoodAsTopic, 1 AS TagCount,\n"+
"    COUNT(distinct OrigDomain) as cntOrigPublishers,\n"+
"    COUNT(distinct RepostWebResourceHash) as cntRepostWRs,\n"+
"    COUNT(distinct (case when c.Valence=1 then c.SentimentHash else null end)) as cntPositives,\n"+
"    COUNT(distinct (case when c.Valence=2 then c.SentimentHash else null end)) as cntNegatives,\n"+
"    COUNT(distinct (case when c.Valence=3 then c.SentimentHash else null end)) as cntAmbiguous,\n"+
"    COUNT(distinct (case when c.Valence=5 then c.SentimentHash else null end)) as cntGeneral,\n"+
"    ARRAY_AGG(DISTINCT c.SentimentHash) AS SentimentHashes,\n"+
"    ARRAY_AGG(DISTINCT c.OrigWebResourceHash) AS OrigWebResourceHashes,\n"+
"    ARRAY_AGG(DISTINCT c.RepostWebResourceHash) AS RepostWebResourceHashes\n"+
"  FROM CalcStatSentiments c\n"+
"  GROUP BY c.SnapshotDateId, c.Tag, c.GoodAsTopic\n"+
"),\n"+
"CalcStatCombiTopics AS (\n"+
"  SELECT \n"+
"    stc.SnapshotDateId, CONCAT(stc.Tag1,' & ',stc.Tag2) AS Topic, [stc.Tag1,stc.Tag2] AS Tags, true AS GoodAsTopic, 2 AS TagCount,\n"+
"    COUNT(distinct wrOrig.Domain) as cntOrigPublishers,\n"+
"    COUNT(distinct wrRepost.WebResourceHash) as cntRepostWRs,\n"+
"    COUNT(distinct (case when s.DominantValence=1 then s.SentimentHash else null end)) as cntPositives,\n"+
"    COUNT(distinct (case when s.DominantValence=2 then s.SentimentHash else null end)) as cntNegatives,\n"+
"    COUNT(distinct (case when s.DominantValence=3 then s.SentimentHash else null end)) as cntAmbiguous,\n"+
"    COUNT(distinct (case when s.DominantValence=5 then s.SentimentHash else null end)) as cntGeneral,\n"+
"    ARRAY_AGG(DISTINCT s.SentimentHash) AS SentimentHashes,\n"+
"    ARRAY_AGG(DISTINCT wrOrig.WebResourceHash) AS OrigWebResourceHashes,\n"+
"    ARRAY_AGG(DISTINCT wrRepost.WebResourceHash) AS RepostWebResourceHashes\n"+
"  FROM SentimentTagCombos stc\n"+
"    INNER JOIN "+datasetName+".sentiment s ON s.SentimentHash = stc.SentimentHash AND s.DocumentDateId = stc.SnapshotDateId\n"+
"    INNER JOIN "+datasetName+".document d ON d.DocumentHash = s.DocumentHash AND d.PublicationDateId = stc.SnapshotDateId\n"+
"    INNER JOIN "+datasetName+".webresource wrOrig ON wrOrig.WebResourceHash = d.MainWebResourceHash\n"+
"    INNER JOIN "+datasetName+".webresource wrRepost ON wrRepost.DocumentHash = d.DocumentHash\n"+
"  GROUP BY stc.SnapshotDateId, stc.Tag1, stc.Tag2\n"+
"  -- HAVING cntPublisherDomains > 1\n"+
"),\n"+
"CalcStatAllTopics AS (\n"+
"  SELECT * FROM CalcStatTopics\n"+
"  WHERE GoodAsTopic = true AND cntRepostWRs > 1\n"+
"  UNION ALL\n"+
"  SELECT * FROM CalcStatCombiTopics\n"+
"  WHERE GoodAsTopic = true AND cntRepostWRs > 1\n"+
")\n"+
"SELECT SnapshotDateId, Topic, Tags, TagCount, cntOrigPublishers, cntRepostWRs,\n"+  
"  cntPositives, cntNegatives, cntAmbiguous, cntGeneral, SentimentHashes, OrigWebResourceHashes, RepostWebResourceHashes\n"+
"FROM CalcStatAllTopics"; 	

			return result;			
			
		}
	}


	
	/*
	 * Query Generators for StatStoryImpact table in BQ. 
	 * 
	 */

	public class StatStoryImpactQueryGenerator implements StatsQueryGenerator {
		public Boolean isDailySnapshots(){
			return true;
		}

		public String buildDeleteQuery(String paramsString, String datasetName){
			String result = "DELETE FROM "+datasetName+".statstoryimpact WHERE " + paramsString;
			return result;
		}
		
		public String buildInsertQuery(String paramsString, String datasetName) {
			
			String result =	
					
"INSERT INTO "+datasetName+".statstoryimpact (SnapshotDateId, DocumentHash, DocumentCollectionId, CollectionItemId,\n"+ 
"	cntWRs, cntDomains, cntFb, Domains, Urls)\n"+    
"WITH \n"+
"p AS (\n"+
paramsString+
"),\n"+
"p2 AS (\n"+
"  SELECT SnapshotDateId, CAST(FORMAT_DATE('%Y%m%d', DATE_SUB(PARSE_DATE('%Y%m%d', CAST(SnapshotDateId AS STRING)), INTERVAL 8 DAY) ) AS INT64) AS TimeWindowStart\n"+
"  FROM p\n"+
"),\n"+
"s1 AS (\n"+
"  SELECT\n"+ 
"    p2.SnapshotDateId, d.DocumentHash, d.DocumentCollectionId, d.CollectionItemId\n"+
"  FROM "+datasetName+".document d, p2\n"+
"  WHERE d.PublicationDateId <= p2.SnapshotDateId AND d.PublicationDateId >= p2.TimeWindowStart\n"+
"),\n"+
"s2 AS (\n"+
"  SELECT\n"+ 
"    s1.SnapshotDateId, s1.DocumentHash, s1.DocumentCollectionId, s1.CollectionItemId,\n"+ 
"    COUNT(DISTINCT wrRepost.WebResourceHash) AS cntWRs, COUNT(DISTINCT wrRepost.Domain) AS cntDomains,\n"+ 
"    ARRAY_AGG(wrRepost.Domain) AS Domains, ARRAY_AGG(wrRepost.Url) AS Urls\n"+
"  FROM s1\n"+
"    INNER JOIN "+datasetName+".webresource wrRepost ON\n"+ 
"      wrRepost.DocumentHash = s1.DocumentHash\n"+
"      AND wrRepost.PublicationDateId <= s1.SnapshotDateId -- only count WRs published up to Snapshot day\n"+
"  GROUP BY 1,2,3,4\n"+
"),\n"+
"s3 AS (\n"+
"  SELECT\n"+ 
"    s1.SnapshotDateId, s1.DocumentHash, s1.DocumentCollectionId, s1.CollectionItemId, wrRepost.WebResourceHash, MAX(sc.FbCount) AS cntFb\n"+
"  FROM s1\n"+
"    INNER JOIN "+datasetName+".webresource wrRepost ON\n"+ 
"      wrRepost.DocumentHash = s1.DocumentHash\n"+
"    LEFT OUTER JOIN "+datasetName+".wrsocialcount sc ON sc.WebResourceHash = wrRepost.WebResourceHash\n"+
"      AND CAST(FORMAT_DATE('%Y%m%d', CAST(sc.CountTime AS DATE)) AS INT64) <= s1.SnapshotDateId\n"+
"  GROUP BY 1,2,3,4,5\n"+
"),\n"+
"s4 AS (\n"+
"  SELECT\n"+ 
"    s3.SnapshotDateId, s3.DocumentHash, s3.DocumentCollectionId, s3.CollectionItemId,\n"+
"    SUM(s3.cntFb) AS cntFb\n"+ 
"  FROM s3\n"+
"  GROUP BY 1,2,3,4\n"+
")\n"+
"SELECT\n"+ 
"  s2.SnapshotDateId, s2.DocumentHash, s2.DocumentCollectionId, s2.CollectionItemId,\n"+ 
"  s2.cntWRs, s2.cntDomains, s4.cntFb,\n"+
"  s2.Domains, s2.Urls\n"+
"FROM s2\n"+
"  INNER JOIN s4 ON s4.DocumentHash = s2.DocumentHash\n"+
"ORDER BY 7 DESC\n";

			return result;			
			
		}
	}
	

	
	/*
	 * Query Generator for StatDomainOpinions table in BQ. 
	 * 
	 */

	public class StatDomainOpinionsQueryGenerator implements StatsQueryGenerator {
		public Boolean isDailySnapshots(){
			return false;
		}

		public String buildDeleteQuery(String paramsString, String datasetName){
			String result = "DELETE FROM "+datasetName+".statdomainopinions WHERE 1=1";
			return result;
		}
		
		public String buildInsertQuery(String paramsString, String datasetName) {
			
			String result =	
					
"INSERT INTO "+datasetName+".statdomainopinions (Topic, Domain, IsTop50Domain, IsTop500Domain, DomainTopicRank,\n"+
"  IsTop20DomainTopic, IsTop100DomainTopic, cntTotalMentions, Basic8Score, MeanBasic8Ratio, StdevBasic8Ratio, MeanStJoy, StdevStJoy,\n"+
"  MeanStAcceptance, StdevStAcceptance, MeanStFear, StdevStFear, MeanStSurprise, StdevStSurprise, MeanStSadness, StdevStSadness,\n"+
"  MeanStDisgust, StdevStDisgust, MeanStAnger, StdevStAnger, MeanStAnticipation, StdevStAnticipation, MeanSentimentRatio, StdevSentimentRatio\n"+
"  )\n"+
"WITH\n"+
"s1 AS (\n"+
"  SELECT wr.Domain, COUNT(DISTINCT DocumentHash) AS cntDocs\n"+
"  FROM "+datasetName+".webresource wr\n"+
"  GROUP BY 1 HAVING COUNT(DISTINCT DocumentHash) > 10\n"+
"),\n"+
"s1a AS (\n"+
"  SELECT \n"+
"    Domain, cntDocs,\n"+
"    RANK() OVER (ORDER BY cntDocs DESC) AS DomainRank\n"+
"  FROM s1\n"+
"),\n"+
"s2 AS (\n"+
"  SELECT \n"+
"    wr.PublicationTime AS PublicationTime,  \n"+
"    wr.Domain AS Domain,\n"+
"    wr.Author AS Author,\n"+
"    IF(s1a.DomainRank <= 50, 1, 0) AS IsTop50Domain,\n"+
"    IF(s1a.DomainRank <= 500, 1, 0) AS IsTop500Domain,\n"+
"    ARRAY(SELECT tg.Tag FROM UNNEST(s.Tags) AS tg WHERE tg.GoodAsTopic = TRUE) AS TopicArray,\n"+
"    s.SentimentTotalScore, \n"+
"    DominantValence AS Valence,\n"+
"    StAcceptance, StAnger, StAnticipation, StAmbiguous, StDisgust, StFear, StGuilt, StInterest, StJoy, StSadness, StShame, StSurprise, StPositive, StNegative, StSentiment, StProfane, StUnsafe\n"+
"  FROM "+datasetName+".sentiment s \n"+
"    INNER JOIN "+datasetName+".webresource wr ON wr.DocumentHash = s.DocumentHash -- correct join on DocumentHash\n"+
"    INNER JOIN s1a ON s1a.Domain = wr.Domain\n"+
"),\n"+
"s3 AS (\n"+
"  SELECT\n"+
"    FORMAT_TIMESTAMP('%Y_%U',PublicationTime) AS PubWeek,\n"+
"    Domain,\n"+
"    IsTop50Domain,\n"+
"    IsTop500Domain,\n"+
"    ta AS Topic,\n"+
"    SUM((case when Valence=1 then 1 else 0 end)) as cntPositives,\n"+
"    SUM((case when Valence=2 then 1 else 0 end)) as cntNegatives,\n"+
"    SUM((case when Valence=3 then 1 else 0 end)) as cntAmbiguous,\n"+
"    SUM((case when Valence=5 then 1 else 0 end)) as cntGeneral,\n"+
"    COUNT(1) AS cntTotalMentions,\n"+
"    SUM(StAcceptance) AS StAcceptance, \n"+
"    SUM(StAnger) AS StAnger, \n"+
"    SUM(StAnticipation) AS StAnticipation, \n"+
"    SUM(StAmbiguous) AS StAmbiguous, \n"+
"    SUM(StDisgust) AS StDisgust, \n"+
"    SUM(StFear) AS StFear, \n"+
"    SUM(StGuilt) AS StGuilt, \n"+
"    SUM(StInterest) AS StInterest, \n"+
"    SUM(StJoy) AS StJoy, \n"+
"    SUM(StSadness) AS StSadness, \n"+
"    SUM(StShame) AS StShame, \n"+
"    SUM(StSurprise) AS StSurprise, \n"+
"    SUM(StPositive) AS StPositive, \n"+
"    SUM(StNegative) AS StNegative, \n"+
"    SUM(StSentiment) AS StSentiment, \n"+
"    SUM(StProfane) AS StProfane, \n"+
"    SUM(StUnsafe) AS StUnsafe,\n"+
"    GREATEST(SUM(StJoy+StAcceptance+StFear+StSurprise+StSadness+StDisgust+StAnger+StAnticipation),0.01) AS Basic8Score,\n"+
"    GREATEST(SUM(StJoy+StAcceptance+StSurprise+StAnticipation),0.01) AS Positive4Score,\n"+
"    GREATEST(SUM(StFear+StSadness+StDisgust+StAnger),0.01) AS Negative4Score\n"+
"  FROM s2, UNNEST(s2.TopicArray) AS ta\n"+
"  WHERE ta NOT IN (SELECT tp.Topic FROM "+datasetName+".topic tp WHERE tp.IsBlocked=1)\n"+
"  GROUP BY 1,2,3,4,5\n"+
"),\n"+
"s4 AS (\n"+
"  SELECT\n"+
"    Domain,\n"+
"    Topic,\n"+
"    SUM(cntTotalMentions) AS cntTotalMentions\n"+
"  FROM s3\n"+
"  GROUP BY 1,2\n"+
"),\n"+
"s5 AS (\n"+
"  SELECT\n"+
"    Domain,\n"+
"    Topic,\n"+
"    cntTotalMentions,\n"+
"    RANK() OVER (PARTITION BY Domain ORDER BY cntTotalMentions DESC, LENGTH(Topic) DESC ) AS DomainTopicRank\n"+
"  FROM s4\n"+
"),\n"+
"s6 AS (\n"+
"  SELECT\n"+
"    s3.PubWeek,\n"+
"    s3.Domain,\n"+
"    s3.IsTop50Domain,\n"+
"    s3.IsTop500Domain,\n"+
"    s3.Topic,\n"+
"    s5.DomainTopicRank,\n"+
"    IF(s5.DomainTopicRank <= 20, 1, 0) AS IsTop20DomainTopic,\n"+
"    IF(s5.DomainTopicRank <= 100, 1, 0) AS IsTop100DomainTopic,\n"+
"    cntPositives,\n"+
"    cntNegatives,\n"+
"    cntAmbiguous,\n"+
"    cntGeneral,\n"+
"    s3.cntTotalMentions,\n"+
"    s3.Basic8Score,\n"+
"    (cntPositives - cntNegatives)/GREATEST(s3.cntTotalMentions,1) AS SentimentRatio,\n"+
"    ROUND(((Positive4Score - Negative4Score) / Basic8Score ),2) AS Basic8Ratio,\n"+
"    -- 8 basic emotions --\n"+
"    ROUND(StJoy/Basic8Score,2) AS StJoy,\n"+
"    ROUND(StAcceptance/Basic8Score,2) AS StAcceptance,\n"+
"    ROUND(StFear/Basic8Score,2) AS StFear,\n"+
"    ROUND(StSurprise/Basic8Score,2) AS StSurprise,\n"+
"    ROUND(StSadness/Basic8Score,2) AS StSadness,\n"+
"    ROUND(StDisgust/Basic8Score,2) AS StDisgust,\n"+
"    ROUND(StAnger/Basic8Score,2) AS StAnger,\n"+
"    ROUND(StAnticipation/Basic8Score,2) AS StAnticipation    \n"+
"  FROM s3\n"+
"    INNER JOIN s5 ON s5.Domain = s3.Domain AND s5.Topic = s3.Topic\n"+
"),\n"+
"s7 AS (\n"+
"  SELECT\n"+
"    Topic,\n"+
"    Domain,\n"+
"\n"+
"    MAX(IsTop50Domain) AS IsTop50Domain,\n"+
"    MAX(IsTop500Domain) AS IsTop500Domain,\n"+
"\n"+
"    MIN(s6.DomainTopicRank) AS DomainTopicRank,\n"+
"    MAX(IsTop20DomainTopic) AS IsTop20DomainTopic,\n"+
"    MAX(IsTop100DomainTopic) AS IsTop100DomainTopic,\n"+
"\n"+
"    SUM(cntTotalMentions) AS cntTotalMentions,\n"+
"    ROUND(SUM(Basic8Score),2) AS Basic8Score,\n"+
"\n"+
"    ROUND(AVG(Basic8Ratio),2) AS MeanBasic8Ratio,\n"+
"    ROUND(STDDEV_POP(Basic8Ratio),2) AS StdevBasic8Ratio,\n"+
"\n"+
"    ROUND(AVG(StJoy),2) AS MeanStJoy,\n"+
"    ROUND(STDDEV_POP(StJoy),2) AS StdevStJoy,\n"+
"\n"+
"    ROUND(AVG(StAcceptance),2) AS MeanStAcceptance,\n"+
"    ROUND(STDDEV_POP(StAcceptance),2) AS StdevStAcceptance,\n"+
"\n"+
"    ROUND(AVG(StFear),2) AS MeanStFear,\n"+
"    ROUND(STDDEV_POP(StFear),2) AS StdevStFear,\n"+
"\n"+
"    ROUND(AVG(StSurprise),2) AS MeanStSurprise,\n"+
"    ROUND(STDDEV_POP(StSurprise),2) AS StdevStSurprise,\n"+
"\n"+
"    ROUND(AVG(StSadness),2) AS MeanStSadness,\n"+
"    ROUND(STDDEV_POP(StSadness),2) AS StdevStSadness,\n"+
"\n"+
"    ROUND(AVG(StDisgust),2) AS MeanStDisgust,\n"+
"    ROUND(STDDEV_POP(StDisgust),2) AS StdevStDisgust,\n"+
"\n"+
"    ROUND(AVG(StAnger),2) AS MeanStAnger,\n"+
"    ROUND(STDDEV_POP(StAnger),2) AS StdevStAnger,\n"+
"\n"+
"    ROUND(AVG(StAnticipation),2) AS MeanStAnticipation,\n"+
"    ROUND(STDDEV_POP(StAnticipation),2) AS StdevStAnticipation,\n"+
"    \n"+
"    ROUND(AVG(SentimentRatio),2) AS MeanSentimentRatio,\n"+
"    ROUND(STDDEV_POP(SentimentRatio),2) AS StdevSentimentRatio\n"+
"       \n"+
"  FROM s6\n"+
"  GROUP BY 1,2\n"+
")\n"+
"SELECT * \n"+
"FROM s7\n"+
"WHERE s7.Basic8Score >= 30\n"; 	

			return result;			
			
		}
	}	
	
	/*
	 * Query Generator for StatStoryRank table in BQ. 
	 * 
	 */

	public class StatStoryRankQueryGenerator implements StatsQueryGenerator {
		public Boolean isDailySnapshots(){
			return false;
		}

		public String buildDeleteQuery(String paramsString, String datasetName){
			String result = "DELETE FROM "+datasetName+".statstoryrank WHERE 1=1";
			return result;
		}
		
		public String buildInsertQuery(String paramsString, String datasetName) {
			
			String result =	
					
"INSERT INTO "+datasetName+".statstoryrank (DocumentHash,PublicationDateId,PubMonth,PubWeek,Title,Url,Domain,Author,rankWeekly,rankMonthly,\n"+
"  cntWRs,cntDomains,cntFb,Domains,Urls\n"+
"  )\n"+
"WITH\n"+
"s1 AS (\n"+
"  SELECT\n"+
"    d.PublicationDateId,\n"+
"    d.DocumentHash,\n"+
"    FORMAT_TIMESTAMP('%Y_%m',d.PublicationTime) AS PubMonth,\n"+
"    FORMAT_TIMESTAMP('%Y_%U',d.PublicationTime) AS PubWeek,\n"+
"    SUBSTR(wrOrig.Title,0,1000) AS Title,\n"+
"    wrOrig.Url,\n"+
"    wrOrig.Domain,\n"+
"    wrOrig.Author\n"+
"  FROM "+datasetName+".document d\n"+
"    INNER JOIN "+datasetName+".webresource wrOrig ON wrOrig.WebResourceHash = d.MainWebResourceHash\n"+
"  WHERE \n"+
"    CAST(d.PublicationTime AS DATE) >= DATE_SUB((SELECT v.DateAsDate FROM "+datasetName+".vwlast7days v WHERE v.TimeMarker = 'T-1'), INTERVAL 45 DAY)\n"+
"),\n"+
"s2 AS (\n"+
"  SELECT s1.DocumentHash, MAX(impact.SnapshotDateId) AS LatestStatsDateId\n"+
"  FROM s1\n"+
"    INNER JOIN "+datasetName+".statstoryimpact impact ON impact.DocumentHash = s1.DocumentHash\n"+
"  GROUP BY s1.DocumentHash\n"+
"  ORDER BY MAX(impact.cntFb + impact.cntWRs) DESC\n"+
"  LIMIT 100000"+ 
"),\n"+
"s3 AS (\n"+
"  SELECT\n"+
"    s1.DocumentHash,\n"+
"    s1.PublicationDateId,\n"+
"    s1.PubMonth,\n"+
"    s1.PubWeek,\n"+
"    s1.Title,\n"+
"    s1.Url,\n"+
"    s1.Domain,\n"+
"    s1.Author,\n"+
"    RANK() OVER (PARTITION BY s1.PubWeek ORDER BY impact.cntFb DESC, impact.cntWRs DESC) AS rankWeekly,\n"+
"    RANK() OVER (PARTITION BY s1.PubMonth ORDER BY impact.cntFb DESC, impact.cntWRs DESC) AS rankMonthly,\n"+
"    impact.cntWRs, \n"+
"    impact.cntDomains, \n"+
"    impact.cntFb, \n"+
"    ARRAY_TO_STRING(impact.Domains,',') AS Domains, \n"+
"    ARRAY_TO_STRING(impact.Urls,',') AS Urls\n"+
"  FROM s1\n"+
"    INNER JOIN s2 ON s2.DocumentHash = s1.DocumentHash\n"+
"    INNER JOIN "+datasetName+".statstoryimpact impact ON impact.DocumentHash = s2.DocumentHash AND impact.SnapshotDateId = s2.LatestStatsDateId\n"+
")\n"+
"SELECT * FROM s3\n"; 	

			return result;			
			
		}
	}		
	
	/*
	 * Query Generator for StatTopTopic7d table in BQ. 
	 * 
	 */

	public class StatTopTopic7dQueryGenerator implements StatsQueryGenerator {
		public Boolean isDailySnapshots(){
			return false;
		}
		
		public String buildDeleteQuery(String paramsString, String datasetName){
			String result = "DELETE FROM "+datasetName+".stattoptopic7d WHERE 1=1";
			return result;
		}
		
		public String buildInsertQuery(String paramsString, String datasetName) {
			
			String result =	
					
"INSERT INTO "+datasetName+".stattoptopic7d (SnapshotDateId,Topic,Tags,TagCount,cntOrigPublishers,cntRepostWRs,cntPositives,\n"+
"  cntNegatives,cntAmbiguous,cntGeneral,OrigWebResourceHashes,RepostWebResourceHashes,SentimentRatio,rankPubdomains,\n"+
"  TimeMarker,IsTop20Topic,IsDominantTopic7d\n"+
"  )\n"+
"WITH \n"+
"toptopics1 AS (\n"+
"  SELECT st.* EXCEPT (SentimentHashes), \n"+
"    (cntPositives - cntNegatives)/GREATEST(cntPositives + cntNegatives + cntAmbiguous + cntGeneral,1) AS SentimentRatio,\n"+
"    RANK() OVER (PARTITION BY st.SnapshotDateId ORDER BY cntOrigPublishers DESC, cntRepostWRs DESC, TagCount DESC, Topic) AS rankPubdomains,\n"+
"    last7days.TimeMarker\n"+
"  FROM "+datasetName+".stattopic st\n"+
"    INNER JOIN "+datasetName+".vwlast7days last7days ON st.SnapshotDateId = last7days.DateId\n"+
"  WHERE st.Topic NOT IN (SELECT tp.Topic FROM "+datasetName+".topic tp WHERE tp.IsBlocked=1)\n"+
"  ORDER BY st.SnapshotDateId, rankPubdomains\n"+
"),\n"+
"toptopics2 AS ( \n"+
"  SELECT toptopics1.*, (CASE WHEN toptopics1.rankPubdomains <= 20 THEN 1 ELSE 0 END) AS IsTop20Topic\n"+
"  FROM toptopics1\n"+
"  WHERE toptopics1.rankPubdomains < 10000\n"+
"),\n"+
"toptopics7d AS (\n"+
"  SELECT \n"+
"    toptopics2.Topic,\n"+
"    MIN(CASE WHEN IsTop20Topic = 1 THEN TimeMarker ELSE NULL END) AS LatestTimeMarker,\n"+
"    SUM(IsTop20Topic) AS NumInTop20,\n"+
"    AVG(1/LN(rankPubdomains+1)) AS AvgTopicDominance,\n"+
"    (SUM(IsTop20Topic) * AVG(1/LN(rankPubdomains+1))) AS CycleTopicDominance\n"+
"  FROM toptopics2\n"+
"  GROUP BY toptopics2.Topic\n"+
"),\n"+
"toptopics7dtop20 AS (\n"+
"  SELECT * FROM toptopics7d ORDER BY CycleTopicDominance DESC LIMIT 20\n"+
"),\n"+
"toptopics3 AS (\n"+
"  SELECT toptopics2.*, (CASE WHEN toptopics7dtop20.Topic IS NOT NULL THEN 1 ELSE 0 END) AS IsDominantTopic7d \n"+
"  FROM toptopics2\n"+
"    LEFT OUTER JOIN toptopics7dtop20 ON toptopics2.Topic = toptopics7dtop20.Topic\n"+
")\n"+
"SELECT * FROM toptopics3 t WHERE t.rankPubdomains < 100 OR t.IsTop20Topic = 1 OR t.IsDominantTopic7d = 1\n";
			return result;			
			
		}
	}		

	
	/*
	 * Query Generator for StatTopStory7d table in BQ. 
	 * 
	 */

	public class StatTopStory7dQueryGenerator implements StatsQueryGenerator {
		public Boolean isDailySnapshots(){
			return false;
		}
		
		public String buildDeleteQuery(String paramsString, String datasetName){
			String result = "DELETE FROM "+datasetName+".stattopstory7d WHERE 1=1";
			return result;
		}
		
		public String buildInsertQuery(String paramsString, String datasetName) {
			
			String result =	
					
"INSERT INTO "+datasetName+".stattopstory7d (SnapshotDateId,WebResourceHash,Title,Url,cntDomains,cntFb,rankSocial,IsTop10Story,Tags\n"+
"  )\n"+
"WITH\n"+
"s1 AS (\n"+
"  SELECT t.SnapshotDateId, origs AS WebResourceHash, ARRAY_AGG(DISTINCT tags) AS Tags\n"+
"  FROM "+datasetName+".stattoptopic7d t, UNNEST(t.OrigWebResourceHashes) origs, UNNEST (t.Tags) tags\n"+
"  WHERE t.IsTop20Topic = 1\n"+
"  GROUP BY 1,2\n"+
"),\n"+
"s2 AS (\n"+
"  SELECT s1.SnapshotDateId, s1.WebResourceHash, s1.Tags, si.Title, si.Url, si.cntDomains, si.cntFb,\n"+
"    RANK() OVER (PARTITION BY s1.SnapshotDateId ORDER BY si.cntFb DESC, si.cntDomains DESC) AS rankSocial\n"+
"  FROM s1\n"+
"    INNER JOIN "+datasetName+".webresource wr ON wr.WebResourceHash = s1.WebResourceHash\n"+
"    INNER JOIN "+datasetName+".statstoryrank si ON si.DocumentHash = wr.DocumentHash\n"+
"),\n"+
"s3 AS (\n"+
"  SELECT SnapshotDateId, WebResourceHash, SUBSTR(Title,0,1000) AS Title, Url, cntDomains, cntFb, rankSocial, IF(rankSocial <= 10, 1, 0) AS IsTop10Story, STRING_AGG(tags) AS Tags\n"+
"  FROM s2, UNNEST(s2.Tags) tags\n"+
"  GROUP BY 1,2,3,4,5,6,7\n"+
")\n"+
"SELECT * FROM s3\n";
			return result;			
			
		}
	}		

	
	/*
	 * Query Generator for StatTopTopic7dSentiment table in BQ. 
	 * 
	 */

	public class StatTopTopic7dSentimentQueryGenerator implements StatsQueryGenerator {
		public Boolean isDailySnapshots(){
			return false;
		}
		
		public String buildDeleteQuery(String paramsString, String datasetName){
			String result = "DELETE FROM "+datasetName+".stattoptopic7dsentiment WHERE 1=1";
			return result;
		}
		
		public String buildInsertQuery(String paramsString, String datasetName) {
			
			String result =	
					
"INSERT INTO "+datasetName+".stattoptopic7dsentiment (SnapshotDateId,TimeMarker,Topic,SentimentTotalScore,DominantValence,Text,AnnotatedText,\n"+
"  AnnotatedHtml,StAcceptance,StAnger,StAnticipation,StAmbiguous,StDisgust,StFear,StGuilt,StInterest,StJoy,\n"+
"  StSadness,StShame,StSurprise,StPositive,StNegative,StSentiment,StProfane,StUnsafe,Title,Url,Domain,Author,IsTop20Topic,IsDominantTopic7d\n"+
"  )\n"+
"SELECT \n"+
"  t.SnapshotDateId, t.TimeMarker, t.Topic, s.SentimentTotalScore, \n"+
"  (CASE DominantValence WHEN 1 THEN 'Positive' WHEN 2 THEN 'Negative' WHEN 3 THEN 'Ambiguous' WHEN 5 THEN 'General' ELSE 'Unknown' END) AS DominantValence,\n"+
"  s.Text,\n"+
"  s.AnnotatedText,\n"+
"  s.AnnotatedHtml,\n"+
"  StAcceptance, StAnger, StAnticipation, StAmbiguous, StDisgust, StFear, StGuilt, StInterest, StJoy, StSadness, StShame, StSurprise, StPositive, StNegative, StSentiment, StProfane, StUnsafe\n"+
"  ,SUBSTR(wr.Title,0,1000) AS Title\n"+
"  ,wr.Url\n"+
"  ,wr.Domain\n"+
"  ,wr.Author\n"+
"  ,t.IsTop20Topic\n"+
"  ,t.IsDominantTopic7d\n"+
"FROM "+datasetName+".stattoptopic7d t\n"+
"  INNER JOIN "+datasetName+".stattopic st ON st.SnapshotDateId = t.SnapshotDateId AND st.Topic = t.Topic,\n"+
"  UNNEST(st.SentimentHashes) AS sh\n"+
"  INNER JOIN "+datasetName+".sentiment s ON s.SentimentHash = sh\n"+
"  INNER JOIN "+datasetName+".webresource wr ON wr.WebResourceHash = s.MainWebResourceHash;";
			return result;			
			
		}
	}		
	
	
	private static final StatsCalcPipelineUtils instance = new StatsCalcPipelineUtils();
	private static final StatTopicQueryGenerator statTopicQueryGenerator = instance.new StatTopicQueryGenerator();
	private static final StatStoryImpactQueryGenerator statStoryImpactQueryGenerator = instance.new StatStoryImpactQueryGenerator();

	private static final StatDomainOpinionsQueryGenerator statDomainOpinionsQueryGenerator = instance.new StatDomainOpinionsQueryGenerator();
	private static final StatStoryRankQueryGenerator statStoryRankQueryGenerator = instance.new StatStoryRankQueryGenerator();
	private static final StatTopTopic7dQueryGenerator statTopTopic7dQueryGenerator = instance.new StatTopTopic7dQueryGenerator();
	private static final StatTopStory7dQueryGenerator statTopStory7dQueryGenerator = instance.new StatTopStory7dQueryGenerator();
	private static final StatTopTopic7dSentimentQueryGenerator statTopTopic7dSentimentQueryGenerator = instance.new StatTopTopic7dSentimentQueryGenerator();
	
	
	public static StatsCalcPipelineUtils getInstance() {
		return instance;
	}

	public static StatTopicQueryGenerator getStatTopicQueryGenerator() {
		return statTopicQueryGenerator;
	}

	public static StatStoryImpactQueryGenerator getStatStoryImpactQueryGenerator() {
		return statStoryImpactQueryGenerator;
	}

	public static StatDomainOpinionsQueryGenerator getStatDomainOpinionsQueryGenerator() {
		return statDomainOpinionsQueryGenerator;
	}
	
	public static StatStoryRankQueryGenerator getStatStoryRankQueryGenerator() {
		return statStoryRankQueryGenerator;
	}
	
	public static StatTopTopic7dQueryGenerator getStatTopTopic7dQueryGenerator() {
		return statTopTopic7dQueryGenerator;
	}
	
	public static StatTopStory7dQueryGenerator getStatTopStory7dQueryGenerator() {
		return statTopStory7dQueryGenerator;
	}

	public static StatTopTopic7dSentimentQueryGenerator getStatTopTopic7dSentimentQueryGenerator() {
		return statTopTopic7dSentimentQueryGenerator;
	}

	public static void validateStatsCalcPipelineOptions(IndexerPipelineOptions options) throws Exception {
		
		
		if (options.getStatsCalcDays() == null &&
			(options.getStatsCalcFromDate() == null || options.getStatsCalcFromDate().isEmpty()) && 
			(options.getStatsCalcToDate() == null || options.getStatsCalcToDate().isEmpty()))
			throw new IllegalArgumentException("One of the FromDate, ToDate, or Days parameters needs to be set.");

		if (!(options.getStatsCalcFromDate() == null || options.getStatsCalcFromDate().isEmpty())){
			Long l = IndexerPipelineUtils.parseDateToLong(options.getStatsCalcFromDate());
			if (l==null)
				throw new IllegalArgumentException("Invalid value of the FromDate parameter: " + options.getStatsCalcFromDate());
		}

		if (!(options.getStatsCalcToDate() == null || options.getStatsCalcToDate().isEmpty())){
			Long l = IndexerPipelineUtils.parseDateToLong(options.getStatsCalcToDate());
			if (l==null)
				throw new IllegalArgumentException("Invalid value of the ToDate parameter: " + options.getStatsCalcToDate());
		}
		
		if (options.getStatsCalcDays() != null) {
			for (int i=0; i < options.getStatsCalcDays().length; i++) {
				String daylabel = options.getStatsCalcDays()[i];
				String regex = "T-[0-9]*";
				if (!daylabel.matches(regex))
					throw new IllegalArgumentException("Invalid value of a Day label: " + daylabel + ". Needs to be in the format T-1, T-2, etc.");
			}
		}
		
		if ( options.getBigQueryDataset().isEmpty()) {
			throw new IllegalArgumentException("Sink BigQuery dataset needs to be specified.");
		}

		if (options.getStatsCalcTables() != null) {
			List<String> allTablesList = Arrays.asList(allStatsCalcTables);
			for (int i=0; i < options.getStatsCalcTables().length; i++) {
				String table = options.getStatsCalcTables()[i];
				if (!allTablesList.contains(table))
					throw new IllegalArgumentException("Invalid table name: " + table + ".");
			}
		}
		
		
	}
	
	public static String[] buildStatsCalcQueries(String[] statsCalcDays, String sFromDate, String sToDate, 
			String bigQueryDataset, StatsQueryGenerator gen) throws Exception {
		if (gen.isDailySnapshots())
			return buildDailyStatsCalcQueries(statsCalcDays,  sFromDate,  sToDate, bigQueryDataset, gen);
		else 
			return buildLatestStatsCalcQueries(bigQueryDataset, gen);
	}
	
	private static String[] buildLatestStatsCalcQueries(String bigQueryDataset, StatsQueryGenerator gen) throws Exception {

		ArrayList<String> querySequenceList = new ArrayList<String>();
		String query;

		// Create single DELETE operation
		query = gen.buildDeleteQuery("",bigQueryDataset);
		querySequenceList.add(query);
			
		// Create single INSERT operation
		query = gen.buildInsertQuery("",bigQueryDataset);
		querySequenceList.add(query);
		
		String[] querySequence = new String[querySequenceList.size()];
		querySequenceList.toArray(querySequence);

		return querySequence;
	}	
	
	
	private static String[] buildDailyStatsCalcQueries(String[] statsCalcDays, String sFromDate, String sToDate, 
			String bigQueryDataset, StatsQueryGenerator gen) throws Exception {

		ArrayList<String> querySequenceList = new ArrayList<String>();
		String query;
		
		if (statsCalcDays!= null) {
			
			String timeMarkerList = StatsCalcPipelineUtils.buildTimeMarkerList(statsCalcDays);

			// DELETE the previous snapshots, if they exist, in one DELETE operation, to save resources
			query = buildDeleteByTimeMarkerQuery(timeMarkerList,bigQueryDataset,gen);
			querySequenceList.add(query);
			
			// Create INSERT operations for all days individually
			for (int i=0; i < statsCalcDays.length; i++) {
				String daylabel = statsCalcDays[i];
				
				// Insert the records of the new daily snapshot
				query = buildInsertByTimeMarkerQuery(daylabel,bigQueryDataset,gen);
				querySequenceList.add(query);
			
			}
			
			
		} else if (sFromDate != null) {
			
			Long lFromDate = IndexerPipelineUtils.parseDateToLong(sFromDate);
			Instant fromDate = new Instant(lFromDate);
			
			Long lToDate = null;
			if (sToDate != null)
				lToDate = IndexerPipelineUtils.parseDateToLong(sToDate);
			
			Instant toDate = null; 
			if (lToDate != null)
				toDate = new Instant(lToDate);
			else
				toDate = Instant.now();
			
			ArrayList<Integer> snapshotDateIds = new ArrayList<Integer>();
			
			Instant t = fromDate;
			while (t.getMillis() <= toDate.getMillis()) {
				Integer dateId = IndexerPipelineUtils.getDateIdFromTimestamp(t.getMillis());
				snapshotDateIds.add(dateId);
				t = t.plus(24L * 3600L * 1000L); //add one day
			}
			
			// Add the DELETE statement for previous snapshots in the range
			Integer[] snapshotIdArray = new Integer[(snapshotDateIds.size())];
			snapshotDateIds.toArray(snapshotIdArray);
			String snapshotIdList = buildIntegerList(snapshotIdArray);
			query = buildDeleteByDateIdQuery(snapshotIdList,bigQueryDataset,gen);
			querySequenceList.add(query);
			
			for (int i=0; i < snapshotDateIds.size(); i++) {
				
				Integer dateId = snapshotDateIds.get(i);
				
				// Add the INSERTs for new daily snapshot
				query = buildInsertByDateIdQuery(dateId,bigQueryDataset,gen);
				querySequenceList.add(query);
							
			}

		} else {
			throw new Exception("Unsufficient time parameters to calculate stats.");
		}
		
		String[] querySequence = new String[querySequenceList.size()];
		querySequenceList.toArray(querySequence);

		return querySequence;
	}	
	
	
	
	public static String buildTimeMarkerList(String[] markerArray){
		if (markerArray == null)
			return null;
		
		String result = "";
		
		for (int i=0; i < markerArray.length; i++) {
			if (!result.isEmpty()) result += ",";
			result += "'"+markerArray[i]+"'";
		}
		return result;

	}

	public static String buildIntegerList(Integer[] integerArray){
		if (integerArray == null)
			return null;
		
		
		String result = "";
		
		for (int i=0; i < integerArray.length; i++) {
			if (!result.isEmpty()) result += ",";
			result += integerArray[i];
		}

		return result;

	}

	public static String buildDeleteByTimeMarkerQuery(String timeMarkerList, String datasetName, StatsQueryGenerator gen) {
		String paramsString = " SnapshotDateId IN (SELECT DateId AS SnapshotDateId FROM "+datasetName+".vwlast7days WHERE TimeMarker IN ("+timeMarkerList+"))\n";
		String result = gen.buildDeleteQuery(paramsString,datasetName);
		return result;
	}

	public static String buildDeleteByDateIdQuery(String snapshotIdList, String datasetName,StatsQueryGenerator gen) {
		String paramsString = " SnapshotDateId IN ("+snapshotIdList+")\n";
		String result = gen.buildDeleteQuery(paramsString,datasetName);
		return result;
	}
	
	

	public static String buildInsertByTimeMarkerQuery(String timeMarker, String datasetName,StatsQueryGenerator gen) {
		String paramsString = " SELECT DateId AS SnapshotDateId FROM "+datasetName+".vwlast7days WHERE TimeMarker = '"+timeMarker+"'\n";
		String result = gen.buildInsertQuery(paramsString,datasetName);
		return result;
	}

	public static String buildInsertByDateIdQuery(Integer snapshotId, String datasetName, StatsQueryGenerator gen) {
		String paramsString = " SELECT "+snapshotId+" AS SnapshotDateId\n";
		String result = gen.buildInsertQuery(paramsString,datasetName);
		return result;
	}
	
	
}
