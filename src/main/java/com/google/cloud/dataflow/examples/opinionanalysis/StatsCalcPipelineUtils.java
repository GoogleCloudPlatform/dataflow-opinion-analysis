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
import org.joda.time.Instant;

/**
 *
 */
public class StatsCalcPipelineUtils {

	//public static final String STATTOPIC_TABLE = "stattopic";
	

	/*
	 * Interface for all Query Generators used to generate particular types of stats. 
	 * 
	 */
	public interface StatsQueryGenerator {
		String buildDeleteQuery(String paramsString, String datasetName);
		String buildInsertQuery(String paramsString, String datasetName);
	}

	/*
	 * Query Generators for StatTopic table in BQ. 
	 * 
	 */

	public class StatTopicQueryGenerator implements StatsQueryGenerator {
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
"CalcStatSentiments AS (\n"+
"  SELECT p.SnapshotDateId, t.Tag, t.GoodAsTopic, d.DocumentHash AS DocumentHash, s.SentimentHash,\n"+
"    wrOrig.WebResourceHash AS OrigWebResourceHash, wrOrig.Domain AS OrigDomain, wrRepost.WebResourceHash AS RepostWebResourceHash,\n"+
"    s.DominantValence AS Valence, d.PublicationTime AS PublicationTime\n"+
"  FROM "+datasetName+".document d, p\n"+
"    INNER JOIN "+datasetName+".sentiment s ON s.DocumentHash = d.DocumentHash, UNNEST(s.Tags) AS t\n"+
"    -- Need to use Sentiment tags, so that sentiments relate to topics\n"+  
"    INNER JOIN "+datasetName+".webresource wrOrig ON wrOrig.DocumentHash = d.DocumentHash\n"+
"    INNER JOIN "+datasetName+".webresource wrRepost ON wrRepost.DocumentCollectionId = d.DocumentCollectionId\n"+
"      AND wrRepost.CollectionItemId = d.CollectionItemId\n"+
"  WHERE\n"+
"    d.PublicationDateId = p.SnapshotDateId AND s.SentimentTotalScore > 0\n"+
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
"    css1.SnapshotDateId, CONCAT(css1.Tag,' & ',css2.Tag) AS Topic, [css1.Tag,css2.Tag] AS Tags, true AS GoodAsTopic, 2 AS TagCount,\n"+
"    COUNT(distinct css1.OrigDomain) as cntOrigPublishers,\n"+
"    COUNT(distinct css1.RepostWebResourceHash) as cntRepostWRs,\n"+ 
"    COUNT(distinct (case when css1.Valence=1 then css1.SentimentHash else null end)) as cntPositives,\n"+
"    COUNT(distinct (case when css1.Valence=2 then css1.SentimentHash else null end)) as cntNegatives,\n"+
"    COUNT(distinct (case when css1.Valence=3 then css1.SentimentHash else null end)) as cntAmbiguous,\n"+
"    COUNT(distinct (case when css1.Valence=5 then css1.SentimentHash else null end)) as cntGeneral,\n"+
"    ARRAY_AGG(DISTINCT css1.SentimentHash) AS SentimentHashes,\n"+
"    ARRAY_AGG(DISTINCT css1.OrigWebResourceHash) AS OrigWebResourceHashes,\n"+
"    ARRAY_AGG(DISTINCT css1.RepostWebResourceHash) AS RepostWebResourceHashes\n"+
"  FROM\n"+
"    CalcStatSentiments css1, CalcStatSentiments css2\n"+
"  WHERE\n"+
"    css1.SentimentHash = css2.SentimentHash AND\n"+
"    css1.Tag < css2.Tag\n"+
"  GROUP BY css1.SnapshotDateId, css1.Tag, css2.Tag\n"+
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
	 * Query Generators for StatTopic table in BQ. 
	 * 
	 */

	public class StatStoryImpactQueryGenerator implements StatsQueryGenerator {
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
"  SELECT SnapshotDateId, CAST(FORMAT_DATE('%Y%m%d', DATE_SUB(PARSE_DATE('%Y%m%d', CAST(SnapshotDateId AS STRING)), INTERVAL 30 DAY) ) AS INT64) AS TimeWindowStart\n"+
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
"      wrRepost.DocumentCollectionId = s1.DocumentCollectionId AND wrRepost.CollectionItemId = s1.CollectionItemId\n"+
"      AND wrRepost.PublicationDateId <= s1.SnapshotDateId -- only count WRs published up to Snapshot day\n"+
"  GROUP BY 1,2,3,4\n"+
"),\n"+
"s3 AS (\n"+
"  SELECT\n"+ 
"    s1.SnapshotDateId, s1.DocumentHash, s1.DocumentCollectionId, s1.CollectionItemId, wrRepost.WebResourceHash, MAX(sc.FbCount) AS cntFb\n"+
"  FROM s1\n"+
"    INNER JOIN "+datasetName+".webresource wrRepost ON\n"+ 
"      wrRepost.DocumentCollectionId = s1.DocumentCollectionId AND wrRepost.CollectionItemId = s1.CollectionItemId\n"+
"    INNER JOIN "+datasetName+".wrsocialcount sc ON sc.WebResourceHash = wrRepost.WebResourceHash\n"+
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
	
	
	
	private static final StatsCalcPipelineUtils instance = new StatsCalcPipelineUtils();
	private static final StatTopicQueryGenerator statTopicQueryGenerator = instance.new StatTopicQueryGenerator();
	private static final StatStoryImpactQueryGenerator statStoryImpactQueryGenerator = instance.new StatStoryImpactQueryGenerator();
	
	public static StatsCalcPipelineUtils getInstance() {
		return instance;
	}

	public static StatTopicQueryGenerator getStatTopicQueryGenerator() {
		return statTopicQueryGenerator;
	}

	public static StatStoryImpactQueryGenerator getStatStoryImpactQueryGenerator() {
		return statStoryImpactQueryGenerator;
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
		
		
	}
	
	
	public static String[] buildStatsCalcQueries(String[] statsCalcDays, String sFromDate, String sToDate, 
			String bigQueryDataset, StatsQueryGenerator gen) throws Exception {

		ArrayList<String> querySequenceList = new ArrayList<String>();
		String query;
		
		if (statsCalcDays!= null) {
			
			String timeMarkerList = StatsCalcPipelineUtils.buildTimeMarkerList(statsCalcDays);

			// DELETE the previous snapshots, if they exist, in one DELETE operation, to save resources
			query = buildDeleteByTimeMarkerQuery(timeMarkerList,bigQueryDataset,gen);;
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
