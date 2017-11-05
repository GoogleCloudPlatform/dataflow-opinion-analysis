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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResponse;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipeline.LogPipelineOptions;

public class StatsCalcPipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(StatsCalcPipeline.class);
	
	public static void main(String[] args) throws Exception {
		
		IndexerPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IndexerPipelineOptions.class);

	    Pipeline pipeline = createStatsCalcPipeline(options); 
		
		pipeline.run();

	}

	/**
	 * This function creates the DAG graph of transforms. It can be called from main()
	 * as well as from the ControlPipeline.
	 * @param options
	 * @return
	 * @throws Exception
	 */
	public static Pipeline createStatsCalcPipeline(IndexerPipelineOptions options) throws Exception {
		
	    StatsCalcPipelineUtils.validateStatsCalcPipelineOptions(options);
		
		Pipeline pipeline = Pipeline.create(options);
		ArrayList<String> statFullQueryBatch = new ArrayList<String>();
		
		List<String> tablesToCalculateList = (options.getStatsCalcTables() == null) ? 
			Arrays.asList(StatsCalcPipelineUtils.allStatsCalcTables) :
			Arrays.asList(options.getStatsCalcTables());	
				
		/*
		 TODO: Reimplement parallel processing
		 Tree of dependencies:
		 	stattopic: not dependent on any stat tables
			statstoryimpact: not dependent on any stat tables
			statdomainopinions: not dependent on any stat tables
			statstoryrank: depends on statstoryimpact
			stattoptopic7d: depends on stattopic
			stattopstory7d: depends on stattoptopic7d, statstoryrank	
			stattoptopic7dsentiment: depends on stattoptopic7d, stattopic
		 */
		
		// Build an array of SQL statements to run sequentially in order to update the StatTopic table
		addSQLQueriesForStatTable(tablesToCalculateList, "stattopic", 
				StatsCalcPipelineUtils.getStatTopicQueryGenerator(), statFullQueryBatch, options);
		
		
		// Update StatStoryImpact table
		addSQLQueriesForStatTable(tablesToCalculateList, "statstoryimpact", 
				StatsCalcPipelineUtils.getStatStoryImpactQueryGenerator(), statFullQueryBatch, options);

		
		// Update StatDomainOpinions table
		addSQLQueriesForStatTable(tablesToCalculateList, "statdomainopinions", 
				StatsCalcPipelineUtils.getStatDomainOpinionsQueryGenerator(), statFullQueryBatch, options);

		
		// Update StatStoryRank table
		addSQLQueriesForStatTable(tablesToCalculateList, "statstoryrank", 
				StatsCalcPipelineUtils.getStatStoryRankQueryGenerator(), statFullQueryBatch, options);

		// Update StatTopTopic7d table
		addSQLQueriesForStatTable(tablesToCalculateList, "stattoptopic7d", 
				StatsCalcPipelineUtils.getStatTopTopic7dQueryGenerator(), statFullQueryBatch, options);

		// Update StatTopStory7d table
		addSQLQueriesForStatTable(tablesToCalculateList, "stattopstory7d", 
				StatsCalcPipelineUtils.getStatTopStory7dQueryGenerator(), statFullQueryBatch, options);

		// Update StatTopTopic7dSentiment table
		addSQLQueriesForStatTable(tablesToCalculateList, "stattoptopic7dsentiment", 
				StatsCalcPipelineUtils.getStatTopTopic7dSentimentQueryGenerator(), statFullQueryBatch, options);

		
		//addSQLCommandTransform(statStoryImpactQueryBatch, pipeline);
		String[] batch = new String[statFullQueryBatch.size()];
		statFullQueryBatch.toArray(batch);
		addSQLCommandTransform(batch, pipeline);
		
		return pipeline;
	}	

	private static void addSQLQueriesForStatTable(
		List<String> tablesToCalculateList, String table, StatsCalcPipelineUtils.StatsQueryGenerator gen, 
		List<String> fullBatch, IndexerPipelineOptions options) throws Exception {
		
		if (tablesToCalculateList.contains(table)) {	
			String[] tableQueryBatch = StatsCalcPipelineUtils.buildStatsCalcQueries(
					options.getStatsCalcDays(), options.getStatsCalcFromDate(), options.getStatsCalcToDate(), 
					options.getBigQueryDataset(), gen);
			
			fullBatch.addAll(Arrays.asList(tableQueryBatch));
		}
		
	}
	
	private static void addSQLCommandTransform(String[] queryBatch, Pipeline pipeline) {

		List<String[]> sqlCommands = new ArrayList<String[]>();
		sqlCommands.add(queryBatch);
		
		pipeline
			.apply(Create.of(sqlCommands)).setCoder(AvroCoder.of(String[].class))
			.apply(ParDo.of(new ExecuteSQLCommandBatch()));
		
	}

	
	static class ExecuteSQLCommandBatch extends DoFn<String[],Void> {

		@ProcessElement
		public void processElement(ProcessContext c) {
			String[] queryBatch = c.element();

			if (queryBatch == null) 
		    	return;
			
		    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

		    for (int i=0; i < queryBatch.length; i++) {
		    	String query = queryBatch[i];
			    QueryRequest queryRequest = QueryRequest
				    	.newBuilder(query)
				    	.setUseLegacySql(false)
				    	.build();
				QueryResponse response = bigquery.query(queryRequest);
				 // Wait for previous query to finish
				Boolean done = response.jobCompleted();
				int retriesLeft = 10;
				while (!done) {
					try {
						Thread.sleep(1000);
						JobId jobId = response.getJobId();
						response = bigquery.getQueryResults(jobId);
						done = response.jobCompleted();
					} catch (NullPointerException e) {
						retriesLeft--;
						LOG.warn("StatsCalcPipeline.ExecuteSQLCommandBatch.processElement: NullPointerException when getting BigQuery results. Retries left: " + retriesLeft);
						if (retriesLeft <= 0) {
							LOG.warn("StatsCalcPipeline.ExecuteSQLCommandBatch.processElement: NullPointerException when getting BigQuery results. Retries left: 0. Aborting this query " + query);
							done = true;
						}
					} catch (InterruptedException e) {
						LOG.warn("StatsCalcPipeline.ExecuteSQLCommandBatch.processElement:" + e.getMessage());
						done = true;
					} catch (Exception e) {
						LOG.warn("StatsCalcPipeline.ExecuteSQLCommandBatch.processElement: Exception " + e.getMessage());
						LOG.warn("StatsCalcPipeline.ExecuteSQLCommandBatch.processElement: error executing query "+ query);
						done = true;
					}
				}
				if (response.hasErrors()) {
					LOG.warn("StatsCalcPipeline.ExecuteSQLCommandBatch.processElement: BigQuery task completed with a response containing errors while executing query: "+ query);
				} else {
					LOG.info("StatsCalcPipeline.ExecuteSQLCommandBatch.processElement: BigQuery task completed with a response containing no [further] errors while executing query "+ query);
				}
		    
		    }
		}
		

	}

	/*
	 *  in main()
	 * 	Pipeline pipeline = Pipeline.create(options);
		List<String> startList = new ArrayList<String>();
		PCollection<String> currStage = pipeline.apply(Create.of(startList)).setCoder(StringUtf8Coder.of());

	 *  To call:
	 *  String query = StatsCalcPipelineUtils.buildBQDailyStatsDeleteByDateIdQuery(dateId,options.getBigQueryDataset());
		currStage = addSQLCommandTransform(query, currStage);

	 * 
	private static PCollection<String> addSQLCommandTransform(String query, PCollection<String> prevStage) {

		List<String> sqlCommand = Arrays.asList(query);
		PCollection<String> sqlCommandCol = prevStage.getPipeline().apply(Create.of(sqlCommand)).setCoder(StringUtf8Coder.of());
		
		PCollectionList<String> merged = PCollectionList.of(prevStage).and(sqlCommandCol);
		
		PCollection<String> nextStage = merged
			.apply(Flatten.<String>pCollections())
	    	.apply(ParDo.of(new ExecuteSQLCommand()));
		
		return nextStage;
		
	}
	*/
	

}
