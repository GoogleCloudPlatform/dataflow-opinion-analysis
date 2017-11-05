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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.joda.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.examples.opinionanalysis.io.RecordFileSource;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
//import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
//import org.apache.beam.sdk.io.gcp.pubsub.PubsubJsonClient;
/*
import com.google.cloud.pubsub.spi.v1.TopicAdminClient;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.cloud.pubsub.spi.v1.SubscriptionAdminClient;
*/
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;

public class ControlPipeline {

	private static final Logger LOG = LoggerFactory.getLogger(ControlPipeline.class);
	
	public static void main(String[] args) throws IOException {
		
		ControlPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ControlPipelineOptions.class);
		PipelineOptionsFactory.register(ControlPipelineOptions.class);
		
		Pipeline pipeline = Pipeline.create(options);
		
		if (options.isControlGCS()) {
			
			// Read commands from GCS file(s)

			final Bounded<String> read = org.apache.beam.sdk.io.Read.from(
				new RecordFileSource<String>(ValueProvider.StaticValueProvider.of(options.getControlGCSPath()), 
					StringUtf8Coder.of(), RecordFileSource.DEFAULT_RECORD_SEPARATOR));
			pipeline
				.apply("Read", read)
				.apply("Process Commands",ParDo.of(new ProcessCommand()));	

			
		} else if (options.isControlPubsub()){

			options.setStreaming(true);
			
			// Accept commands from a Control Pub/Sub topic
			pipeline
				.apply("Read from control topic",
					PubsubIO.readStrings().fromTopic(options.getControlPubsubTopic()))
				.apply("Process Commands",ParDo.of(new ProcessCommand()));		
			
			
			/* This section will eventually work with the 0.19.0-alpha
			 * and later versions of the ideomatic Java client google-cloud
			 * But for now remove this check.

			String subscriptionId = "indexercommands_controller";
			String topicId = options.getControlPubsubTopic();
			String projectId = options.getProject();
			String subscriptionPath = "projects/"+projectId+"/subscriptions/"+subscriptionId;

			// Support legacy way of passing the Control Topic that included the whole path
			if (topicId.startsWith("projects/")) {
				String[] tokens = topicId.split("/");
				topicId = tokens[tokens.length-1];
			}
			
			SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create();
			SubscriptionName subscriptionName = SubscriptionName.create(projectId, subscriptionId);
			TopicName topicName = TopicName.create(projectId, topicId);
			
			Subscription subscription;
		    try {
		        subscription = subscriptionAdminClient.getSubscription(subscriptionName);
		    } catch (Exception e) {
		    	subscription = null;
		    }
		    
		    if (subscription == null) {
		    	try {
		    		// create a pull subscription
		    		subscription = subscriptionAdminClient.createSubscription(
		    				subscriptionName, topicName, PushConfig.getDefaultInstance(), 60);
		    	} catch (Exception e) {
		    		LOG.error(e.getMessage());
		    		System.exit(1);
		    	}
		    }
			*/
			/*
			PubsubClient pubsubClient = PubsubJsonClient.FACTORY.newClient(null, null, options.as(DataflowPipelineOptions.class));
			PubsubClient.ProjectPath pp = PubsubClient.projectPathFromPath("projects/"+options.getProject());
			PubsubClient.TopicPath tp = PubsubClient.topicPathFromPath(options.getControlPubsubTopic());
			PubsubClient.SubscriptionPath sp = PubsubClient.subscriptionPathFromPath(subscriptionPath);
			
			List<PubsubClient.SubscriptionPath> l = pubsubClient.listSubscriptions(pp, tp);
			if (!l.contains(sp))
				pubsubClient.createSubscription(tp, sp, 60);
			*/
			
		}
		
		pipeline.run();

	}
	

	/**
	 * 
	 */
	static class ProcessCommand extends DoFn<String, Void> {
		@ProcessElement
		public void processElement(ProcessContext c) {

			LOG.info("ProcessCommand.processElement entered.");
			
			String commandEnvelope = null;
			try {
				commandEnvelope = c.element();
				if (commandEnvelope == null || commandEnvelope.isEmpty())
					throw new Exception("ProcessCommand.processElement: null or empty command envelope");
				
				ControlPipelineOptions options = c.getPipelineOptions().as(ControlPipelineOptions.class);
				
				PipelineCommand command = PipelineCommand.createPipelineCommand(commandEnvelope);

				// Triage the various commands to their processors
				if (Arrays.asList(PipelineCommand.documentImportCommands).contains(command.command))
					startDocumentImportPipeline(command, options);
				else if (Arrays.asList(PipelineCommand.socialImportCommands).contains(command.command))
					startSocialImportPipeline(command, options);
				else if (Arrays.asList(PipelineCommand.statsCalcCommands).contains(command.command))
					startStatsCalcPipeline(command, options);
				else
					throw new Exception ("Unsupported command "+command.command);
				

			} catch (Exception e) {
				LOG.warn(e.getMessage());
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				e.printStackTrace(pw);
				LOG.info(sw.toString());
			}

		}

		/**
		 * 
		 * 
		 */
		public void startDocumentImportPipeline(PipelineCommand command, ControlPipelineOptions options) throws Exception {

			LOG.info("ProcessCommand.startDocumentImportPipeline entered with command "+command.command);
			IndexerPipelineOptions copiedOptions = createJobOptions(options, command);

			// do some common option transfer and setting
			transferOptions(options, copiedOptions);

			if (command.historyWindowSec != null)
				copiedOptions.setProcessedUrlHistorySec(command.historyWindowSec);

			if (command.writeTruncate != null)
				copiedOptions.setWriteTruncate(command.writeTruncate);
			
			// do command-dependent options transfers
			if (command.command.equals(PipelineCommand.START_GCS_IMPORT)) {

				copiedOptions.setSourceRecordFile(true);
				copiedOptions.setJobName(options.getJobName() + "-gcsdocimport");

				if (command.gcsPath != null)
					copiedOptions.setInputFile(command.gcsPath);
				
			} else if (command.command.equals(PipelineCommand.START_GDELTBUCKET_IMPORT)) {

				copiedOptions.setStreaming(false);
				
				copiedOptions.setSourceGDELTbucket(true);
				copiedOptions.setJobName(options.getJobName() + "-gdeltbucketimport");

				if (command.gcsPath != null)
					copiedOptions.setInputFile(command.gcsPath);
				
			
			} else if (command.command.equals(PipelineCommand.START_JDBC_IMPORT)) {
				
				copiedOptions.setSourceJDBC(true); 
				copiedOptions.setJobName(options.getJobName() + "-jdbcdocimport");

				if (command.timeWindowSec != null)
					copiedOptions.setJdbcSourceTimeWindowSec(command.timeWindowSec);

				if (command.fromDate != null)
					copiedOptions.setJdbcSourceFromDate(command.fromDate);

				if (command.toDate != null)
					copiedOptions.setJdbcSourceToDate(command.toDate);
			
			} else if (command.command.equals(PipelineCommand.START_PUBSUB_IMPORT)) {
				
				copiedOptions.setSourcePubsub(true); 
				copiedOptions.setJobName(options.getJobName() + "-pubsubdocimport");
			
			} else if (command.command.equals(PipelineCommand.START_REDDIT_IMPORT)) {
				
				copiedOptions.setSourceRedditBQ(true);
				
				if (command.postsTable != null)
					copiedOptions.setRedditPostsTableName(command.postsTable);
				
				if (command.postsQuery != null)
					copiedOptions.setRedditPostsQuery(command.postsQuery);

				if (command.commentsTable != null)
					copiedOptions.setRedditCommentsTableName(command.commentsTable);
				
				if (command.commentsQuery != null)
					copiedOptions.setRedditCommentsQuery(command.commentsQuery);
				
				copiedOptions.setJobName(options.getJobName() + "-redditimport");
			
			}

		    Pipeline pipeline = IndexerPipeline.createIndexerPipeline(copiedOptions); 
			LOG.info("Starting Job " + copiedOptions.getJobName());
			pipeline.run();
			
		}		

		
		
		/**
		 * 
		 * 
		 */
		public void startSocialImportPipeline(PipelineCommand command, ControlPipelineOptions options) throws Exception {

			LOG.info("ProcessCommand.startSocialImportPipeline entered with command "+command.command);

			if (!command.command.equals(PipelineCommand.START_SOCIAL_IMPORT)) 
				return;
			
			IndexerPipelineOptions copiedOptions = createJobOptions(options, command);
			
			transferOptions(options, copiedOptions);

			if (command.historyWindowSec != null)
				copiedOptions.setWrSocialCountHistoryWindowSec(command.historyWindowSec);

			if (command.writeTruncate != null)
				copiedOptions.setWriteTruncate(command.writeTruncate);
			

			// These 3 options are more applicable to document import, since social import is
			// via jdbc only anyway, but still, set them, in case we support social import
			// from elsewhere in the future
			copiedOptions.setSourceRecordFile(false);
			copiedOptions.setSourcePubsub(false); 
			copiedOptions.setSourceJDBC(true); 

			copiedOptions.setJobName(options.getJobName() + "-jdbcsocialimport");
			
			if (command.timeWindowSec != null)
				copiedOptions.setJdbcSourceTimeWindowSec(command.timeWindowSec);

			if (command.fromDate != null)
				copiedOptions.setJdbcSourceFromDate(command.fromDate);

			if (command.toDate != null)
				copiedOptions.setJdbcSourceToDate(command.toDate);

		    Pipeline pipeline = SocialStatsPipeline.createSocialStatsPipeline(copiedOptions); 

			LOG.info("Starting Job "+copiedOptions.getJobName());
			pipeline.run();
			
		}		
		
		/**
		 * 
		 * 
		 */
		public void startStatsCalcPipeline(PipelineCommand command, ControlPipelineOptions options) throws Exception {

			LOG.info("ProcessCommand.startStatsCalcPipeline entered with command "+command.command);

			if (!command.command.equals(PipelineCommand.START_STATS_CALC)) 
				return;
			
			IndexerPipelineOptions copiedOptions = createJobOptions(options, command);
			
			transferOptions(options, copiedOptions);

			copiedOptions.setJobName(options.getJobName() + "-statscalc");
			
			if (command.fromDate != null)
				copiedOptions.setStatsCalcFromDate(command.fromDate);

			if (command.toDate != null)
				copiedOptions.setStatsCalcToDate(command.toDate);
			
			if (command.days != null)
				copiedOptions.setStatsCalcDays(command.days);

			
		    Pipeline pipeline = StatsCalcPipeline.createStatsCalcPipeline(copiedOptions); 
			LOG.info("Starting Job "+copiedOptions.getJobName());
			pipeline.run();
			
		}		
		
		/**
		 * @param controlOptions
		 * @param jobOptions
		 */
		private void transferOptions(ControlPipelineOptions controlOptions, IndexerPipelineOptions jobOptions) {
			if (controlOptions.getJobMaxNumWorkers() != null)
				jobOptions.setMaxNumWorkers(controlOptions.getJobMaxNumWorkers());
			if (controlOptions.getJobAutoscalingAlgorithm() != null)
				jobOptions.setAutoscalingAlgorithm(DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.valueOf(
						controlOptions.getJobAutoscalingAlgorithm()));
			if (controlOptions.getJobWorkerMachineType() != null)
				jobOptions.setWorkerMachineType(controlOptions.getJobWorkerMachineType());
			if (controlOptions.getJobDiskSizeGb() != null)
				jobOptions.setDiskSizeGb(controlOptions.getJobDiskSizeGb());
			if (controlOptions.getJobStagingLocation() != null)
				jobOptions.setStagingLocation(controlOptions.getJobStagingLocation());
			else
				jobOptions.setStagingLocation(controlOptions.getStagingLocation());
			
		}

		
		private IndexerPipelineOptions createJobOptions(ControlPipelineOptions options, PipelineCommand command) throws Exception {
			IndexerPipelineOptions result = PipelineOptionsFactory.as(IndexerPipelineOptions.class);
			
			/* CloneAs kept failing with error 
			 * 		java.lang.IllegalStateException: Failed to serialize the pipeline options to JSON. 
			 * 			at org.apache.beam.sdk.options.ProxyInvocationHandler.cloneAs(ProxyInvocationHandler.java:272) 
			 * To fix, implemented a manual shallow copy of options
			 */
			//IndexerPipelineOptions copiedOptions = options.cloneAs(IndexerPipelineOptions.class);

			
			// TODO: Is this still necessary? This will be overwritten anyways
			if (options.isSourcePubsub() != null)
				result.setSourcePubsub(options.isSourcePubsub());
			if (options.isSourceJDBC() != null)
				result.setSourceJDBC(options.isSourceJDBC());
			if (options.isSourceRecordFile() != null)
				result.setSourceRecordFile(options.isSourceRecordFile());
			
			// copy the default options of the IndexerPipelineOptions interface
			if (options.getInputFile() != null)
				result.setInputFile(options.getInputFile());
			if (options.getPubsubTopic() != null)
				result.setPubsubTopic(options.getPubsubTopic());
			if (options.getJdbcDriverClassName() != null)
				result.setJdbcDriverClassName(options.getJdbcDriverClassName());
			if (options.getJdbcSourceUrl() != null)
				result.setJdbcSourceUrl(options.getJdbcSourceUrl());
			if (options.getJdbcSourceUsername() != null)
				result.setJdbcSourceUsername(options.getJdbcSourceUsername());
			if (options.getJdbcSourcePassword() != null)
				result.setJdbcSourcePassword(options.getJdbcSourcePassword());
			if (options.getJdbcSourceTimeWindowSec() != null)
				result.setJdbcSourceTimeWindowSec(options.getJdbcSourceTimeWindowSec());
			if (options.getJdbcSourceFromDate() != null)
				result.setJdbcSourceFromDate(options.getJdbcSourceFromDate());
			if (options.getJdbcSourceToDate() != null)
				result.setJdbcSourceToDate(options.getJdbcSourceToDate());
			if (options.getBigQueryDataset() != null)
				result.setBigQueryDataset(options.getBigQueryDataset());
			if (options.getWriteTruncate() != null)
				result.setWriteTruncate(options.getWriteTruncate());
			if (options.getProcessedUrlHistorySec() != null)
				result.setProcessedUrlHistorySec(options.getProcessedUrlHistorySec());
			if (options.getWrSocialCountHistoryWindowSec() != null)
				result.setWrSocialCountHistoryWindowSec(options.getWrSocialCountHistoryWindowSec());
			
			if (options.getStatsCalcFromDate() != null)
				result.setStatsCalcFromDate(options.getStatsCalcFromDate());
		    
			if (options.getStatsCalcToDate() != null)
				result.setStatsCalcToDate(options.getStatsCalcToDate());

		    if (options.getStatsCalcDays() != null)
		    	result.setStatsCalcDays(options.getStatsCalcDays());

		    if (options.getStatsCalcTables() != null)
		    	result.setStatsCalcTables(options.getStatsCalcTables());
		    
			if (options.getRedditPostsTableName() != null)
				result.setRedditPostsTableName(options.getRedditPostsTableName());

			if (options.getRedditCommentsTableName() != null)
				result.setRedditCommentsTableName(options.getRedditCommentsTableName());

			if (options.getRedditPostsQuery() != null)
				result.setRedditPostsQuery(options.getRedditPostsQuery());

			if (options.getRedditCommentsQuery() != null)
				result.setRedditCommentsQuery(options.getRedditCommentsQuery());

			// Other options
			if (options.getProject() != null)
				result.setProject(options.getProject());

			/*
			 *  Staging location will be set in transferOptions
			 */

			if (options.getTempLocation() != null)
				result.setTempLocation(options.getTempLocation());
			
			if (options.getFilesToStage() != null)
				result.setFilesToStage(options.getFilesToStage());
			
			result.setRunner(options.getRunner());
			result.setJobName(options.getJobName());
			
			if (options.getCredentialFactoryClass() != null)
				result.setCredentialFactoryClass(options.getCredentialFactoryClass());
			
			if (options.getGcpCredential()!=null)
				result.setGcpCredential(options.getGcpCredential());
			
			if (options.getPubsubRootUrl()!=null)
				result.setPubsubRootUrl(options.getPubsubRootUrl());
			
			return result;
			
		}
		
		
		
	}
	
	
	

}
