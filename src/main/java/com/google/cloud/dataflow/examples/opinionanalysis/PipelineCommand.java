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

import java.util.Arrays;

import org.apache.beam.sdk.options.Description;

public class PipelineCommand {

	public String command;
	
	public Integer timeWindowSec;
	public String fromDate;
	public String toDate;
	public String gcsPath;
	public String pubsubTopic;
	public Integer historyWindowSec;
	public Boolean writeTruncate;
	public String[] days; // Day labels, e.g. T-1, T-2, T-3
	public String postsTable; // For Reddit
	public String commentsTable; // For Reddit
	public String postsQuery; // For Reddit
	public String commentsQuery; // For Reddit
	public String additionalText;
	
	public static final String START_GCS_IMPORT = "start_gcs_import";
	public static final String START_JDBC_IMPORT = "start_jdbc_import";
	public static final String START_PUBSUB_IMPORT = "start_pubsub_import";
	public static final String START_SOCIAL_IMPORT = "start_social_import";
	public static final String START_REDDIT_IMPORT = "start_reddit_import";
	public static final String START_GDELTBUCKET_IMPORT = "start_gdeltbucket_import";

	public static final String START_STATS_CALC = "start_stats_calc";
	
	
	public static final String[] permittedCommands = new String[] {START_GCS_IMPORT,START_JDBC_IMPORT,START_PUBSUB_IMPORT, START_SOCIAL_IMPORT, START_REDDIT_IMPORT, START_GDELTBUCKET_IMPORT, START_STATS_CALC};

	public static final String[] documentImportCommands = new String[] {START_GCS_IMPORT,START_JDBC_IMPORT,START_PUBSUB_IMPORT, START_REDDIT_IMPORT, START_GDELTBUCKET_IMPORT};
	public static final String[] socialImportCommands = new String[] {START_SOCIAL_IMPORT};
	public static final String[] statsCalcCommands = new String[] {START_STATS_CALC};
	
	
	public PipelineCommand() {
	}

	public static PipelineCommand createPipelineCommand(String s) throws Exception
	{

		TextWithProperties t = TextWithProperties.deserialize(s);
		PipelineCommand result = new PipelineCommand();
		result.command = t.properties.get("command");
		result.additionalText = t.text;
		
		if (!Arrays.asList(permittedCommands).contains(result.command))
			throw new Exception("Invalid command passed: " + result.command);

		// used for all IMPORT commands
		String propvalue = t.properties.get("timewindowsec");
		if (propvalue!=null)
			result.timeWindowSec = Integer.decode(propvalue);

		// used for all IMPORT commands
		propvalue = t.properties.get("historywindowsec");
		if (propvalue!=null)
			result.historyWindowSec = Integer.decode(propvalue);
		
		// used for all commands
		result.fromDate = t.properties.get("fromdate");
		result.toDate = t.properties.get("todate");

		//used for START_GCS_IMPORT, START_GDELTBUCKET_IMPORT
		result.gcsPath = t.properties.get("path"); // if it is not populated, we will use the options passed via command line
			
		//used for START_PUBSUB_IMPORT
		result.pubsubTopic = t.properties.get("topic");

		// used for all IMPORT commands
		propvalue = t.properties.get("writetruncate");
		if (propvalue!=null)
			result.writeTruncate = Boolean.valueOf(propvalue);

		// used for START_STATS_CALC
		// Needs to be in the following format: T-1, T-2, ...
		propvalue = t.properties.get("days");
		if (propvalue!=null) {
			result.days = propvalue.split(",");
		}
		
		// used for START_REDDIT_IMPORT
		result.postsTable = t.properties.get("poststable");
		result.commentsTable = t.properties.get("commentstable");
		result.postsQuery = t.properties.get("postsquery");
		result.commentsQuery = t.properties.get("commentsquery");
		
		return result;		
				
	}
	
	
}
