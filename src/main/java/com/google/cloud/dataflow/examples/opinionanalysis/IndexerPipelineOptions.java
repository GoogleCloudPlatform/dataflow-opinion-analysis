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

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/**
 * Options supported by {@link IndexerPipeline}.
 *
 */
public interface IndexerPipelineOptions extends DataflowPipelineOptions {

	@Description("Whether to connect to Pub/Sub as source")
	@Default.Boolean(false)
	Boolean isSourcePubsub();
	void setSourcePubsub(Boolean value);
	
	@Description("Whether to connect to a JDBC source")
	@Default.Boolean(false)
	Boolean isSourceJDBC();
	void setSourceJDBC(Boolean value);
	
	@Description("Whether to connect to BigQuery dataset with Reddit posts and comments")
	@Default.Boolean(false)
	Boolean isSourceRedditBQ();
	void setSourceRedditBQ(Boolean value);

	@Description("Whether to read files from Reddit archive file")
	@Default.Boolean(false)
	Boolean isSourceRedditArchive();
	void setSourceRedditArchive(Boolean value);

	@Description("Whether to read files from GDELT bucket")
	@Default.Boolean(false)
	Boolean isSourceGDELTbucket();
	void setSourceGDELTbucket(Boolean value);
		
	@Description("Whether to connect to a GCS source in RecordFile format")
	@Default.Boolean(false)
	Boolean isSourceRecordFile();
	void setSourceRecordFile(Boolean value);
	
	@Description("Source files. Use with RecordFile and GDELTbucket sources. Names can be a pattern like *.gz or *.txt")
    String getInputFile();
    void setInputFile(String value);

	@Description("Source RecordFile: Record delimiter characters dividing records in the file. If multiple characters are used, separate them by a comma, e.g. 13,10 stands for <CR><LF>. Use delimiter 30 (=<RS>) to prevent regular new line delimiters splitting text files.")
    String getRecordDelimiters();
    void setRecordDelimiters(String value);

	@Description("Source RecordFile: Should records be processed as CSV")
	@Default.Boolean(false)
    Boolean getReadAsCSV();
    void setReadAsCSV(Boolean value);
    
	@Description("Source RecordFile: Should records be processed as bag of properties")
    Boolean getReadAsPropertyBag();
    void setReadAsPropertyBag(Boolean value);

    @Description("CSV inputs: Zero-based index of the Text column in input file")
    Integer getTextColumnIdx();
    void setTextColumnIdx(Integer textColumnIdx);
	
    @Description("CSV inputs: Zero-based index of the Collection Item ID column - unique identifier - in input file")
    Integer getCollectionItemIdIdx();
    void setCollectionItemIdIdx(Integer collectionItemIdIdx);
    
    
    @Description("Source Pub/Sub topic")
    String getPubsubTopic();
    void setPubsubTopic(String topic);
    
    @Description("Source JDBC driver class")
    String getJdbcDriverClassName();
    void setJdbcDriverClassName(String driverClassName);
    
    @Description("Source JDBC Url")
    String getJdbcSourceUrl();
    void setJdbcSourceUrl(String jdbcSourceUrl);

    @Description("Source JDBC Username")
    String getJdbcSourceUsername();
    void setJdbcSourceUsername(String jdbcSourceUsername);

    @Description("Source JDBC Password")
    String getJdbcSourcePassword();
    void setJdbcSourcePassword(String jdbcSourcePassword);
    
    @Description("Source JDBC Time Window (in seconds) till Now()")
    Integer getJdbcSourceTimeWindowSec();
    void setJdbcSourceTimeWindowSec(Integer seconds);

    @Description("Source JDBC From Date")
    String getJdbcSourceFromDate();
    void setJdbcSourceFromDate(String jdbcSourceFromDate);
    
    @Description("Source JDBC To Date")
    String getJdbcSourceToDate();
    void setJdbcSourceToDate(String jdbcSourceToDate);

    @Description("Source Reddit Posts [fully qualified] Table Name")
    String getRedditPostsTableName();
    void setRedditPostsTableName(String redditPostsTableName);

    @Description("Source Reddit [fully qualified] Comments Table Name")
    String getRedditCommentsTableName();
    void setRedditCommentsTableName(String redditCommentsTableName);

    @Description("Source Reddit Query for Posts")
    String getRedditPostsQuery();
    void setRedditPostsQuery(String redditPostsQuery);

    @Description("Source Reddit Query for Comments")
    String getRedditCommentsQuery();
    void setRedditCommentsQuery(String redditCommentsQuery);
    
    @Description("Sink BigQuery dataset name")
    String getBigQueryDataset();
    void setBigQueryDataset(String dataset);
    
	@Description("Sink GCS output file.")
    String getOutputFile();
    void setOutputFile(String value);
    
    @Description("Deduplicate based on text content")
    Boolean getDedupeText();
    void setDedupeText(Boolean dedupeText);

    @Description("Text indexing option: Index as Short text or as an Article. Default: false")
    @Default.Boolean(false)
    Boolean getIndexAsShorttext();
    void setIndexAsShorttext(Boolean indexAsShorttext);
    
    @Description("Truncate sink BigQuery dataset before writing")
    @Default.Boolean(false)
    Boolean getWriteTruncate();
    void setWriteTruncate(Boolean writeTruncate);
    
    @Description("Time window in seconds counting from Now() for processed url cache. Set to "+Integer.MAX_VALUE+ " to cache all Urls, don't set for no cache.")
    Integer getProcessedUrlHistorySec();
    void setProcessedUrlHistorySec(Integer seconds);

    @Description("Time window in seconds counting from Now() for cache of webresource social counts. Set to "+Integer.MAX_VALUE+ " to cache all pages, don't set for no cache.")
    Integer getWrSocialCountHistoryWindowSec();
    void setWrSocialCountHistoryWindowSec(Integer seconds);

    @Description("Stats Calc From Date")
    String getStatsCalcFromDate();
    void setStatsCalcFromDate(String fromDate);
    
    @Description("Stats Calc To Date")
    String getStatsCalcToDate();
    void setStatsCalcToDate(String toDate);

    @Description("Stats Calc Days, as an array of string labels, e.g. T-1, T-2, ...")
    String[] getStatsCalcDays();
    void setStatsCalcDays(String[] days);

    @Description("Stats Calc Tables, as an array of string table names, e.g. stattopic, statstoryimpact, ...")
    String[] getStatsCalcTables();
    void setStatsCalcTables(String[] tables);
    
    @Description("Set the Bigtable IndexerAdminDB instance for Dead Letter log and config")
    String getBigtableIndexerAdminDB();
    void setBigtableIndexerAdminDB(String instance);

    @Description("Ratio of elements to enrich with CNLP data")
    Float getRatioEnrichWithCNLP();
    void setRatioEnrichWithCNLP(Float ratio);
    
    
}
