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
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Options supported by {@link IndexerPipeline}.
 *
 */
public interface IndexerPipelineOptions
		extends PipelineOptions, DataflowPipelineOptions {

	@Description("Whether to connect to Pub/Sub as source")
	@Default.Boolean(false)
	Boolean isSourcePubsub();
	void setSourcePubsub(Boolean value);
	
	@Description("Whether to connect to a JDBC source")
	@Default.Boolean(false)
	Boolean isSourceJDBC();
	void setSourceJDBC(Boolean value);
	
	@Description("Whether to connect to a GCS source in RecordFile format")
	@Default.Boolean(false)
	Boolean isSourceRecordFile();
	void setSourceRecordFile(Boolean value);
	
    @Description("Source GCS files. Use with RecordFile file sources.")
    String getInputFile();
    void setInputFile(String value);

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

    @Description("Sink BigQuery dataset name")
    String getBigQueryDataset();
    void setBigQueryDataset(String dataset);
    
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
 	
}
