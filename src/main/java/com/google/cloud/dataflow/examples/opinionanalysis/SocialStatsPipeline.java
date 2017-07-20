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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.examples.opinionanalysis.model.WebresourceSocialCount;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.transforms.View;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;

import java.sql.ResultSet;


public class SocialStatsPipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(SocialStatsPipeline.class);
	
	public static void main(String[] args) throws Exception {
		
		IndexerPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IndexerPipelineOptions.class);

	    Pipeline pipeline = createSocialStatsPipeline(options); 
		
		pipeline.run();

	}

	/**
	 * This function creates the DAG graph of transforms. It can be called from main()
	 * as well as from the ControlPipeline.
	 * @param options
	 * @return
	 * @throws Exception
	 */
	public static Pipeline createSocialStatsPipeline(IndexerPipelineOptions options) throws Exception {
		
	    IndexerPipelineUtils.validateSocialStatsPipelineOptions(options);
		
		Pipeline pipeline = Pipeline.create(options);

		PCollection<WebresourceSocialCount> readCounts = null;
			
		String query = IndexerPipelineUtils.buildJdbcSourceImportQueryForSocialStats(options);
		
		readCounts = pipeline.apply (
            JdbcIO.<WebresourceSocialCount>read()
                .withDataSourceConfiguration(
                	JdbcIO.DataSourceConfiguration.create(options.getJdbcDriverClassName(), options.getJdbcSourceUrl())
                	.withUsername(options.getJdbcSourceUsername())
				    .withPassword(options.getJdbcSourcePassword())
                )
                .withQuery(query)
                .withRowMapper(new RowMapper<WebresourceSocialCount>() {
                	@Override
                	public WebresourceSocialCount mapRow(ResultSet resultSet) throws Exception {
                		WebresourceSocialCount result = new WebresourceSocialCount(
                				resultSet.getLong("page_pub_time")*1000L,
                				resultSet.getString("url"),
                				resultSet.getString("doc_col_id"),
                				resultSet.getString("col_item_id"),
                				resultSet.getLong("count_time")*1000L,
                				resultSet.getInt("count_tw"),
                				resultSet.getInt("count_fb")
                		); 
                	  
                		return result;
                	}
                })
                .withCoder(AvroCoder.of(WebresourceSocialCount.class))
	    );
		
		// if content is to be added to bigquery, then obtain a list of
		// latest social stats per page
		PCollection<WebresourceSocialCount> countsToProcess = null;
		
		if (options.getWriteTruncate() != null && !options.getWriteTruncate() && options.getWrSocialCountHistoryWindowSec() != null) {
			String queryCache = IndexerPipelineUtils.buildBigQueryProcessedSocialCountsQuery(options);
			PCollection<KV<String,Long>> lastCountTimes = pipeline
				.apply("Get processed social count times", BigQueryIO.read().fromQuery(queryCache))
				.apply(ParDo.of(new GetLastCountTime()));
	
			final PCollectionView<Map<String,Long>> lastCountTimesSideInput =
					lastCountTimes.apply(View.<String,Long>asMap());
			  
			countsToProcess = readCounts
				.apply(ParDo
						.of(new DoFn<WebresourceSocialCount, WebresourceSocialCount>() {
							@ProcessElement
							public void processElement(ProcessContext c) {
								WebresourceSocialCount i = c.element();
								// check in the map if we already processed this Url, and if we haven't, add the input content to 
								// the list that needs to be processed 
								Long lastTime = c.sideInput(lastCountTimesSideInput).get(i.webResourceHash);
								
								if (lastTime == null || lastTime < i.countTime)
									c.output(i);
							}
						})
						.withSideInputs(lastCountTimesSideInput)
						);
		} else {
			countsToProcess = readCounts;
		}

		PCollection<TableRow> wrSocialCounts = countsToProcess
			.apply(ParDo.of(new CreateWrSocialCountTableRowFn()));
		
		// Now write to BigQuery
		WriteDisposition dispo = options.getWriteTruncate() ? 
				WriteDisposition.WRITE_TRUNCATE: WriteDisposition.WRITE_APPEND; 
		
		wrSocialCounts
			.apply("Write to wrsocialcount", BigQueryIO
				.writeTableRows() 
				.withSchema(getWrSocialCountSchema())
				.withWriteDisposition(dispo)
				.to(getWRSocialCountTableReference(options))); 
		

		return pipeline;
	}	

	/**
	 * Setup step {A}
	 * Helper method that defines the BigQuery schema used for the output.
	 */
	private static TableSchema getWrSocialCountSchema() {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("WebResourceHash").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("WrPublicationDateId").setType("INTEGER").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("CountTime").setType("TIMESTAMP").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("DocumentCollectionId").setType("STRING"));
		fields.add(new TableFieldSchema().setName("CollectionItemId").setType("STRING"));
		fields.add(new TableFieldSchema().setName("FbCount").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("TwCount").setType("INTEGER"));

		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}	


	static class CreateWrSocialCountTableRowFn extends DoFn<WebresourceSocialCount, TableRow> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			WebresourceSocialCount sc = c.element();
			
			Instant countTime = new Instant(sc.countTime);
			
			TableRow row = new TableRow()
				.set("WebResourceHash", sc.webResourceHash)
				.set("WrPublicationDateId", sc.wrPublicationDateId)
				.set("CountTime", countTime.toString())
				.set("DocumentCollectionId", sc.documentCollectionId)
				.set("CollectionItemId", sc.collectionItemId)
				.set("FbCount", sc.fbCount)
				.set("TwCount", sc.twCount);

			c.output(row);

		}
	}

	/**
	 * 
	 */

	static class GetLastCountTime extends DoFn<TableRow, KV<String,Long>> {
		
		@ProcessElement
		public void processElement(ProcessContext c) {
			TableRow row = c.element();
			String url = row.get("WebResourceHash").toString();
			String lastCountTime = row.get("LastCountTime").toString();
			if (url != null && !url.isEmpty())
			{
				Long l = IndexerPipelineUtils.parseDateToLong(IndexerPipelineUtils.dateTimeFormatYMD_HMS_MSTZ, lastCountTime);
				if (l == null) l = 1L;
				KV<String,Long> kv = KV.of(url, l);
				c.output(kv);
			}
		}
	}

	/**
	 * 
	 */	
	

	private static TableReference getWRSocialCountTableReference(IndexerPipelineOptions options) {
		TableReference tableRef = new TableReference();
		tableRef.setProjectId(options.getProject());
		tableRef.setDatasetId(options.getBigQueryDataset());
		tableRef.setTableId(IndexerPipelineUtils.WRSOCIALCOUNT_TABLE);
		return tableRef;
	}
	

}
