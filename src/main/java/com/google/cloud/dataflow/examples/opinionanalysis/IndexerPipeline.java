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
import com.google.cloud.dataflow.examples.opinionanalysis.io.RecordFileSource;
import com.google.cloud.dataflow.examples.opinionanalysis.model.InputContent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.CalendarWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;

import sirocco.indexer.Indexer;
import sirocco.indexer.IndexingConsts;
import sirocco.indexer.util.LogUtils;
import sirocco.model.ContentIndex;
import sirocco.model.summary.ContentIndexSummary;
import sirocco.model.summary.WebResource;
import sirocco.util.HashUtils;

import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSet;


public class IndexerPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(IndexerPipeline.class);
	static final int WINDOW_SIZE = 3; // Default window duration in seconds
	static final long REPORT_LONG_INDEXING_DURATION = 10000; // Report indexing duration longer than 10s.  
	
	static final TupleTag<TableRow> webresourceTag = new TupleTag<TableRow>(){};
	static final TupleTag<TableRow> documentTag = new TupleTag<TableRow>(){};
	static final TupleTag<TableRow> sentimentTag = new TupleTag<TableRow>(){};
	
	static final TupleTag<InputContent> contentToIndexTag = new TupleTag<InputContent>(){};
	static final TupleTag<InputContent> contentNotToIndexTag = new TupleTag<InputContent>(){};
	
	public static void main(String[] args) throws Exception {
		
		IndexerPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IndexerPipelineOptions.class);

	    Pipeline pipeline = createIndexerPipeline(options); 
		
		pipeline.run();

	}

	/**
	 * This function creates the DAG graph of transforms. It can be called from main()
	 * as well as from the ControlPipeline.
	 * @param options
	 * @return
	 * @throws Exception
	 */
	public static Pipeline createIndexerPipeline(IndexerPipelineOptions options) throws Exception {
		
	    IndexerPipelineUtils.validateIndexerPipelineOptions(options);
		Pipeline pipeline = Pipeline.create(options);
		
		PCollection<InputContent> readContent = null;
		
		if (options.isSourcePubsub()) {
			
			PCollection<String> rawInput = pipeline.apply(PubsubIO.readStrings().fromTopic(options.getPubsubTopic())); // Pipeline step {1.a}
			readContent = rawInput.apply(ParDo.of(new ParseRawInput()));

		} else if (options.isSourceJDBC()){
			
			String query = IndexerPipelineUtils.buildJdbcSourceImportQuery(options);
			
			readContent = pipeline.apply (
	            JdbcIO.<InputContent>read()
	                .withDataSourceConfiguration(
	                	JdbcIO.DataSourceConfiguration.create(options.getJdbcDriverClassName(), options.getJdbcSourceUrl())
	                	.withUsername(options.getJdbcSourceUsername())
					    .withPassword(options.getJdbcSourcePassword())
	                )
	                .withQuery(query)
	                .withRowMapper(new RowMapper<InputContent>() {
	                	@Override
	                	public InputContent mapRow(ResultSet resultSet) throws Exception {
	                		InputContent result = new InputContent(
		                		resultSet.getString("url"),
		                		resultSet.getLong("pub_time")*1000L,
		                		resultSet.getString("title"),
		                		resultSet.getString("author"),
		                		resultSet.getString("language"),
		                		resultSet.getString("page_text"),
		                		resultSet.getString("doc_col_id"), 
		                		resultSet.getString("col_item_id"),
		                		resultSet.getInt("skip_indexing")
	                		); 
	                	  
	                		return result;
	                	}
	                })
	                .withCoder(AvroCoder.of(InputContent.class))
		    );

		} else if (options.isSourceRedditBQ()) {	

			PCollection<TableRow> posts = null;
			PCollection<TableRow> comments = null;
			
			if (options.getRedditPostsQuery() != null)
				posts = pipeline.apply(BigQueryIO.read().fromQuery(options.getRedditPostsQuery()));
			else if (options.getRedditPostsTableName() != null)
				posts = pipeline.apply(BigQueryIO.read().from(options.getRedditPostsTableName()));
			
			if (options.getRedditCommentsQuery() != null)
				comments = pipeline.apply(BigQueryIO.read().fromQuery(options.getRedditCommentsQuery()));
			else if (options.getRedditCommentsTableName() != null)
				comments = pipeline.apply(BigQueryIO.read().from(options.getRedditCommentsTableName()));
			
			// Create InputContent by doing CoGroupByKey on Posts and Comments
			readContent = IndexerPipelineUtils.joinRedditPostsAndComments(posts, comments);
		
		} else if (options.isSourceGDELTbucket()) {	
			
			readContent = pipeline
				.apply("Read GDELT files", TextIO.read().from(options.getInputFile()))
				.apply("Parse JSON into InputContent", ParDo.of(new ParseGDELTJsonInput()));
			
			
		} else {
			// Read from GCS files
			final Bounded<String> read = org.apache.beam.sdk.io.Read.from(
					new RecordFileSource<String>(ValueProvider.StaticValueProvider.of(options.getInputFile()), 
							StringUtf8Coder.of(), RecordFileSource.DEFAULT_RECORD_SEPARATOR));

			PCollection<String> rawInput = pipeline.apply("Read", read);

			readContent = rawInput.apply(ParDo.of(new ParseRawInput())); 
		}

		PCollection<InputContent> contentToProcess = null;
		
		// if content is to be added to bigquery, then obtain a cache of
		// already processed Urls and remove from indexing queue items that were already 
		// inserted to bigquery
		
		if (options.getWriteTruncate() != null && !options.getWriteTruncate()) {
			String query = IndexerPipelineUtils.buildBigQueryProcessedUrlsQuery(options);
			PCollection<KV<String,Long>> alreadyProcessedUrls = pipeline
				.apply("Get processed URLs",BigQueryIO.read().fromQuery(query))
				.apply(ParDo.of(new GetUrlFn()));
	
			final PCollectionView<Map<String,Long>> alreadyProcessedUrlsSideInput =
				alreadyProcessedUrls.apply(View.<String,Long>asMap());
			  
			contentToProcess = readContent
				.apply(ParDo.of(
					new DoFn<InputContent, InputContent>() {
						@ProcessElement
						public void processElement(ProcessContext c) {
							InputContent i = c.element();
							// check in the map if we already processed this Url, and if we haven't, add the input content to 
							// the list that needs to be processed 
							Long proTime = c.sideInput(alreadyProcessedUrlsSideInput).get(i.url);
							if (proTime == null)
								c.output(i);
						}
					})
					.withSideInputs(alreadyProcessedUrlsSideInput)
					);
		} else {
			contentToProcess = readContent;
		}
		

		// Split the remaining items into items to index and items just to create as webresource
		// based on skipIndexing flag
		PCollection<InputContent> contentToIndex = null;
		PCollection<InputContent> contentNotToIndexSkipped = null;
		
		PCollectionTuple indexOrNot = contentToProcess
			.apply("Filter items to index based on skipIndexing flag", ParDo.of(new FilterItemsToIndex())
				.withOutputTags(contentToIndexTag, // main output collection
					TupleTagList.of(contentNotToIndexTag)) // side output collection		
		); 

		contentToIndex = indexOrNot.get(contentToIndexTag);
		contentNotToIndexSkipped = indexOrNot.get(contentNotToIndexTag);

		
		// Split items in main flow based on weather they are dupes or not
		PCollection<InputContent> contentNotToIndexDupes = null;

		if (options.isSourceGDELTbucket()) {
			indexOrNot = contentToIndex
				.apply("Extract DocumentHash key", ParDo.of(new GetInputContentDocumentHashFn()))
				.apply("Group by DocumentHash key", GroupByKey.<String, InputContent>create())
				.apply("Eliminate InputContent Dupes", ParDo.of(new EliminateInputContentDupesFn())
					.withOutputTags(contentToIndexTag, // main output collection
						TupleTagList.of(contentNotToIndexTag))); // side output collection	
			
			contentToIndex = indexOrNot.get(contentToIndexTag);
			contentNotToIndexDupes = indexOrNot.get(contentNotToIndexTag);
			
		}

		PCollection<InputContent> contentNotToIndex = null;
		
		if (contentNotToIndexDupes == null)
			contentNotToIndex = contentNotToIndexSkipped;
		else {
			// Merge the sets of items that are dupes or skipped
			PCollectionList<InputContent> contentNotToIndexList = PCollectionList.of(contentNotToIndexDupes).and(contentNotToIndexSkipped);
			contentNotToIndex = contentNotToIndexList.apply(Flatten.<InputContent>pCollections());
		}
			

		// Process content that does not need to be indexed
		PCollection<TableRow> webresourceRowsUnindexed = contentNotToIndex
			.apply(ParDo.of(new CreateWebresourceTableRowFromInputContentFn()));
		
		// Proceed with content that need to be indexed
		PCollection<ContentIndexSummary> indexes = contentToIndex
			.apply(ParDo.of(new IndexDocument())) 
			.setCoder(AvroCoder.of(ContentIndexSummary.class)); // Setup step {B}
		
		PCollectionTuple bqrows= indexes
			.apply(ParDo.of(new CreateTableRowsFromIndexSummaryFn())
				.withOutputTags(webresourceTag, // main output collection
					TupleTagList.of(documentTag).and(sentimentTag)) // 2 side output collections
				); 
		
		PCollection<TableRow> webresourceRows = bqrows.get(webresourceTag);
		PCollection<TableRow> documentRows = bqrows.get(documentTag);
		PCollection<TableRow> sentimentRows = bqrows.get(sentimentTag);

		// Now write to BigQuery
		WriteDisposition dispo = options.getWriteTruncate() ? 
				WriteDisposition.WRITE_TRUNCATE: WriteDisposition.WRITE_APPEND; 
		
		//Merge the two collections with WebResource table records
		PCollectionList<TableRow> webresourceRowsList = PCollectionList.of(webresourceRows).and(webresourceRowsUnindexed);
		PCollection<TableRow> allWebresourceRows = webresourceRowsList.apply(Flatten.<TableRow>pCollections());
				
		allWebresourceRows
			.apply("Write to webresource", 
				BigQueryIO.writeTableRows()
					.to(getWebResourceTableReference(options)) 
					.withSchema(getWebResourceSchema())
					.withCreateDisposition(CreateDisposition.CREATE_NEVER)
					.withWriteDisposition(dispo)); 
		
		
		documentRows
			.apply("Write to document", 
				BigQueryIO.writeTableRows()
					.to(getDocumentTableReference(options))
					.withSchema(getDocumentTableSchema())
					.withCreateDisposition(CreateDisposition.CREATE_NEVER)
					.withWriteDisposition(dispo)); 
		
		sentimentRows
			.apply("Write to sentiment", 
				BigQueryIO.writeTableRows()
					.to(getSentimentTableReference(options)) 
					.withSchema(getSentimentSchema())
					.withCreateDisposition(CreateDisposition.CREATE_NEVER)
					.withWriteDisposition(dispo));

		
		/* TODO: sso 6/29 Implement Partion Writes
		documentRows.apply(Window.<TableRow>into(CalendarWindows.days(1)))
			.apply(BigQueryIO.writeTableRows()
				.withSchema(getDocumentTableSchema())
				.withWriteDisposition(dispo)
				.to(new SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination>() {
					public TableDestination apply(ValueInSingleWindow<TableRow> value) {
						// The cast below is safe because CalendarWindows.days(1) produces IntervalWindows.
						 
						String dayString = DateTimeFormat.forPattern("yyyy_MM_dd")
							.withZone(DateTimeZone.UTC)
							.print(((IntervalWindow) value.getWindow()).start());
						return "my-project:output.output_table_" + dayString;
						 
						// return getDocumentTableReference(options) 
						return new TableDestination("dark-cat:opinions.document", null);
					}
				}));
		*/		 
		
		
		return pipeline;
	}	

	/**
	 * Setup step {A}
	 * Helper method that defines the BigQuery schema used for the output.
	 */
	private static TableSchema getWebResourceSchema() {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("WebResourceHash").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("Url").setType("STRING"));
		fields.add(new TableFieldSchema().setName("PublicationTime").setType("TIMESTAMP").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("PublicationDateId").setType("INTEGER").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("ProcessingTime").setType("TIMESTAMP").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("ProcessingDateId").setType("INTEGER").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("DocumentHash").setType("STRING"));
		fields.add(new TableFieldSchema().setName("DocumentCollectionId").setType("STRING"));
		fields.add(new TableFieldSchema().setName("CollectionItemId").setType("STRING"));
		fields.add(new TableFieldSchema().setName("Title").setType("STRING"));
		fields.add(new TableFieldSchema().setName("Domain").setType("STRING"));
		fields.add(new TableFieldSchema().setName("Author").setType("STRING"));
		fields.add(new TableFieldSchema().setName("ParentWebResourceHash").setType("STRING"));

		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}	

	/**
	 * Setup step {A}
	 * Helper method that defines the BigQuery schema used for the output.
	 */
	private static TableSchema getDocumentTableSchema() {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("DocumentHash").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("PublicationTime").setType("TIMESTAMP").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("PublicationDateId").setType("INTEGER").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("ProcessingTime").setType("TIMESTAMP").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("ProcessingDateId").setType("INTEGER").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("DocumentCollectionId").setType("STRING"));
		fields.add(new TableFieldSchema().setName("CollectionItemId").setType("STRING"));
		fields.add(new TableFieldSchema().setName("Title").setType("STRING"));
		fields.add(new TableFieldSchema().setName("Type").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("Language").setType("STRING"));
		fields.add(new TableFieldSchema().setName("ParseDepth").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("ContentLength").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("Author").setType("STRING"));
		fields.add(new TableFieldSchema().setName("Text").setType("STRING"));
		fields.add(new TableFieldSchema().setName("MainWebResourceHash").setType("STRING"));
		fields.add(new TableFieldSchema().setName("ParentWebResourceHash").setType("STRING"));
		
		List<TableFieldSchema> tagsFields = new ArrayList<>();
		tagsFields.add(new TableFieldSchema().setName("Tag").setType("STRING"));
		tagsFields.add(new TableFieldSchema().setName("Weight").setType("FLOAT"));
		tagsFields.add(new TableFieldSchema().setName("GoodAsTopic").setType("BOOLEAN"));
		fields.add(new TableFieldSchema().setName("Tags").setType("RECORD").setFields(tagsFields).setMode("REPEATED"));

		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}	
	
	
	/**
	 * Setup step {A}
	 * Helper method that defines the BigQuery schema used for the output.
	 */
	private static TableSchema getSentimentSchema() {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("SentimentHash").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("DocumentHash").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("DocumentTime").setType("TIMESTAMP").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("DocumentDateId").setType("INTEGER").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("Text").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("LabelledPositions").setType("STRING"));
		fields.add(new TableFieldSchema().setName("AnnotatedText").setType("STRING"));
		fields.add(new TableFieldSchema().setName("AnnotatedHtml").setType("STRING"));
		fields.add(new TableFieldSchema().setName("SentimentTotalScore").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("DominantValence").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StAcceptance").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StAnger").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StAnticipation").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StAmbiguous").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StDisgust").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StFear").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StGuilt").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StInterest").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StJoy").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StSadness").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StShame").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StSurprise").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StPositive").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StNegative").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StSentiment").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StProfane").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("StUnsafe").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("MainWebResourceHash").setType("STRING"));
		fields.add(new TableFieldSchema().setName("ParentWebResourceHash").setType("STRING"));
		
		List<TableFieldSchema> tagsFields = new ArrayList<>();
		tagsFields.add(new TableFieldSchema().setName("Tag").setType("STRING"));
		tagsFields.add(new TableFieldSchema().setName("GoodAsTopic").setType("BOOLEAN"));
		fields.add(new TableFieldSchema().setName("Tags").setType("RECORD").setFields(tagsFields).setMode("REPEATED"));

		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}	
	
	
	static class LogPipelineOptions extends DoFn<Integer, Void> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			LOG.info("LogPipelineOptions: " + c.getPipelineOptions().toString());
		}			
	}
	
	/**
	 * Pipeline step 2.a
	 * IndexDocument - a ParDo that analyzes just one document at a time
	 * and produces its Sentiment Analysis summary
	 */
	
	static class IndexDocument extends DoFn<InputContent, ContentIndexSummary> {

		@ProcessElement
		public void processElement(ProcessContext c) {

			ContentIndex contentindex = null;
			ContentIndexSummary summary = null;
			InputContent ic = null;
			
			try {
				ic = c.element();
				if (ic == null || ic.text == null || ic.text == "")
					throw new Exception("null or empty document");
				
				long processingTime = System.currentTimeMillis();
				
				contentindex = new ContentIndex(
					ic.text, 
					IndexingConsts.IndexingType.TOPSENTIMENTS,
					IndexingConsts.ContentType.ARTICLE,
					processingTime,
					ic.url,
					ic.pubTime,
					ic.title,
					ic.author,
					ic.documentCollectionId,
					ic.collectionItemId,
					ic.parentUrl,
					ic.parentPubTime);
				
				Indexer.index(contentindex); // Call to the NLP package
				
				summary = contentindex.getContentIndexSummary();
				
				long indexingDuration = System.currentTimeMillis() - processingTime;
				if (indexingDuration > IndexerPipeline.REPORT_LONG_INDEXING_DURATION) {
					LOG.warn("IndexDocument.processElement: Indexing took " + indexingDuration + " milliseconds.");
				    StringBuilder sb = new StringBuilder();
				    LogUtils.printIndex(1, contentindex, sb);
				    String docIndex = sb.toString();
				    LOG.warn("IndexDocument.processElement: Contents of Index: " + docIndex);
				}
				
				if (summary == null)
					throw new Exception("null ContentIndexSummary returned");
				else
					c.output(summary);
				
			} catch (Exception e) {
				LOG.warn("IndexDocument.processElement:" + e.getMessage());
			}
			
		}
	}

	/**
	 * 
	 * ProcessRawInput - a DoFn that extracts attributes like URL, Title, Author from raw text
	 * and puts them into InputContent
	 */
	
	static class ParseRawInput extends DoFn<String,InputContent> {

		@ProcessElement
		public void processElement(ProcessContext c) {

			String rawInput = null;
			InputContent iContent = null;
			
			try {
				rawInput = c.element();
				if (rawInput == null || rawInput == "")
					throw new Exception("ProcessRawInput: null or empty raw content");

				iContent = InputContent.createInputContent(rawInput);

			} catch (Exception e) {
				LOG.warn(e.getMessage());
			}
			
			if (iContent != null) 
				c.output(iContent);
		}
		

	}
	
	/**
	 * 
	 * ParseGDELTJsonInput - a DoFn that extracts attributes like URL, Title, Author from JSON 
	 * in GDELT format and puts them into InputContent
	 */
	
	static class ParseGDELTJsonInput extends DoFn<String,InputContent> {

		@ProcessElement
		public void processElement(ProcessContext c) {

			String rawInput = null;
			InputContent iContent = null;
			
			try {
				rawInput = c.element();
				if (rawInput == null || rawInput == "")
					throw new Exception("ParseGDELTJsonInput: null or empty raw content");

				iContent = InputContent.createInputContentFromGDELTJson(rawInput);
				
				// Skip non-English content for now
				if (!iContent.language.equals("EN"))
					iContent = null;

			} catch (Exception e) {
				LOG.warn(e.getMessage());
			}
			
			if (iContent != null) 
				c.output(iContent);
		}
		

	}
	
	/**
	 * Pipeline step 3
	 * FormatAsTableRowFn - a DoFn for converting a sentiment summary into a BigQuery WebResources record
	 */

	static class CreateTableRowsFromIndexSummaryFn extends DoFn<ContentIndexSummary, TableRow> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			ContentIndexSummary summary = c.element();

			// Create the webresource entry
			Instant pubTime = new Instant(summary.wr.publicationTime);
			Instant proTime = new Instant(summary.wr.processingTime);
			
			TableRow wrrow = new TableRow()
				.set("WebResourceHash", summary.wr.webResourceHash)
				.set("Url", summary.wr.url)
				.set("PublicationTime", pubTime.toString())
				.set("PublicationDateId", summary.wr.publicationDateId)
				.set("ProcessingTime", proTime.toString())
				.set("ProcessingDateId", summary.wr.processingDateId)
				.set("DocumentHash", summary.wr.documentHash);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow, "DocumentCollectionId", summary.wr.documentCollectionId);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow, "CollectionItemId", summary.wr.collectionItemId);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow, "Title", summary.wr.title);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow, "Domain", summary.wr.domain);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow, "Author", summary.wr.author);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow, "ParentWebResourceHash", summary.wr.parentWebResourceHash);

			c.output(wrrow);

			// Create the document entry
			pubTime = new Instant(summary.doc.publicationTime);
			proTime = new Instant(summary.doc.processingTime);
		
			List<TableRow> tags = new ArrayList<>();
			if (summary.doc.tags != null) 
				for (int i=0; i < summary.doc.tags.length; i++) {
					TableRow row = new TableRow();
					row.set("Tag",summary.doc.tags[i].tag);
					row.set("Weight",summary.doc.tags[i].weight);
					IndexerPipelineUtils.setTableRowFieldIfNotNull(row,"GoodAsTopic",summary.doc.tags[i].goodAsTopic);
					tags.add(row);
				}
					
			TableRow drow = new TableRow()
					.set("DocumentHash", summary.doc.documentHash)
					.set("PublicationTime", pubTime.toString())
					.set("PublicationDateId", summary.doc.publicationDateId)
					.set("ProcessingTime", proTime.toString())
					.set("ProcessingDateId", summary.doc.processingDateId);
			
			IndexerPipelineUtils.setTableRowFieldIfNotNull(drow,"DocumentCollectionId", summary.doc.documentCollectionId);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(drow,"CollectionItemId", summary.doc.collectionItemId);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(drow,"Title", summary.doc.title);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(drow,"Type", summary.doc.type.ordinal());
			IndexerPipelineUtils.setTableRowFieldIfNotNull(drow,"Language", summary.doc.language);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(drow,"ParseDepth", summary.doc.contentParseDepth.ordinal());
			IndexerPipelineUtils.setTableRowFieldIfNotNull(drow,"ContentLength", summary.doc.contentLength);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(drow,"Author", summary.wr.author);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(drow,"Text", summary.doc.text);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(drow,"MainWebResourceHash", summary.doc.mainWebResourceHash);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(drow,"ParentWebResourceHash", summary.doc.parentWebResourceHash);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(drow,"Tags", tags);
			
			c.output(documentTag, drow);
					
			if (summary.sentiments != null) {
				for (int i=0; i < summary.sentiments.length; i++)
				{
					List<TableRow> sttags = new ArrayList<>();
					if (summary.sentiments[i].tags != null) 
						for (int j=0; j < summary.sentiments[i].tags.length; j++) {
							TableRow strow = new TableRow();
							strow.set("Tag",summary.sentiments[i].tags[j].tag);
							IndexerPipelineUtils.setTableRowFieldIfNotNull(strow,"GoodAsTopic",summary.sentiments[i].tags[j].goodAsTopic);
							sttags.add(strow);
						}
					
					Instant docTime = new Instant(summary.sentiments[i].documentTime);
					
					TableRow strow = new TableRow()
						.set("SentimentHash", summary.sentiments[i].sentimentHash)
						.set("DocumentHash", summary.sentiments[i].documentHash)
						.set("DocumentTime", docTime.toString())
						.set("DocumentDateId", summary.sentiments[i].documentDateId)
						.set("Text", summary.sentiments[i].text)
						.set("LabelledPositions", summary.sentiments[i].labelledPositions)
						.set("AnnotatedText", summary.sentiments[i].annotatedText)
						.set("AnnotatedHtml", summary.sentiments[i].annotatedHtmlText)
						.set("SentimentTotalScore", summary.sentiments[i].sentimentTotalScore)
						.set("DominantValence", summary.sentiments[i].dominantValence.ordinal())
						.set("StAcceptance", summary.sentiments[i].stAcceptance)
						.set("StAnger", summary.sentiments[i].stAnger)
						.set("StAnticipation", summary.sentiments[i].stAnticipation)
						.set("StAmbiguous", summary.sentiments[i].stAmbiguous)
						.set("StDisgust", summary.sentiments[i].stDisgust)
						.set("StFear", summary.sentiments[i].stFear)
						.set("StGuilt", summary.sentiments[i].stGuilt)
						.set("StInterest", summary.sentiments[i].stInterest)
						.set("StJoy", summary.sentiments[i].stJoy)
						.set("StSadness", summary.sentiments[i].stSadness)
						.set("StShame", summary.sentiments[i].stShame)
						.set("StSurprise", summary.sentiments[i].stSurprise)
						.set("StPositive", summary.sentiments[i].stPositive)
						.set("StNegative", summary.sentiments[i].stNegative)
						.set("StSentiment", summary.sentiments[i].stSentiment)
						.set("StProfane", summary.sentiments[i].stProfane)
						.set("StUnsafe", summary.sentiments[i].stUnsafe);
					
					IndexerPipelineUtils.setTableRowFieldIfNotNull(strow,"MainWebResourceHash", summary.sentiments[i].mainWebResourceHash);
					IndexerPipelineUtils.setTableRowFieldIfNotNull(strow,"ParentWebResourceHash", summary.sentiments[i].parentWebResourceHash);
					IndexerPipelineUtils.setTableRowFieldIfNotNull(strow,"Tags", sttags);

					c.output(sentimentTag, strow);
					
				}
			}
		}
	}

	static class CreateWebresourceTableRowFromInputContentFn extends DoFn<InputContent, TableRow> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			InputContent ic = c.element();
			long processingTime = System.currentTimeMillis();
			
			WebResource wr = new WebResource();
			
			// retrieve the Parent Web Resource Hash, if available
			String parentWebResourceHash = ic.expectedParentWebResourceHash;
			
			wr.initialize(ic.url, ic.pubTime, processingTime, 
				null /*documentHash*/, ic.documentCollectionId, ic.collectionItemId,
				ic.title, ic.author, parentWebResourceHash);

			Instant pubTime = new Instant(wr.publicationTime);
			Instant proTime = new Instant(wr.processingTime);
			
			TableRow wrrow = new TableRow()
				.set("WebResourceHash", wr.webResourceHash)
				.set("Url", wr.url)
				.set("PublicationTime", pubTime.toString())
				.set("PublicationDateId", wr.publicationDateId)
				.set("ProcessingTime", proTime.toString())
				.set("ProcessingDateId", wr.processingDateId)
				.set("DocumentHash", wr.documentHash)
				.set("DocumentCollectionId", wr.documentCollectionId)
				.set("CollectionItemId", wr.collectionItemId)
				.set("Title", wr.title)
				.set("Domain", wr.domain)
				.set("Author", wr.author)
				.set("ParentWebResourceHash", wr.parentWebResourceHash);

			c.output(wrrow);

		}
	}

	/**
	 * 
	 */

	static class GetUrlFn extends DoFn<TableRow, KV<String,Long>> {
		
		@ProcessElement
		public void processElement(ProcessContext c) {
			TableRow row = c.element();
			String url = row.get("Url").toString();
			String processingTime = row.get("ProcessingTime").toString();
			if (url != null && !url.isEmpty())
			{
				Long l = IndexerPipelineUtils.parseDateToLong(IndexerPipelineUtils.dateTimeFormatYMD_HMS_MSTZ, processingTime);
				if (l == null) l = 1L;
				KV<String,Long> kv = KV.of(url, l);
				c.output(kv);
			}
		}
	}

	/**
	 * 
	 */

	static class GetInputContentDocumentHashFn extends DoFn<InputContent, KV<String,InputContent>> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			KV<String,InputContent> kv = KV.of(c.element().expectedDocumentHash, c.element());
			c.output(kv);
		}
	}
	/**
	 * 
	 */

	static class FilterItemsToIndex extends DoFn<InputContent, InputContent> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			InputContent ic = c.element();
			if (ic.skipIndexing == 0)
				c.output(ic);
			else
				c.output(contentNotToIndexTag, ic);
		}
	}
	
	
	/**
	 * 
	 */

	static class EliminateInputContentDupesFn extends DoFn<KV<String,Iterable<InputContent>>, InputContent> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			KV<String, Iterable<InputContent>> kv = c.element();
			String documentHash = kv.getKey();
			Iterable<InputContent> dupes = kv.getValue();
			boolean isFirst = true;	
			for (InputContent ic : dupes) {
				if (isFirst) {
					if (ic.skipIndexing == 0) {
						isFirst = false;
						c.output(ic);
					}
				} else {
					if (ic.skipIndexing == 0) {
						c.output(contentNotToIndexTag,ic);
					}
				}
			}
		}
	}

	private static TableReference getWebResourceTableReference(IndexerPipelineOptions options) {
		TableReference tableRef = new TableReference();
		tableRef.setProjectId(options.getProject());
		tableRef.setDatasetId(options.getBigQueryDataset());
		tableRef.setTableId(IndexerPipelineUtils.WEBRESOURCE_TABLE);
		return tableRef;
	}

	private static TableReference getDocumentTableReference(IndexerPipelineOptions options) {
		TableReference tableRef = new TableReference();
		tableRef.setProjectId(options.getProject());
		tableRef.setDatasetId(options.getBigQueryDataset());
		tableRef.setTableId(IndexerPipelineUtils.DOCUMENT_TABLE);
		return tableRef;
	}
	
	private static TableReference getSentimentTableReference(IndexerPipelineOptions options) {
		TableReference tableRef = new TableReference();
		tableRef.setProjectId(options.getProject());
		tableRef.setDatasetId(options.getBigQueryDataset());
		tableRef.setTableId(IndexerPipelineUtils.SENTIMENT_TABLE);
		return tableRef;
	}
	
	

}
