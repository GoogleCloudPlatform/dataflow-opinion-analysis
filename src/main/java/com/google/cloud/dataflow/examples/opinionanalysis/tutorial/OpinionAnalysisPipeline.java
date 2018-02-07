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
package com.google.cloud.dataflow.examples.opinionanalysis.tutorial;

import com.fasterxml.jackson.databind.deser.DataFormatReaders.Match;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipelineOptions;
import com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipelineUtils;
import com.google.cloud.dataflow.examples.opinionanalysis.io.RecordFileSource;
import com.google.cloud.dataflow.examples.opinionanalysis.model.InputContent;
import com.google.common.collect.Iterables;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.Read;
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
import sirocco.model.summary.Document;
import sirocco.model.summary.DocumentTag;
import sirocco.model.summary.WebResource;
import sirocco.util.HashUtils;

import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSet;


public class OpinionAnalysisPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(OpinionAnalysisPipeline.class);

	static final long REPORT_LONG_INDEXING_DURATION = 10000; // Report indexing duration longer than 10s.  
	static final String EMPTY_TITLE_KEY_PREFIX = "No Title"; // Used in text dedupe grouping.  
	
	static final TupleTag<TableRow> webresourceTag = new TupleTag<TableRow>(){};
	static final TupleTag<TableRow> documentTag = new TupleTag<TableRow>(){};
	static final TupleTag<TableRow> sentimentTag = new TupleTag<TableRow>(){};
	

	public static void main(String[] args) throws Exception {
		
		IndexerPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IndexerPipelineOptions.class);

	    Pipeline pipeline = createNLPPipeline(options); 
		
		pipeline.run();

	}

	/**
	 * This function creates the DAG graph of transforms. It can be called from main()
	 * as well as from the ControlPipeline.
	 * @param options
	 * @return
	 * @throws Exception
	 */
	public static Pipeline createNLPPipeline(IndexerPipelineOptions options) throws Exception {
		
	    IndexerPipelineUtils.validateIndexerPipelineOptions(options);
		Pipeline pipeline = Pipeline.create(options);
		
		PCollection<InputContent> readContent;
		PCollection<String> rawInput;
		
		if (options.isStreaming()) {
			
			// Continuously read from a Pub/Sub topic
			rawInput = pipeline.apply("Read from PubSub", 
				PubsubIO.readStrings().fromTopic(
					options.getPubsubTopic())); 
			
		
		} else {
			// Read from GCS files

			rawInput = pipeline.apply("Read from GCS files", 
				Read.from(new RecordFileSource<String>(
					ValueProvider.StaticValueProvider.of(options.getInputFile()), 
					StringUtf8Coder.of(), 
					RecordFileSource.DEFAULT_RECORD_SEPARATOR)));
		}
	
		readContent = rawInput.apply(ParDo.of(new ParseRawInput()));
		
		// Extract opinions from online opinions
		PCollection<ContentIndexSummary> indexes = readContent
			.apply(ParDo.of(new IndexDocument())) 
			.setCoder(AvroCoder.of(ContentIndexSummary.class));
		

		// Write into BigQuery 
		PCollectionTuple bqrows= indexes
			.apply(ParDo.of(new CreateTableRowsFromIndexSummaryFn())
				.withOutputTags(webresourceTag, // main output collection
					TupleTagList.of(documentTag).and(sentimentTag)) // 2 side output collections
				); 
		
		PCollection<TableRow> webresourceRows = bqrows.get(webresourceTag);
		PCollection<TableRow> documentRows = bqrows.get(documentTag);
		PCollection<TableRow> sentimentRows = bqrows.get(sentimentTag);

		// Append or Overwrite
		WriteDisposition dispo = options.getWriteTruncate() ? 
				WriteDisposition.WRITE_TRUNCATE: WriteDisposition.WRITE_APPEND; 
		
			
		webresourceRows
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
		
		fields.add(new TableFieldSchema().setName("Signals").setType("STRING").setMode("REPEATED"));

		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
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
			IndexerPipelineOptions options = c.getPipelineOptions().as(IndexerPipelineOptions.class);
			IndexingConsts.ContentType contentType = options.getIndexAsShorttext() ? IndexingConsts.ContentType.SHORTTEXT: IndexingConsts.ContentType.ARTICLE;
			
			try {
				ic = c.element();
				if (ic == null || ic.text == null || ic.text.isEmpty())
					throw new Exception("null or empty document");
				
				long processingTime = System.currentTimeMillis();
				
				contentindex = new ContentIndex(
					ic.text, 
					IndexingConsts.IndexingType.TOPSENTIMENTS,
					contentType,
					processingTime,
					ic.url,
					ic.pubTime,
					ic.title,
					ic.author,
					ic.documentCollectionId,
					ic.collectionItemId,
					ic.parentUrl,
					ic.parentPubTime,
					ic.metaFields);
				
				Indexer.index(contentindex); // Call to the NLP package
				
				summary = contentindex.getContentIndexSummary();
				
				long indexingDuration = System.currentTimeMillis() - processingTime;
				if (indexingDuration > OpinionAnalysisPipeline.REPORT_LONG_INDEXING_DURATION) {
					LOG.warn("IndexDocument.processElement: Indexing took " + indexingDuration + " milliseconds.");
				    StringBuilder sb = new StringBuilder();
				    LogUtils.printIndex(1, contentindex, sb);
				    String docIndex = sb.toString();
				    LOG.warn("IndexDocument.processElement: Contents of Index ["+indexingDuration+" ms]: " + docIndex);
				}
				
				if (summary == null)
					throw new Exception("null ContentIndexSummary returned");
				else
					c.output(summary);
				
			} catch (Exception e) {
				// LOG.warn("IndexDocument.processElement:" + e.getMessage());
				LOG.warn("IndexDocument.processElement:",e);
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
				if (rawInput == null)
					throw new Exception("ProcessRawInput: null raw content");
				rawInput = rawInput.trim();
				if (rawInput.isEmpty())
					throw new Exception("ProcessRawInput: empty raw content or whitespace chars only");
				iContent = InputContent.createInputContent(rawInput);

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
				.set("PublicationTime", pubTime.toString())
				.set("PublicationDateId", summary.wr.publicationDateId)
				.set("ProcessingTime", proTime.toString())
				.set("ProcessingDateId", summary.wr.processingDateId)
				.set("DocumentHash", summary.wr.documentHash);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow, "Url", summary.wr.url);
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
					
					IndexerPipelineUtils.setTableRowFieldIfNotNull(strow,"Signals", summary.sentiments[i].signals);

					c.output(sentimentTag, strow);
					
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
