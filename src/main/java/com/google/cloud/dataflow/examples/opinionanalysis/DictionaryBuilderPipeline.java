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
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.TextIO.Read;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipeline.CreateTableRowsFromIndexSummaryFn;
import com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipelineUtils.ExtractCommentInfoFn;
import com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipelineUtils.ExtractPostDataFn;
import com.google.cloud.dataflow.examples.opinionanalysis.io.RecordFileSource;
import com.google.cloud.dataflow.examples.opinionanalysis.model.InputContent;
import com.google.cloud.dataflow.examples.opinionanalysis.solutions.FileIndexerPipelineOptions;
import com.google.cloud.dataflow.examples.opinionanalysis.transforms.Reshuffle;
import com.google.cloud.dataflow.examples.opinionanalysis.util.PartitionedTableRef;
import com.google.cloud.dataflow.examples.opinionanalysis.util.PipelineTags;

import com.google.cloud.language.v1.Document.Type;
import com.google.cloud.language.v1.EncodingType;
import com.google.cloud.language.v1.Entity;
import com.google.cloud.language.v1.LanguageServiceClient;
import com.google.cloud.language.v1.Sentiment;
import com.google.cloud.language.v1.AnalyzeEntitiesRequest;
import com.google.cloud.language.v1.AnalyzeEntitiesResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;


import sirocco.indexer.Indexer;
import sirocco.indexer.IndexingConsts;
import sirocco.indexer.util.LogUtils;
import sirocco.model.ContentIndex;

import sirocco.model.summary.ContentIndexSummary;
import sirocco.model.summary.Document;
import sirocco.model.summary.DocumentTag;
import sirocco.model.summary.WebResource;

public class DictionaryBuilderPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(IndexerPipeline.class);
	private static final long REPORT_LONG_INDEXING_DURATION = 10000; // Report indexing duration longer than 10s.  
	
	public static void main(String[] args) throws Exception {
		
		IndexerPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IndexerPipelineOptions.class);

	    Pipeline pipeline = createDictionaryBuilderPipeline(options); 
		
		pipeline.run();

	}

	/**
	 * This function creates the DAG graph of transforms. It can be called from main()
	 * as well as from the ControlPipeline.
	 * @param options
	 * @return
	 * @throws Exception
	 */
	public static Pipeline createDictionaryBuilderPipeline(IndexerPipelineOptions options) throws Exception {
		
	    IndexerPipelineUtils.validateIndexerPipelineOptions(options);
		Pipeline pipeline = Pipeline.create(options);
		
		// PHASE: Read raw content from sources
		PCollection<InputContent> readContent = null;
		
		if (options.isSourceGDELTbucket()) {	
			
			readContent = pipeline
				.apply("Read GDELT files", TextIO.read().from(options.getInputFile()))
				.apply("Parse JSON into InputContent", ParDo.of(new ParseGDELTJsonInput()));
		} else if (options.isSourceRedditBQ()) {	
			
			//  Read Reddit Posts and Comments and then join them using CoGroupByKey
			readContent = readRedditPostsAndComments(pipeline, options);
		
		} else if (options.isSourceRedditArchive()) {
			
		} else {
			
			// Read from GCS files
			Read r = TextIO.read().from(options.getInputFile());
			if (options.getRecordDelimiters()!=null && !options.getRecordDelimiters().isEmpty())  
				r = r.withDelimiter(IndexerPipelineUtils.extractRecordDelimiters(options.getRecordDelimiters()));
				
			DoFn<String,InputContent> tr = (options.getReadAsCSV())? 
				new ParseCSVFile() : new ParseRawInput();
					
			readContent = pipeline
				.apply("Read from GCS files", r)
				.apply("Parse input into InputContent objects",ParDo.of(tr)); 
			
		}
		
		
		PCollection<KV<String,Integer>> ngramStats = readContent
			.apply(ParDo.of(new BuildNgramStats()));
		
		/*
		PCollection<KV<String,Integer>> mergedNgramStats = ngramStats
			.apply(Combine.<String, Integer, Integer>perKey(Sum.ofIntegers()));
		*/
		PCollection<KV<String,NgramStatsFn.Stats>> mergedNgramStats = ngramStats
			.apply(Combine.perKey(new NgramStatsFn()));

		
		// Write the results
		if ( options.getBigQueryDataset() != null) {
			
			// Now write to BigQuery
			WriteDisposition dispo = options.getWriteTruncate() ? 
				WriteDisposition.WRITE_TRUNCATE: WriteDisposition.WRITE_APPEND; 
			
			mergedNgramStats
				.apply("Prepare table rows",ParDo.of(new CreateTableRowsFromNgramStatsFn()))
				.apply("Write to statngram table",BigQueryIO.writeTableRows()
					.to(getStatNgramTableRef(options)) 
					.withSchema(getStatNgramSchema())
					.withCreateDisposition(CreateDisposition.CREATE_NEVER)
					.withWriteDisposition(dispo)); 
			
		} else if (options.getOutputFile() != null) {
			
			mergedNgramStats
			.apply(MapElements.via(new FormatAsTextFn()))
			.apply("WriteCounts", TextIO.write().to(options.getOutputFile()));
		}
		
		

		
		return pipeline;
	}

	private static TableSchema getStatNgramSchema() {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("Ngram").setType("STRING"));
		fields.add(new TableFieldSchema().setName("cntOccurences").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("cntDocuments").setType("INTEGER"));
		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}	

	
	private static TableReference getStatNgramTableRef(IndexerPipelineOptions options) {
		
		TableReference reference = new TableReference();
        reference.setProjectId(options.getProject());
        reference.setDatasetId(options.getBigQueryDataset());
        reference.setTableId(IndexerPipelineUtils.STATNGRAM_TABLE);
        return reference;
        

	}
	
	/*
	public class NgramStats{
	    public final Integer sum;
	    public final Integer count;

	    public NgramStats(Integer sum, Integer count) { 
	        this.sum = sum;
	        this.count = count;
	    }
	}
	*/
	
	public static class NgramStatsFn extends CombineFn<Integer, NgramStatsFn.Stats, NgramStatsFn.Stats> {
		
		@DefaultCoder(AvroCoder.class)
		public static class Stats {
			int sum = 0;
			int count = 0;
		}

		@Override
		public Stats createAccumulator() { return new Stats(); }

		@Override
		public Stats addInput(Stats accum, Integer input) {
			accum.sum += input;
			accum.count++;
			return accum;
		}

		@Override
		public Stats mergeAccumulators(Iterable<Stats> accums) {
			Stats merged = createAccumulator();
			for (Stats accum : accums) {
				merged.sum += accum.sum;
				merged.count += accum.count;
			}
			return merged;
		}

		@Override
		public Stats extractOutput(Stats accum) {
			return accum;
		}
	}	
	
	
	static class CreateTableRowsFromNgramStatsFn extends DoFn<KV<String,NgramStatsFn.Stats>, TableRow> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			KV<String,NgramStatsFn.Stats> kv = c.element();
			String ngram = kv.getKey();
			NgramStatsFn.Stats stats = kv.getValue();

			TableRow row = new TableRow()
				.set("Ngram", ngram)
				.set("cntOccurences", stats.sum)
				.set("cntDocuments", stats.count) ; 
			
			c.output(row);
		}
	}
	
	
	/** A SimpleFunction that converts a Word and Count into a printable string. */
	public static class FormatAsTextFn extends SimpleFunction<KV<String, NgramStatsFn.Stats>, String> {
	    @Override
	    public String apply(KV<String, NgramStatsFn.Stats> input) {
	      return input.getKey() + '\t' + input.getValue().sum + '\t' + input.getValue().count ;
	    }
	}
	
	
	private static PCollection<InputContent> readRedditPostsAndComments(Pipeline pipeline, IndexerPipelineOptions options) throws Exception {

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
		
		
		final TupleTag<TableRow> postInfoTag = new TupleTag<TableRow>();
		final TupleTag<TableRow> commentInfoTag = new TupleTag<TableRow>();

		// transform both input collections to tuple collections, where the keys
		// are the post-id
		PCollection<KV<String, TableRow>> postInfo = posts.apply(ParDo.of(new ExtractPostDataFn()));
		PCollection<KV<String, TableRow>> commentInfo = comments.apply(ParDo.of(new ExtractCommentInfoFn()));

		PCollection<KV<String, CoGbkResult>> kvpCollection = 
			KeyedPCollectionTuple
				.of(postInfoTag, postInfo)
				.and(commentInfoTag, commentInfo)
				.apply(CoGroupByKey.<String>create());

		// Process the CoGbkResult elements generated by the CoGroupByKey
		// transform.
		PCollection<InputContent> finalResultCollection = kvpCollection.apply(
				"Create InputContent from Posts and Comments",
				ParDo.of(new DoFn<KV<String, CoGbkResult>, InputContent>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						KV<String, CoGbkResult> e = c.element();

						// The CoGbkResult element contains all the data
						// associated with each unique key
						// from any of the input collections.
						String postId = e.getKey();

						// While we are expecting exactly one post record per
						// key, do some error handling here.
						TableRow post = e.getValue().getOnly(postInfoTag, null);
						if (post == null)
							return;

						// create a list that will hold all InputContent records
						ArrayList<InputContent> postAndCommentList = new ArrayList<InputContent>();

						String postPermalink = post.get("permalink").toString();
						Long postPubTime = IndexerPipelineUtils.extractRedditTime(post.get("created_utc").toString());
						
						/*
						 * sso 11/20/2017: Create two webresource records per post record
						 * 		The first WR record will have the external post URL, 
						 * 		the second one will have the reddit post URL 
						 */
						String postUrl = IndexerPipelineUtils.buildRedditPostUrl(postPermalink);
						String[] postMetaFields = IndexerPipelineUtils.extractRedditPostMetaFields(post);
						
						// Create the first InputContent for the post item itself
						InputContent icPost = new InputContent(/* url */ postUrl, /* pubTime */ postPubTime,
								/* title */ post.get("title").toString(), /* author */ post.get("author").toString(),
								/* language */ null, /* text */ post.get("selftext").toString(),
								/* documentCollectionId */ IndexerPipelineUtils.DOC_COL_ID_REDDIT_FH_BIGQUERY, /* collectionItemId */ postId,
								/* skipIndexing */ 0, /* parentUrl */ null, // the post record will become the beginning of the thread
								/* parentPubTime */ null, /* metaFields */ postMetaFields);

						postAndCommentList.add(icPost);

						// Build a map of Url and Publication Time for the post
						// and each comment
						HashMap<String, Long> pubTimes = new HashMap<String, Long>();
						// seed it with the post, which will be the parent of
						// some of the comments
						// in the chain
						pubTimes.put(postUrl, postPubTime);

						// Take advantage of the fact that all comments of a
						// post are local to this code
						// and build a map of pub times for each comment
						Iterable<TableRow> commentsOfPost = e.getValue().getAll(commentInfoTag);
						for (TableRow comment : commentsOfPost) {

							String commentUrl = IndexerPipelineUtils.buildRedditCommentUrl(postPermalink, comment.get("id").toString());
							Long commentPubTime = IndexerPipelineUtils.extractRedditTime(comment.get("created_utc").toString());
							String commentId = "t1_" + comment.get("id").toString();

							String parentId = comment.get("parent_id").toString();
							String parentUrl = (parentId.startsWith("t1_"))
									? IndexerPipelineUtils.buildRedditCommentUrl(postPermalink, comment.get("id").toString()) : postUrl;
							
							String[] commentMetaFields = IndexerPipelineUtils.extractRedditCommentMetaFields(comment);		
									
							InputContent icComment = new InputContent(/* url */ commentUrl,
								/* pubTime */ commentPubTime, /* title */ null,
								/* author */ comment.get("author").toString(), /* language */ null,
								/* text */ comment.get("body").toString(),
								/* documentCollectionId */ IndexerPipelineUtils.DOC_COL_ID_REDDIT_FH_BIGQUERY,
								/* collectionItemId */ commentId, /* skipIndexing */ 0, /* parentUrl */ parentUrl,
								/* parentPubTime */ null, // don't set time yet, because we might not have read that record yet
								/* metaFields */ commentMetaFields
							);

							pubTimes.put(commentUrl, commentPubTime); // save the pub time of the current comment
							postAndCommentList.add(icComment); // add comment to the list
						}

						// iterate through all posts and comments and populate
						// the Parent pub times
						for (InputContent ic : postAndCommentList) {
							if (ic.parentUrl != null)
								ic.parentPubTime = pubTimes.get(ic.parentUrl);
							c.output(ic);
						}

					}
				}));

		return finalResultCollection;
	}
	
	
	/**
	 * 
	 * BuildNgramStats - a ParDo that analyzes just one document at a time
	 * and produces its Sentiment Analysis summary
	 */
	
	static class BuildNgramStats extends DoFn<InputContent, KV<String,Integer>> {

		@ProcessElement
		public void processElement(ProcessContext c) {

			ContentIndex contentindex = null;
			ContentIndexSummary summary = null;
			InputContent ic = null;
			IndexerPipelineOptions options = c.getPipelineOptions().as(IndexerPipelineOptions.class);
			IndexingConsts.ContentType contentType = options.getIndexAsShorttext() ? IndexingConsts.ContentType.SHORTTEXT: IndexingConsts.ContentType.ARTICLE;
			
			try {
				long processingTime = System.currentTimeMillis();

				ic = c.element();
				
				contentindex = new ContentIndex(
					ic.text, IndexingConsts.IndexingType.NGRAMSTATS, 
					contentType, processingTime);
				
				contentindex.NgramMaxN = 5;
				contentindex.NgramBreakAtPunctuation = true;

				Indexer.index(contentindex); // Call to the NLP package
				
				if (!contentindex.IsIndexingSuccessful)
					throw new Exception(contentindex.IndexingErrors + ". Text: "+ic.text);
				
				long indexingDuration = System.currentTimeMillis() - processingTime;
				if (indexingDuration > REPORT_LONG_INDEXING_DURATION) {
					LOG.warn("BuildNgramStats.processElement: Indexing took " + indexingDuration + " milliseconds.");
				    StringBuilder sb = new StringBuilder();
				    LogUtils.printIndex(1, contentindex, sb);
				    String docIndex = sb.toString();
				    LOG.warn("BuildNgramStats.processElement: Contents of Index ["+indexingDuration+" ms]: " + docIndex);
				}
				
				HashMap<String,Integer> stats = contentindex.NgramStats;
				
				if (stats == null)
					throw new Exception("null NgramStats returned");
				else
					for (Entry<String,Integer> kvp : contentindex.NgramStats.entrySet())
						c.output(KV.of(kvp.getKey(), kvp.getValue()));
				
			} catch (Exception e) {
				LOG.warn("BuildNgramStats.processElement:",e);
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

	public static class ParseCSVFile extends DoFn<String,InputContent> {

		@ProcessElement
		public void processElement(ProcessContext c) {

			String rawInput = null;
			InputContent iContent = null;
			
			try {
				rawInput = c.element();
				if (rawInput == null)
					throw new Exception("ParseCSVFile: null raw content");
				
				
				FileIndexerPipelineOptions options = c.getPipelineOptions().as(FileIndexerPipelineOptions.class);
				Integer textColumnIdx = options.getTextColumnIdx();
				Integer collectionItemIdIdx = options.getCollectionItemIdIdx();
				
				InputStreamReader isr = new InputStreamReader(IOUtils.toInputStream(rawInput,StandardCharsets.UTF_8.name()));
				
				Iterable<CSVRecord> records = CSVFormat.DEFAULT
					.withFirstRecordAsHeader()
					.parse(isr);
				
				for (CSVRecord record : records) {
					
					String text = record.get(textColumnIdx);
					String documentCollectionId = IndexerPipelineUtils.DOC_COL_ID_CSV_FILE;
					String collectionItemId = (collectionItemIdIdx!=null)? record.get(collectionItemIdIdx): null;
					
					InputContent ic = new InputContent(
						null /*url*/, null /*pubTime*/, null /*title*/, null /*author*/, null /*language*/, 
						text, documentCollectionId, collectionItemId, 0 /*skipIndexing*/);					
					
					c.output(ic);
				}
				

			} catch (Exception e) {
				LOG.warn(e.getMessage());
			}
		}
		

	}
	
	/**
	 * 
	 * Use in the future, when we are able to parallelize import at the record file source
	 * @author sezok
	 *
	 */
	static class ParseCSVLine extends DoFn<String,InputContent> {

		/*
		@Setup
		public void setup(){
		}
		
		@Teardown
		public void teardown(){
		}
		*/
		
		@ProcessElement
		public void processElement(ProcessContext c) {

			String rawInput = null;
			InputContent iContent = null;
			
			try {
				rawInput = c.element();
				if (rawInput == null)
					throw new Exception("ParseCSVLine: null raw content");
				rawInput = rawInput.trim();
				if (rawInput.isEmpty())
					throw new Exception("ParseCSVLine: empty raw content or whitespace chars only");
				
				FileIndexerPipelineOptions options = c.getPipelineOptions().as(FileIndexerPipelineOptions.class);
				Integer textColumnIdx = options.getTextColumnIdx();
				Integer collectionItemIdIdx = options.getCollectionItemIdIdx();
				
				InputStreamReader isr = new InputStreamReader(IOUtils.toInputStream(rawInput,StandardCharsets.UTF_8.name()));
				
				Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(isr);
				
				for (CSVRecord record : records) { // should only be one record, but handle multi-record case as well
					
					String text = record.get(textColumnIdx);
					String documentCollectionId = IndexerPipelineUtils.DOC_COL_ID_CSV_FILE;
					String collectionItemId = record.get(collectionItemIdIdx);
					
					InputContent ic = new InputContent(
						null /*url*/, null /*pubTime*/, null /*title*/, null /*author*/, null /*language*/, 
						text, documentCollectionId, collectionItemId, 0 /*skipIndexing*/);					
					
					c.output(ic);
				}
				

			} catch (Exception e) {
				LOG.warn(e.getMessage());
			}
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
	

	
	
	

}