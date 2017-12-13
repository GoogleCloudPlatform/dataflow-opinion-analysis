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
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
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
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipelineUtils.ExtractCommentInfoFn;
import com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipelineUtils.ExtractPostDataFn;
import com.google.cloud.dataflow.examples.opinionanalysis.io.RecordFileSource;
import com.google.cloud.dataflow.examples.opinionanalysis.model.InputContent;
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

public class IndexerPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(IndexerPipeline.class);
	private static final long REPORT_LONG_INDEXING_DURATION = 10000; // Report indexing duration longer than 10s.  
	private static final String EMPTY_TITLE_KEY_PREFIX = "No Title"; // Used in text dedupe grouping.  
	
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
		
		// PHASE: Read raw content from sources
		PCollection<InputContent> readContent = null;
		
		if (options.isSourcePubsub()) {
			
			readContent = pipeline
				.apply(PubsubIO.readStrings().fromTopic(options.getPubsubTopic())) 
				.apply(ParDo.of(new ParseRawInput()));
		} else if (options.isSourceJDBC()){
			
			readContent = pipeline
				.apply(readDBRows(options));
		} else if (options.isSourceRedditBQ()) {	
			
			//  Read Reddit Posts and Comments and then join them using CoGroupByKey
			readContent = joinRedditPostsAndComments(pipeline, options);
		} else if (options.isSourceGDELTbucket()) {	
			
			readContent = pipeline
				.apply("Read GDELT files", TextIO.read().from(options.getInputFile()))
				.apply("Parse JSON into InputContent", ParDo.of(new ParseGDELTJsonInput()));
		} else {
			
			// Read from GCS files
			readContent = pipeline
				.apply("Read from GCS files", org.apache.beam.sdk.io.Read.from(new RecordFileSource<String>(
					ValueProvider.StaticValueProvider.of(options.getInputFile()), 
					StringUtf8Coder.of(), RecordFileSource.DEFAULT_RECORD_SEPARATOR)))
				.apply(ParDo.of(new ParseRawInput())); 
		}
		
		// PHASE: Filter already processed URLs
		// If we are supposed to truncate destination tables before write, then don't do any
		// extra filtering based on what is in the destination tables.
		// Otherwise, obtain a cache of already processed URLs and remove from indexing set 
		// the items that are already in the destination Bigquery tables
		PCollection<InputContent> contentToProcess = options.getWriteTruncate() ? readContent:
			filterAlreadyProcessedUrls(readContent, pipeline, options);
		
		// PHASE: Filter by a special Skip flag in the input records
		// Split the remaining items into items to index and items just to create as webresource
		// based on skipIndexing flag
		ContentToIndexOrNot contentPerSkipFlag = filterBasedOnSkipFlag(contentToProcess);
		
		PCollection<InputContent> contentToIndexNotSkipped = contentPerSkipFlag.contentToIndex;
		PCollection<InputContent> contentNotToIndexSkipped = contentPerSkipFlag.contentNotToIndex;

		
		// Define the accumulators of all filters
		PCollection<InputContent> contentToIndex = null;
		PCollection<InputContent> contentNotToIndex = null;

		
		// PHASE: If we were instructed to de-duplicate based on exact wording of documents,
		// split items in main flow based on whether they are dupes or not
		if (options.getDedupeText()) {

			ContentToIndexOrNot content = filterAlreadyProcessedDocuments(
				contentToIndexNotSkipped,
				contentNotToIndexSkipped, pipeline, options);
			
			contentToIndex = content.contentToIndex;
			contentNotToIndex = content.contentNotToIndex;
			
		} else {
			contentToIndex = contentToIndexNotSkipped;
			contentNotToIndex = contentNotToIndexSkipped;
		}

		// Process content that does not need to be indexed and just needs to be stored as a webresource
		PCollection<TableRow> webresourceRowsUnindexed = contentNotToIndex
			.apply(ParDo.of(new CreateWebresourceTableRowFromInputContentFn()));
		
		// PHASE: Index documents (extract opinions and entities/tags). 
		// Return successfully indexed docs, and create a Bigtable write transform to store errors 
		// in Dead Letter table.
		PCollection<ContentIndexSummary> indexes = indexDocuments(options, contentToIndex);
		
		PCollection<ContentIndexSummary> filteredIndexes = null;
		PCollection<TableRow> webresourceDeduped = null;
		
		// PHASE: Filter "soft" duplicates
		// After Indexing, do another grouping by Title, Round(Length/1000), and Tags
		// This grouping needs to happen after the "indexing" operation because we will be using Tags identified
		// by indexing as one of the grouping elements. 
		// This type of grouping and filtering will catch small variations in text (e.g. in copyright notices, bylines, etc) 
		if (options.getDedupeText()) {

			ContentDuplicateOrNot contentDuplicateOrNot = filterSoftDuplicates(indexes);
			
			filteredIndexes = contentDuplicateOrNot.uniqueIndexes;
			webresourceDeduped = contentDuplicateOrNot.duplicateWebresources;
			
		} else {
			filteredIndexes = indexes;
		}
		
		//PHASE: Enrich with CloudNLP entities

		if (options.getRatioEnrichWithCNLP() > 0)
			filteredIndexes = enrichWithCNLP(filteredIndexes, options.getRatioEnrichWithCNLP());
		
		// PHASE: Write to BigQuery
		// For the Indexes that are unique ("filteredIndexes"), create records in webresource, document, and sentiment.
		// Then, merge resulting webresources with webresourceRowsUnindexed and webresourceDeduped
		PCollectionTuple bqrows= filteredIndexes
			.apply(ParDo.of(new CreateTableRowsFromIndexSummaryFn())
				.withOutputTags(PipelineTags.webresourceTag, // main output collection
					TupleTagList.of(PipelineTags.documentTag).and(PipelineTags.sentimentTag))); // 2 side output collections
		
		writeAllTablesToBigQuery(bqrows, webresourceRowsUnindexed, webresourceDeduped, options);
		
		return pipeline;
	}

	/**
	 * @param filteredIndexes
	 * @return
	 */
	private static PCollection<ContentIndexSummary> enrichWithCNLP(
			PCollection<ContentIndexSummary> filteredIndexes, Float ratio) {
		
		PCollectionTuple splitAB = filteredIndexes
			.apply(ParDo.of(new SplitAB(ratio))
				.withOutputTags(PipelineTags.BranchA,  
					TupleTagList.of(PipelineTags.BranchB))); 
		
		PCollection<ContentIndexSummary> branchACol = splitAB.get(PipelineTags.BranchA);
		PCollection<ContentIndexSummary> branchBCol = splitAB.get(PipelineTags.BranchB);
		
		PCollection<ContentIndexSummary> enrichedBCol = branchBCol.apply(
			ParDo.of(new EnrichWithCNLPEntities()));
		
		//Merge all collections with WebResource table records
		PCollectionList<ContentIndexSummary> contentIndexSummariesList = 
			PCollectionList.of(branchACol).and(enrichedBCol);
		PCollection<ContentIndexSummary> allIndexSummaries = 
			contentIndexSummariesList.apply(Flatten.<ContentIndexSummary>pCollections());

		filteredIndexes = allIndexSummaries;
		return filteredIndexes;
	}

	/**
	 * @param options
	 * @param contentToIndex
	 * @return
	 */
	private static PCollection<ContentIndexSummary> indexDocuments(
			IndexerPipelineOptions options,
			PCollection<InputContent> contentToIndex) {
		
		PCollectionTuple alldocuments = contentToIndex
			.apply(ParDo.of(new IndexDocument())
				.withOutputTags(PipelineTags.successfullyIndexed, // main output
					TupleTagList.of(PipelineTags.unsuccessfullyIndexed))); // side output

		PCollection<ContentIndexSummary> indexes = alldocuments
				.get(PipelineTags.successfullyIndexed)
				.setCoder(AvroCoder.of(ContentIndexSummary.class));

		// if the Bigtable admin DB is set, write into dead letter table
		if (options.getBigtableIndexerAdminDB() != null) {
			
			
			PCollection<InputContent> unprocessedDocuments = alldocuments
				.get(PipelineTags.unsuccessfullyIndexed);
			
			BigtableOptions.Builder optionsBuilder =
				new BigtableOptions.Builder()
					.setProjectId(options.getProject())
					.setInstanceId(options.getBigtableIndexerAdminDB());
			BigtableOptions bigtableOptions = optionsBuilder.build();
			
			unprocessedDocuments
				.apply(ParDo.of(new CreateDeadLetterEntries()))
				.apply("Write to Dead Letter table in Bigtable", BigtableIO.write()
						.withBigtableOptions(bigtableOptions)
						.withTableId(IndexerPipelineUtils.DEAD_LETTER_TABLE));
		}
		
		return indexes;
	}


	/**
	 * @param contentToProcess
	 * @return
	 */
	private static ContentToIndexOrNot filterBasedOnSkipFlag(PCollection<InputContent> contentToProcess) {
		PCollectionTuple indexOrNotBasedOnSkipFlag = contentToProcess
			.apply("Filter items to index based on skipIndexing flag", ParDo.of(new FilterItemsToIndex())
				.withOutputTags(PipelineTags.contentToIndexNotSkippedTag, // main output collection
					TupleTagList.of(PipelineTags.contentNotToIndexSkippedTag))); // side output collection		

		
		ContentToIndexOrNot contentPerSkipFlag = new ContentToIndexOrNot(
			indexOrNotBasedOnSkipFlag.get(PipelineTags.contentToIndexNotSkippedTag), 
			indexOrNotBasedOnSkipFlag.get(PipelineTags.contentNotToIndexSkippedTag));
		
		return contentPerSkipFlag;
	}

	/**
	 * @param contentToIndexNotSkipped
	 * @param contentNotToIndexSkipped
	 * @param pipeline
	 * @param options
	 * @return
	 */
	private static ContentToIndexOrNot filterAlreadyProcessedDocuments(
			PCollection<InputContent> contentToIndexNotSkipped, PCollection<InputContent> contentNotToIndexSkipped,
			Pipeline pipeline, IndexerPipelineOptions options) {
		PCollection<KV<String,Long>> alreadyProcessedDocs = null;
		
		if (!options.getWriteTruncate()) {
			String query = IndexerPipelineUtils.buildBigQueryProcessedDocsQuery(options);
			alreadyProcessedDocs = pipeline
				.apply("Get already processed Documents",BigQueryIO.read().fromQuery(query))
				.apply(ParDo.of(new GetDocumentHashFn()));

		} else {
			Map<String, Long> map = new HashMap<String,Long>();
			alreadyProcessedDocs = pipeline
				.apply("Create empty side input of Docs",
					Create.of(map).withCoder(KvCoder.of(StringUtf8Coder.of(),VarLongCoder.of())));
		}			
		
		final PCollectionView<Map<String,Long>> alreadyProcessedDocsSideInput =  
			alreadyProcessedDocs.apply(View.<String,Long>asMap());
		
		PCollectionTuple indexOrNotBasedOnExactDupes = contentToIndexNotSkipped
			.apply("Extract DocumentHash key", ParDo.of(new GetInputContentDocumentHashFn()))
			.apply("Group by DocumentHash key", GroupByKey.<String, InputContent>create())
			.apply("Eliminate InputContent Dupes", ParDo.of(new EliminateInputContentDupes(alreadyProcessedDocsSideInput))
				.withSideInputs(alreadyProcessedDocsSideInput)
				.withOutputTags(PipelineTags.contentToIndexNotExactDupesTag, // main output collection
					TupleTagList.of(PipelineTags.contentNotToIndexExactDupesTag))); // side output collection	
		
		PCollection<InputContent> contentToIndexNotExactDupes = indexOrNotBasedOnExactDupes.get(PipelineTags.contentToIndexNotExactDupesTag);
		PCollection<InputContent> contentNotToIndexExactDupes = indexOrNotBasedOnExactDupes.get(PipelineTags.contentNotToIndexExactDupesTag);
		
		// Merge the sets of items that are dupes or skipped
		PCollectionList<InputContent> contentNotToIndexList = PCollectionList.of(contentNotToIndexExactDupes).and(contentNotToIndexSkipped);
		
		ContentToIndexOrNot content = new ContentToIndexOrNot(contentToIndexNotExactDupes, contentNotToIndexList.apply(Flatten.<InputContent>pCollections()));
		return content;
	}

	/**
	 * @param options
	 * @param pipeline
	 * @param readContent
	 * @return
	 */
	private static PCollection<InputContent> filterAlreadyProcessedUrls(
			PCollection<InputContent> readContent, Pipeline pipeline, 
			IndexerPipelineOptions options) {
		PCollection<InputContent> contentToProcess;
		String query = IndexerPipelineUtils.buildBigQueryProcessedUrlsQuery(options);
		PCollection<KV<String,Long>> alreadyProcessedUrls = pipeline
			.apply("Get processed URLs",BigQueryIO.read().fromQuery(query))
			.apply(ParDo.of(new GetUrlFn()));

		final PCollectionView<Map<String,Long>> alreadyProcessedUrlsSideInput =
			alreadyProcessedUrls.apply(View.<String,Long>asMap());
		  
		contentToProcess = readContent
			.apply(ParDo.of(new FilterProcessedUrls(alreadyProcessedUrlsSideInput))
				.withSideInputs(alreadyProcessedUrlsSideInput));
		return contentToProcess;
	}

	/**
	 * @param Document indexes
	 * @return a POJO containing 2 PCollections: Unique docs, and Duplicates
	 */
	private static ContentDuplicateOrNot filterSoftDuplicates(
			PCollection<ContentIndexSummary> indexes) {
		// 
		PCollectionTuple dedupeOrNot = indexes
			.apply("Extract Text grouping key", 
				ParDo.of(new GetContentIndexSummaryKeyFn()))
			.apply("Group by Text grouping key", 
				GroupByKey.<ContentSoftDeduplicationKey, ContentIndexSummary>create())
			.apply("Eliminate Text dupes", 
				ParDo.of(new EliminateTextDupes())
					.withOutputTags(PipelineTags.indexedContentNotToDedupeTag, 
						TupleTagList.of(PipelineTags.indexedContentToDedupeTag))); 	
			
		PCollection<TableRow> dedupedWebresources = 
			dedupeOrNot.get(PipelineTags.indexedContentToDedupeTag)
				.apply(ParDo.of(new CreateWebresourceTableRowFromDupeIndexSummaryFn()));
		
		ContentDuplicateOrNot contentDuplicateOrNot = new ContentDuplicateOrNot(
			dedupeOrNot.get(PipelineTags.indexedContentNotToDedupeTag),
			dedupedWebresources);
		
		return contentDuplicateOrNot;
	}
	
	
	/**
	 * @param options
	 * @return PTransform that reads from a JDBC source
	 */
	private static org.apache.beam.sdk.io.jdbc.JdbcIO.Read<InputContent> readDBRows(IndexerPipelineOptions options) {
		
		String query = IndexerPipelineUtils.buildJdbcSourceImportQuery(options);
		
		return JdbcIO.<InputContent>read()
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
		    .withCoder(AvroCoder.of(InputContent.class));
	}	
	
	
	
	/**
	 * Join two collections, using post id as the join key Sample:
	 * https://github.com/GoogleCloudPlatform/DataflowJavaSDK-examples/blob/master/src/main/java/com/google/cloud/dataflow/examples/cookbook/JoinExamples.java
	 */
	private static PCollection<InputContent> joinRedditPostsAndComments(Pipeline pipeline, IndexerPipelineOptions options) throws Exception {

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
	 * @param bqrows
	 * @param webresourceRowsUnindexed
	 * @param webresourceDeduped
	 * @param options
	 */
	private static void writeAllTablesToBigQuery(PCollectionTuple bqrows,
			PCollection<TableRow> webresourceRowsUnindexed, PCollection<TableRow> webresourceDeduped,
			IndexerPipelineOptions options) {
		PCollection<TableRow> webresourceRows = bqrows.get(PipelineTags.webresourceTag);
		PCollection<TableRow> documentRows = bqrows.get(PipelineTags.documentTag);
		PCollection<TableRow> sentimentRows = bqrows.get(PipelineTags.sentimentTag);

		// Now write to BigQuery
		WriteDisposition dispo = options.getWriteTruncate() ? 
			WriteDisposition.WRITE_TRUNCATE: WriteDisposition.WRITE_APPEND; 
		
		//Merge all collections with WebResource table records
		PCollectionList<TableRow> webresourceRowsList = (webresourceDeduped == null) ?
			PCollectionList.of(webresourceRows).and(webresourceRowsUnindexed) :
			PCollectionList.of(webresourceRows).and(webresourceRowsUnindexed).and(webresourceDeduped);
				
		PCollection<TableRow> allWebresourceRows = 
			webresourceRowsList.apply(Flatten.<TableRow>pCollections());
				
		allWebresourceRows = !options.isStreaming() ? 
			allWebresourceRows.apply("Reshuffle Webresources", new Reshuffle<TableRow>()) : 
			allWebresourceRows;
		
		allWebresourceRows
			.apply("Write to webresource", 
				BigQueryIO.writeTableRows()
					.to(getWebResourcePartitionedTableRef(options)) 
					.withSchema(getWebResourceSchema())
					.withCreateDisposition(CreateDisposition.CREATE_NEVER)
					.withWriteDisposition(dispo)); 
		
		documentRows = !options.isStreaming() ?
			documentRows.apply("Reshuffle Documents", new Reshuffle<TableRow>()):
			documentRows;
				
		documentRows
			.apply("Write to document", 
				BigQueryIO.writeTableRows()
					.to(getDocumentPartitionedTableRef(options))
					.withSchema(getDocumentTableSchema())
					.withCreateDisposition(CreateDisposition.CREATE_NEVER)
					.withWriteDisposition(dispo)); 
		
		sentimentRows = !options.isStreaming() ?
			sentimentRows.apply("Reshuffle Sentiments", new Reshuffle<TableRow>()):
			sentimentRows;
				
		sentimentRows
			.apply("Write to sentiment", 
				BigQueryIO.writeTableRows()
					.to(getSentimentPartitionedTableRef(options)) 
					.withSchema(getSentimentSchema())
					.withCreateDisposition(CreateDisposition.CREATE_NEVER)
					.withWriteDisposition(dispo));
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
		fields.add(new TableFieldSchema().setName("MetaFields").setType("STRING").setMode("REPEATED"));

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

	
	static class ContentToIndexOrNot {
		public final PCollection<InputContent> contentToIndex;
		public final PCollection<InputContent> contentNotToIndex;
		
		public ContentToIndexOrNot(PCollection<InputContent> contentToIndex, PCollection<InputContent> contentNotToIndex){
			this.contentToIndex = contentToIndex;
			this.contentNotToIndex = contentNotToIndex;
		}
	}

	static class ContentDuplicateOrNot {
		public final PCollection<ContentIndexSummary> uniqueIndexes;
		public final PCollection<TableRow> duplicateWebresources;
		
		public ContentDuplicateOrNot(PCollection<ContentIndexSummary> uniqueIndexes, PCollection<TableRow> duplicateWebresources){
			this.uniqueIndexes = uniqueIndexes;
			this.duplicateWebresources = duplicateWebresources;
		}
	}
	

	static class LogPipelineOptions extends DoFn<Integer, Void> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			LOG.info("LogPipelineOptions: " + c.getPipelineOptions().toString());
		}			
	}

	/**
	 * Create  items to be stored in Bigtable dead letter table unprocessed-documents
	 * @author sezok
	 *
	 */
	static class CreateDeadLetterEntries extends DoFn<InputContent, KV<ByteString, Iterable<Mutation>>> {
		
		@ProcessElement
		public void processElement(ProcessContext c) {
			InputContent i = c.element();
			String jobName = c.getPipelineOptions().getJobName();
			ByteString rowkey = ByteString.copyFromUtf8(jobName + "#" + i.expectedDocumentHash); 
			ByteString value = ByteString.copyFromUtf8(i.text);
			
			Iterable<Mutation> mutations =
				ImmutableList.of(Mutation.newBuilder()
					.setSetCell(
						Mutation.SetCell.newBuilder()
							.setFamilyName(IndexerPipelineUtils.DEAD_LETTER_TABLE_ERR_CF)
							.setColumnQualifier(ByteString.copyFromUtf8("text"))
							.setValue(value)
					)
	                .build());
			
			c.output(KV.of(rowkey, mutations));			
		}
	}
	
	
	/**
	 * Check in the map if we already processed this Url, and if we haven't, 
	 * add the input content to the list that needs to be processed 
	 * @author sezok
	 *
	 */
	static class FilterProcessedUrls extends DoFn<InputContent, InputContent> {
		
		final PCollectionView<Map<String,Long>> alreadyProcessedUrlsSideInput;
		
		public FilterProcessedUrls(PCollectionView<Map<String,Long>> si) {
			this.alreadyProcessedUrlsSideInput = si;
		}
		
		@ProcessElement
		public void processElement(ProcessContext c) {
			InputContent i = c.element();
			Long proTime = c.sideInput(this.alreadyProcessedUrlsSideInput).get(i.url);
			if (proTime == null)
				c.output(i);
		}
	}
	

	static class EliminateInputContentDupes extends DoFn<KV<String,Iterable<InputContent>>, InputContent> {
		
		final PCollectionView<Map<String,Long>> alreadyProcessedDocsSideInput;
		
		public EliminateInputContentDupes(PCollectionView<Map<String,Long>> si) {
			this.alreadyProcessedDocsSideInput = si;
		}
		
		@ProcessElement
		public void processElement(ProcessContext c) {
			KV<String, Iterable<InputContent>> kv = c.element();
			String documentHash = kv.getKey();
			Iterable<InputContent> dupes = kv.getValue();
			boolean isFirst = true;
			int groupSize = Iterables.size(dupes);
			for (InputContent ic : dupes) {
	
				// Check if this doc was already processed and stored in BQ
				Map<String,Long> sideInputMap = c.sideInput(alreadyProcessedDocsSideInput);
				Long proTime = sideInputMap.get(ic.expectedDocumentHash);
				if (proTime!=null) {
					c.output(PipelineTags.contentNotToIndexExactDupesTag,ic);
					continue;
				}
				
				if (isFirst) {
					isFirst = false;
					c.output(ic);
				} else {
					c.output(PipelineTags.contentNotToIndexExactDupesTag,ic);
				}
			}
		}
	}
	
	
	
	/**
	 * EliminateTextDupes - a ParDo that takes a group of text documents and selects one that 
	 * will represent all of them
	 */
	
	static class EliminateTextDupes extends DoFn<KV<ContentSoftDeduplicationKey,Iterable<ContentIndexSummary>>, ContentIndexSummary> {

		@ProcessElement
		public void processElement(ProcessContext c) {
			KV<ContentSoftDeduplicationKey, Iterable<ContentIndexSummary>> kv = c.element();
			ContentSoftDeduplicationKey key = kv.getKey();
			Iterable<ContentIndexSummary> group = kv.getValue();
											
			// Calculate stats for Tags and determine the shortest text
			HashMap<String,Integer> tagStats = new HashMap<String,Integer>();
			Integer minLength = Integer.MAX_VALUE;
			Integer groupSize = Iterables.size(group);

			// for single element document groups stop the checks right here
			if (groupSize == 1) {
				c.output(group.iterator().next());
				return;
			}
			
			for (ContentIndexSummary is : group) {
				// build tag stats
				for (DocumentTag dt : is.doc.tags){
					Integer i = tagStats.get(dt.tag);
					if (i == null)
						tagStats.put(dt.tag, 1);
					else
						tagStats.put(dt.tag, i + 1);
				}
			}
											
			// Iterate through the group again, this time checking for passing criteria by Tags
			// For a tag to count as a good match, it needs to occur in half the documents of the group
			Integer minTagOccurences = Math.max(Math.round(groupSize/2),2);
			ContentIndexSummary shortestMatch = null;
			ArrayList<ContentIndexSummary> indexesToRemap = new ArrayList<ContentIndexSummary>();
			
			for (ContentIndexSummary is : group) {
				Integer matchedTags = 0;
				Integer totalTags = is.doc.tags.length;
				for (DocumentTag dt : is.doc.tags){
					Integer tagOcc = tagStats.get(dt.tag);
					if (tagOcc >= minTagOccurences)
						matchedTags++;
				}
				
				Float matchedRatio = ((float) matchedTags / (float) totalTags);
				
				if (matchedRatio >= 0.5 && matchedTags >= 2) {
					// Documents that have an acceptable match on tags should be remapped to a single document
					// The shortest doc becomes the winning document, and all other docs will be
					// remapped to this document
					if (is.doc.contentLength < minLength) {
						if (shortestMatch != null)
							indexesToRemap.add(shortestMatch); // push the previous shortie to the list of remaps
						shortestMatch = is;
						minLength = is.doc.contentLength;
					} else {
						// For now add the current doc to a list, because we might not have hit the 
						// shortest document yet 
						indexesToRemap.add(is);
					}
				} else {
					// this doc does not pass the deduplication criteria, so release it into the main output
					c.output(is);
				}
			}
			
			if (shortestMatch!=null) {
				c.output(shortestMatch); // the shortest match goes to main output
				String shortestMatchHash = shortestMatch.doc.documentHash;
				for (ContentIndexSummary is : indexesToRemap) {
					KV<String,ContentIndexSummary> kvRemap = KV.of(shortestMatchHash,is);
					c.output(PipelineTags.indexedContentToDedupeTag,kvRemap);
				}
			}
			
		}
		
		
	}
	
	
	/**
	 * 
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
				long processingTime = System.currentTimeMillis();

				ic = c.element();
				
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
				
				if (!contentindex.IsIndexingSuccessful)
					throw new Exception(contentindex.IndexingErrors + ". Text: "+ic.text);
				
				summary = contentindex.getContentIndexSummary();
				
				long indexingDuration = System.currentTimeMillis() - processingTime;
				if (indexingDuration > IndexerPipeline.REPORT_LONG_INDEXING_DURATION) {
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
				LOG.warn("IndexDocument.processElement:",e);
				c.output(PipelineTags.unsuccessfullyIndexed, ic);
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
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow, "MetaFields", summary.wr.metaFields);
			
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
			
			c.output(PipelineTags.documentTag, drow);
					
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

					c.output(PipelineTags.sentimentTag, strow);
					
				}
			}
		}
	}

	
	static class CreateWebresourceTableRowFromDupeIndexSummaryFn extends DoFn<KV<String,ContentIndexSummary>, TableRow> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			KV<String,ContentIndexSummary> kv = c.element();
			String newDocumentHash = kv.getKey();
			ContentIndexSummary summary = kv.getValue();

			// Create the webresource entry
			Instant pubTime = new Instant(summary.wr.publicationTime);
			Instant proTime = new Instant(summary.wr.processingTime);

			// TODO: we are leaving summary.wr.collectionItemId, summary.wr.publicationDateId unchanged for now 
			// These values are different from the Document to which we repointed the WebResource
			// It could be a good or a bad thing, depending on the circumstances
			
			TableRow wrrow = new TableRow()
				.set("WebResourceHash", summary.wr.webResourceHash)
				.set("Url", summary.wr.url)
				.set("PublicationTime", pubTime.toString())
				.set("PublicationDateId", summary.wr.publicationDateId)
				.set("ProcessingTime", proTime.toString())
				.set("ProcessingDateId", summary.wr.processingDateId)
				.set("DocumentHash", newDocumentHash); // replace the original DocumentHash with the passed value
			
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow, "DocumentCollectionId", summary.wr.documentCollectionId);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow, "CollectionItemId", summary.wr.collectionItemId);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow, "Title", summary.wr.title);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow, "Domain", summary.wr.domain);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow, "Author", summary.wr.author);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow, "ParentWebResourceHash", summary.wr.parentWebResourceHash);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow, "MetaFields", summary.wr.metaFields);

			c.output(wrrow);
		}
	}
	
	static class CreateWebresourceTableRowFromInputContentFn extends DoFn<InputContent, TableRow> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			InputContent ic = c.element();
			long processingTime = System.currentTimeMillis();
			
			WebResource wr = new WebResource();
			
			// retrieve the Parent Web Resource Hash and Document Hash, if available
			String parentWebResourceHash = ic.expectedParentWebResourceHash;
			String documentHash = ic.expectedDocumentHash; 
			
			wr.initialize(ic.url, ic.pubTime, processingTime, 
				documentHash, ic.documentCollectionId, ic.collectionItemId,
				ic.title, ic.author, parentWebResourceHash, ic.metaFields);

			Instant pubTime = new Instant(wr.publicationTime);
			Instant proTime = new Instant(wr.processingTime);
			
			TableRow wrrow = new TableRow()
				.set("WebResourceHash", wr.webResourceHash)
				.set("Url", wr.url)
				.set("PublicationTime", pubTime.toString())
				.set("PublicationDateId", wr.publicationDateId)
				.set("ProcessingTime", proTime.toString())
				.set("ProcessingDateId", wr.processingDateId)
				.set("DocumentHash", wr.documentHash);
			
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow,"DocumentCollectionId", wr.documentCollectionId);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow,"CollectionItemId", wr.collectionItemId);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow,"Title", wr.title);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow,"Domain", wr.domain);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow,"Author", wr.author);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow,"ParentWebResourceHash", wr.parentWebResourceHash);
			IndexerPipelineUtils.setTableRowFieldIfNotNull(wrrow,"MetaFields", wr.metaFields);

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
			String url = IndexerPipelineUtils.getTableRowStringFieldIfNotNull(row,"Url");
			String processingTime = IndexerPipelineUtils.getTableRowStringFieldIfNotNull(row,"ProcessingTime");
			if (url != null && !url.isEmpty())
			{
				Long l = IndexerPipelineUtils.parseDateToLong(IndexerPipelineUtils.dateTimeFormatYMD_HMS_MSTZ, processingTime);
				if (l == null) l = 1L;
				KV<String,Long> kv = KV.of(url, l);
				c.output(kv);
			}
		}
	}

	static class GetDocumentHashFn extends DoFn<TableRow, KV<String,Long>> {
		
		@ProcessElement
		public void processElement(ProcessContext c) {
			TableRow row = c.element();
			String documentHash = row.get("DocumentHash").toString();
			String processingTime = row.get("ProcessingTime").toString();
			if (documentHash != null && !documentHash.isEmpty())
			{
				Long l = IndexerPipelineUtils.parseDateToLong(IndexerPipelineUtils.dateTimeFormatYMD_HMS_MSTZ, processingTime);
				if (l == null) l = 1L;
				KV<String,Long> kv = KV.of(documentHash, l);
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
	 * Re-implementation of GetContentIndexSummaryKeyFn that produces a composite key 
	 * ala https://cloud.google.com/blog/big-data/2017/08/guide-to-common-cloud-dataflow-use-case-patterns-part-2#pattern-groupby-using-multiple-data-properties
	 */
	
	static class GetContentIndexSummaryKeyFn extends DoFn<ContentIndexSummary, KV<ContentSoftDeduplicationKey,ContentIndexSummary>> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			String origTitle = c.element().doc.title;
			String title = (origTitle == null) || origTitle.isEmpty() ? EMPTY_TITLE_KEY_PREFIX : origTitle;
			Integer contentLengthType = Math.round ( c.element().doc.contentLength / 1000);
			ContentSoftDeduplicationKey key = new ContentSoftDeduplicationKey(title, contentLengthType);
			KV<ContentSoftDeduplicationKey,ContentIndexSummary> kv = KV.of(key, c.element());
			c.output(kv);
		}
	}

	@DefaultCoder(AvroCoder.class)
	static class ContentSoftDeduplicationKey {
		public String title;
		public Integer contentLengthType;
		
		public ContentSoftDeduplicationKey() {}
		
		public ContentSoftDeduplicationKey (String title, Integer contentLengthType) {
			this.title = title;
			this.contentLengthType = contentLengthType;
		}
	}
	
	
	/**
	 * Original implementation of GetContentIndexSummaryKeyFn that produces concatenated string [title + "-" Math.round(contentLength/1000)]
	 */

	/*
	static class GetContentIndexSummaryKeyFn extends DoFn<ContentIndexSummary, KV<String,ContentIndexSummary>> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			String title = c.element().doc.title;
			Integer contentLength = c.element().doc.contentLength;
			String key = (((title == null) || title.isEmpty()) ? EMPTY_TITLE_KEY_PREFIX : title) + " + " + Math.round(contentLength/1000);
			KV<String,ContentIndexSummary> kv = KV.of(key, c.element());
			c.output(kv);
		}
	}
	*/
	
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
				c.output(PipelineTags.contentNotToIndexSkippedTag, ic);
		}
	}
	
	
	private static PartitionedTableRef getWebResourcePartitionedTableRef(IndexerPipelineOptions options) {
		
		return PartitionedTableRef.perDay(
			options.getProject(), options.getBigQueryDataset(),
			IndexerPipelineUtils.WEBRESOURCE_TABLE,
			"PublicationDateId", false);
	}

	private static PartitionedTableRef getDocumentPartitionedTableRef(IndexerPipelineOptions options) {
		
		return PartitionedTableRef.perDay(
			options.getProject(), options.getBigQueryDataset(),
			IndexerPipelineUtils.DOCUMENT_TABLE,
			"PublicationDateId", false);
	}
	
	private static PartitionedTableRef getSentimentPartitionedTableRef(IndexerPipelineOptions options) {
		
		return PartitionedTableRef.perDay(
			options.getProject(), options.getBigQueryDataset(),
			IndexerPipelineUtils.SENTIMENT_TABLE,
			"DocumentDateId", false);
	}
	
	
	/**
	 * Call CloudNLP
	 *
	 */
	static class EnrichWithCNLPEntities extends DoFn<ContentIndexSummary, ContentIndexSummary> {

		private LanguageServiceClient languageClient;

		@StartBundle
		public void startBundle(){
			try {
				this.languageClient = LanguageServiceClient.create();
			} catch (Exception e) {
				LOG.warn(e.getMessage());
			}
			
		}
		
		@FinishBundle
		public void finishBundle(){
			if (this.languageClient == null)
				return;
			
			try {
				this.languageClient.close();
			} catch (Exception e) {
				LOG.warn(e.getMessage());
			}
		}

		@ProcessElement
		public void processElement(ProcessContext c) {
			ContentIndexSummary is = c.element();

			try {

				if (this.languageClient == null)
					throw new Exception("CNLP client not initialized");
				
				com.google.cloud.language.v1.Document doc = com.google.cloud.language.v1.Document.newBuilder()
					.setContent(is.doc.text).setType(Type.PLAIN_TEXT).build();

				AnalyzeEntitiesRequest request = AnalyzeEntitiesRequest.newBuilder()
					.setDocument(doc).setEncodingType(EncodingType.UTF16).build();

				AnalyzeEntitiesResponse response = languageClient.analyzeEntities(request);
				
				// get at most as many entities as we have tags in the Sirocco-based output
				int entitiesToGet = Math.min(is.doc.tags.length, response.getEntitiesList().size());
				DocumentTag[] newTags = new DocumentTag[entitiesToGet];
				
				// Create additional Document Tags and add them to the output index summary
				for (int idx = 0; idx < entitiesToGet; idx++) {
					// Entities are sorted by salience in the response list, so pick the first ones
					Entity entity = response.getEntitiesList().get(idx);
					DocumentTag dt = new DocumentTag();
					String tag = IndexerPipelineUtils.CNLP_TAG_PREFIX + entity.getName();
					Float weight = entity.getSalience();
					Boolean goodAsTopic = null;
					dt.initialize(tag, weight, goodAsTopic);
					newTags[idx] = dt;
				}
				
				if (entitiesToGet>0)
				{
					ContentIndexSummary iscopy = is.copy();
					DocumentTag[] combinedTags = new DocumentTag[newTags.length + iscopy.doc.tags.length];
					System.arraycopy(iscopy.doc.tags, 0, combinedTags, 0, iscopy.doc.tags.length);
					System.arraycopy(newTags, 0, combinedTags, iscopy.doc.tags.length, newTags.length);
					iscopy.doc.tags = combinedTags;
					c.output(iscopy);
				}
				else
					c.output(is);
				
			} catch (Exception e) {
				LOG.warn(e.getMessage());
			}

		}
	}	
	
	
	/**
	 * Splits incoming collection into A (main output) and B (side output)
	 * 
	 *
	 */
	static class SplitAB extends DoFn<ContentIndexSummary, ContentIndexSummary> {
		
		/**
		 * bRatio - Ratio of elements to route to "B" side output.
		 * Needs to be a float value between 0 and 1.
		 */
		private final Float bRatio;
		private final int threshold;
		private transient ThreadLocalRandom random;

		
		public SplitAB(Float bRatio) {
			this.bRatio = (bRatio < 0) ? 0: (bRatio < 1)? bRatio : 1; // valid values are between 0 and 1
			this.threshold = (int) (((float) Integer.MAX_VALUE) * this.bRatio);
		}
		
		@StartBundle
		public void startBundle() {
			random = ThreadLocalRandom.current();
		}
		
		@ProcessElement
		public void processElement(ProcessContext c) {
			ContentIndexSummary i = c.element();
			int dice = random.nextInt(Integer.MAX_VALUE);
			
			if (dice > this.threshold)
				c.output(i);
			else
				c.output(PipelineTags.BranchB, i);
		}
	}
	
	
	

}
