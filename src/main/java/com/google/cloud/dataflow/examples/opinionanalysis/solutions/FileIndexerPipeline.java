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

package com.google.cloud.dataflow.examples.opinionanalysis.solutions;


import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipeline;
import com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipelineOptions;
import com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipelineUtils;
import com.google.cloud.dataflow.examples.opinionanalysis.io.RecordFileSource;
import com.google.cloud.dataflow.examples.opinionanalysis.model.InputContent;
import com.google.cloud.dataflow.examples.opinionanalysis.util.PipelineTags;

import com.google.cloud.language.v1.Document.Type;
import com.google.cloud.language.v1.EncodingType;
import com.google.cloud.language.v1.Entity;
import com.google.cloud.language.v1.LanguageServiceClient;
import com.google.cloud.language.v1.Sentiment;
import com.google.cloud.language.v1.AnalyzeEntitiesRequest;
import com.google.cloud.language.v1.AnalyzeEntitiesResponse;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import sirocco.indexer.Indexer;
import sirocco.indexer.IndexingConsts;
import sirocco.indexer.util.LogUtils;
import sirocco.model.ContentIndex;

import sirocco.model.summary.ContentIndexSummary;
import sirocco.model.summary.DocumentTag;


public class FileIndexerPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(FileIndexerPipeline.class);
	private static final long REPORT_LONG_INDEXING_DURATION = 10000; // Report indexing duration longer than 10s.  
	
	public static void main(String[] args) throws Exception {
		
		FileIndexerPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FileIndexerPipelineOptions.class);

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
	public static Pipeline createIndexerPipeline(FileIndexerPipelineOptions options) throws Exception {
		
	    IndexerPipelineUtils.validateIndexerPipelineOptions(options);
		Pipeline pipeline = Pipeline.create(options);
		
		// PHASE: Read raw content from sources
		
		PCollection<InputContent> readContent = pipeline
				.apply("Read entire CSV file", org.apache.beam.sdk.io.Read.from(new RecordFileSource<String>(
					ValueProvider.StaticValueProvider.of(options.getInputFile()), 
					StringUtf8Coder.of(), RecordFileSource.DEFAULT_RECORD_SEPARATOR))) //
				.apply("Parse CSV file into InputContent objects", ParDo.of(new IndexerPipeline.ParseCSVFile()));
		
		// Define the accumulators of all filters
		PCollection<InputContent> contentToIndex = readContent;
		
		// PHASE: Index documents (extract opinions and entities/tags). 
		// Return successfully indexed docs, and create a Bigtable write transform to store errors 
		// in Dead Letter table.
		PCollection<ContentIndexSummary> indexes = indexDocuments(options, contentToIndex);
		
		if (options.getRatioEnrichWithCNLP() > 0)
			indexes = enrichWithCNLP(indexes, options.getRatioEnrichWithCNLP());
		
		// PHASE: Write to BigQuery
		// For the Indexes that are unique ("filteredIndexes"), create records in webresource, document, and sentiment.
		// Then, merge resulting webresources with webresourceRowsUnindexed and webresourceDeduped
		indexes
			.apply(ParDo.of(new CreateCSVLineFromIndexSummaryFn()))
			.apply(TextIO.write()
				.to(options.getOutputFile()));
		
		
		return pipeline;
	}

	
	
	
	/**
	 * @param indexes
	 * @return
	 */
	private static PCollection<ContentIndexSummary> enrichWithCNLP(
			PCollection<ContentIndexSummary> indexes, Float ratio) {
		
		PCollectionTuple splitAB = indexes
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

		indexes = allIndexSummaries;
		return indexes;
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
	 * Pipeline step 3
	 * FormatAsTableRowFn - a DoFn for converting a sentiment summary into a BigQuery WebResources record
	 */

	static class CreateCSVLineFromIndexSummaryFn extends DoFn<ContentIndexSummary, String> {
		
		@ProcessElement
		public void processElement(ProcessContext c) {
			
			ContentIndexSummary summary = c.element();

			if (summary.sentiments == null) 
				return;
			
			try {
			
				StringWriter stringWriter = new StringWriter();
				CSVPrinter csvPrinter = new CSVPrinter(stringWriter,CSVFormat.DEFAULT);
					
				for (int i=0; i < summary.sentiments.length; i++)
				{
					
					ArrayList<String> linefields = new ArrayList<String>();
					
					addField(linefields,"RecordID",summary.doc.collectionItemId);
	
					ArrayList<String> sttags = new ArrayList<>();
					if (summary.sentiments[i].tags != null) 
						for (int j=0; j < summary.sentiments[i].tags.length; j++)
							sttags.add(summary.sentiments[i].tags[j].tag);
					
					addField(linefields,"Tags",sttags.toString()); // will write as [a,b,c]
					
					addField(linefields,"SentimentHash", summary.sentiments[i].sentimentHash);
					addField(linefields,"Text", summary.sentiments[i].text);
					addField(linefields,"LabelledPositions", summary.sentiments[i].labelledPositions);
					addField(linefields,"AnnotatedText", summary.sentiments[i].annotatedText);
					addField(linefields,"AnnotatedHtml", summary.sentiments[i].annotatedHtmlText);
					addField(linefields,"SentimentTotalScore", summary.sentiments[i].sentimentTotalScore);
					addField(linefields,"DominantValence", summary.sentiments[i].dominantValence.ordinal());
					addField(linefields,"StAcceptance", summary.sentiments[i].stAcceptance);
					addField(linefields,"StAnger", summary.sentiments[i].stAnger);
					addField(linefields,"StAnticipation", summary.sentiments[i].stAnticipation);
					addField(linefields,"StAmbiguous", summary.sentiments[i].stAmbiguous);
					addField(linefields,"StDisgust", summary.sentiments[i].stDisgust);
					addField(linefields,"StFear", summary.sentiments[i].stFear);
					addField(linefields,"StGuilt", summary.sentiments[i].stGuilt);
					addField(linefields,"StInterest", summary.sentiments[i].stInterest);
					addField(linefields,"StJoy", summary.sentiments[i].stJoy);
					addField(linefields,"StSadness", summary.sentiments[i].stSadness);
					addField(linefields,"StShame", summary.sentiments[i].stShame);
					addField(linefields,"StSurprise", summary.sentiments[i].stSurprise);
					addField(linefields,"StPositive", summary.sentiments[i].stPositive);
					addField(linefields,"StNegative", summary.sentiments[i].stNegative);
					addField(linefields,"StSentiment", summary.sentiments[i].stSentiment);
					addField(linefields,"StProfane", summary.sentiments[i].stProfane);
					addField(linefields,"StUnsafe", summary.sentiments[i].stUnsafe);
					
					ArrayList<String> signalsarray = new ArrayList<>();
					if (summary.sentiments[i].signals != null) 
						for (int j=0; j < summary.sentiments[i].signals.length; j++)
							signalsarray.add(summary.sentiments[i].signals[j]);
					
					addField(linefields,"Signals",signalsarray.toString());
					
					csvPrinter.printRecord(linefields);
					
					String output = stringWriter.toString().trim(); // need to trim, because printRecord will add the record separator, as will the TextIO.write method at the end of the pipeline
					csvPrinter.flush(); // will also flush the stringWriter
					
					c.output(output);
					
					
				}
				
				csvPrinter.close();
			} catch (IOException e) {
				LOG.warn(e.getMessage());
			}
		}
		
		private void addField(ArrayList<String> fields, String fieldName, String value) {
			fields.add(value);
			// TODO: should we quote the string?
		}
		private void addField(ArrayList<String> fields, String fieldName, Integer value) {
			fields.add(value.toString());
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
				if (indexingDuration > FileIndexerPipeline.REPORT_LONG_INDEXING_DURATION) {
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
				// int entitiesToGet = Math.min(is.doc.tags.length, response.getEntitiesList().size());
				int entitiesToGet = response.getEntitiesList().size();
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
