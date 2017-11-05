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

package com.google.cloud.dataflow.examples.opinionanalysis.util;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.examples.opinionanalysis.model.InputContent;

import sirocco.model.summary.ContentIndexSummary;

/**
 * To aid in moving transform out of the main pipeline class
 * all input/output tags have been defined in a separate class
 * 
 * @author sezok
 *
 */
public class PipelineTags {

	public static final TupleTag<TableRow> webresourceTag = new TupleTag<TableRow>(){};
	public static final TupleTag<TableRow> documentTag = new TupleTag<TableRow>(){};
	public static final TupleTag<TableRow> sentimentTag = new TupleTag<TableRow>(){};
	
	public static final TupleTag<InputContent> contentToIndexNotSkippedTag = new TupleTag<InputContent>(){};
	public static final TupleTag<InputContent> contentNotToIndexSkippedTag = new TupleTag<InputContent>(){};
	
	public static final TupleTag<InputContent> contentToIndexNotExactDupesTag = new TupleTag<InputContent>(){};
	public static final TupleTag<InputContent> contentNotToIndexExactDupesTag = new TupleTag<InputContent>(){};

	public static final TupleTag<KV<String,ContentIndexSummary>> indexedContentToDedupeTag = new TupleTag<KV<String,ContentIndexSummary>> (){};
	public static final TupleTag<ContentIndexSummary> indexedContentNotToDedupeTag = new TupleTag<ContentIndexSummary>(){};

	public static final TupleTag<ContentIndexSummary> successfullyIndexed = new TupleTag<ContentIndexSummary>(){};
	public static final TupleTag<InputContent> unsuccessfullyIndexed = new TupleTag<InputContent>(){};

	public static final TupleTag<ContentIndexSummary> BranchA = new TupleTag<ContentIndexSummary>(){};
	public static final TupleTag<ContentIndexSummary> BranchB = new TupleTag<ContentIndexSummary>(){};
	
	
}
