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
package com.google.cloud.dataflow.examples.opinionanalysis.model;


import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipelineUtils;
import com.google.cloud.dataflow.examples.opinionanalysis.TextWithProperties;




@DefaultCoder(AvroCoder.class)
public class InputContent {

	@Nullable
	public String url;
	@Nullable
	public Long pubTime;
	@Nullable
	public String title;
	@Nullable
	public String author;
	@Nullable
	public String language;
	@Nullable
	public String text;
	@Nullable
	public String documentCollectionId; 
	@Nullable
	public String collectionItemId;
	@Nullable
	public Integer skipIndexing;

	public InputContent() {}

	
	
	
	public InputContent(String url, Long pubTime, String title, String author, String language, String text, 
			String documentCollectionId, String collectionItemId, Integer skipIndexing) {
		this.url = url;
		this.pubTime = pubTime;
		this.title = title;
		this.author = author;
		this.language = language;
		this.text = text;
		this.documentCollectionId = documentCollectionId;
		this.collectionItemId = collectionItemId;
		this.skipIndexing = skipIndexing;
	}
	
	public static InputContent createInputContent(String s) throws Exception
	{

		TextWithProperties t = TextWithProperties.deserialize(s);
		InputContent result = new InputContent();
		result.url = t.properties.get("url");
		result.title = t.properties.get("title");
		result.author = t.properties.get("author");
		result.language = t.properties.get("language");
		result.text = t.text;

		String sPubTime = t.properties.get("pubtime");
		if (sPubTime != null)
			result.pubTime=IndexerPipelineUtils.parseDateToLong(sPubTime);	

		result.documentCollectionId = t.properties.get("collectionid");
		result.collectionItemId = t.properties.get("itemid");
		result.skipIndexing = Integer.decode(t.properties.get("skipindexing"));
		
		return result;		
				
	}

}
