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

import org.apache.beam.sdk.coders.DefaultCoder;

import sirocco.util.HashUtils;
import sirocco.util.IdConverterUtils;

import org.apache.avro.reflect.Nullable;

import org.apache.beam.sdk.coders.AvroCoder;


@DefaultCoder(AvroCoder.class)
public class WebresourceSocialCount {

	public Integer wrPublicationDateId; // Integer in the format YYYYMMDD. Also, partition key.
	public String webResourceHash;
	public Long countTime;
	@Nullable
	public String documentCollectionId; 
	@Nullable
	public String collectionItemId;
	public Integer fbCount;
	public Integer twCount;
	
	public WebresourceSocialCount() {}
	
	public void initialize(Integer wrPublicationDateId,String webResourceHash,Long countTime,String documentCollectionId,
			String collectionItemId,Integer fbCount,Integer twCount) {
		this.wrPublicationDateId = wrPublicationDateId; 
		this.webResourceHash = webResourceHash;
		this.countTime = countTime;	
		this.documentCollectionId = documentCollectionId; 
		this.collectionItemId = collectionItemId;
		this.fbCount = fbCount;
		this.twCount = twCount;
	}
	
	public WebresourceSocialCount(Long pagePubTime, String url, String documentCollectionId, String collectionItemId, Long countTime, 
			Integer countTw, Integer countFb) {

		initialize (
			IdConverterUtils.getDateIdFromTimestamp(pagePubTime),
			HashUtils.getSHA1HashBase64(pagePubTime + url), // Keep in sync with formula in WebResource
			countTime,	
			documentCollectionId, 
			collectionItemId,
			countFb,
			countTw);
				
	}

}
