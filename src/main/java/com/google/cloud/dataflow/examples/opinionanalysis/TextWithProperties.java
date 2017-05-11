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

import java.io.BufferedReader;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class TextWithProperties {
	
	public String text = null;

	public HashMap<String,String> properties;
	
	public TextWithProperties() {
		properties = new HashMap<String,String>();
	}

	private void initialize(String s) throws Exception {
		
		boolean done = false;
		boolean lineConsumed = false;
		BufferedReader reader = new BufferedReader(new StringReader(s));
		String line = null;
		
		// Read the properties first. One property per line, divided by =. Comment lines needs to start with #
		
		while(!done) {
			line = reader.readLine();
			
			if (line == null) {
				done = true;
				continue;
			}
			
			if (line.startsWith("#")) {
				lineConsumed = true;
				continue;
			}
			
			int equalsPos = line.indexOf("=");
			
			if (equalsPos >= 0) {
				String leftOf = line.substring(0, equalsPos).toLowerCase();
				String rightOf = line.substring(equalsPos+1, line.length());
				lineConsumed = true;
				this.properties.put(leftOf,rightOf);
			} else{
				done = true;
				lineConsumed = false;
			}
		}
		char[] caRest = new char[s.length()];
		
		reader.read(caRest);
		String remainingText = (!lineConsumed && line!=null?line+"\n":"") + new String(caRest);
		this.text = remainingText;
		
		
	}
	
	public static TextWithProperties deserialize(String s) throws Exception {
		
		TextWithProperties result  = new TextWithProperties();
		result.initialize(s);
		return result;
		
	}
	
	//TODO: Move the date parsing functions to DateUtils
	public static Long parseDate(String format, String s){

		Long result = null;
		
		try {
			SimpleDateFormat formatter = new SimpleDateFormat(format);
			Date date = formatter.parse(s);
			result = date.getTime();
		} catch (Exception e) {
			result = null;	
		}
		return result;
	}
	
	
}
