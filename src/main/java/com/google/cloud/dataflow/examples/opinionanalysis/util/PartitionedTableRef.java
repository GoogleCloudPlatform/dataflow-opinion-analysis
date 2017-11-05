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

/*
 *  Hat tip to Alex Van Boxel https://medium.com/@alexvb for suggested approach
 */

package com.google.cloud.dataflow.examples.opinionanalysis.util;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.Instant;

public class PartitionedTableRef implements SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination> {

	public static final DateTimeFormatter partitionFormatter = DateTimeFormat
			.forPattern("yyyyMMdd")
			.withZoneUTC();
	
    private final String projectId;
    private final String datasetId;
    
    private final String partitionPrefix; // e.g. tablename$
    private final String fieldName; // Instant or Number field that defined the time dimension for partitions 
    private final Boolean isTimeField; // if yes, then extract time and do the conversion to number, otherwise just take the value as partition 

    public static PartitionedTableRef perDay(String projectId, String datasetId, String table, String fieldName, Boolean isTimeField) {
        return new PartitionedTableRef(projectId, datasetId, table + "$", fieldName, isTimeField);
    }

    private PartitionedTableRef(String projectId, String datasetId, String partitionPrefix, String fieldName, Boolean isTimeField) {
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.partitionPrefix = partitionPrefix;
        this.fieldName = fieldName;
        this.isTimeField = isTimeField;
    }

    /**
     * input - a tupel that contains the data element (TableRow), the window, the timestamp, and the pane
     */
    
    @Override
    public TableDestination apply(ValueInSingleWindow<TableRow> input) {
        
    	String partition;
    	
    	if (this.isTimeField) {
	        String sTime = (String) input.getValue().get(this.fieldName);
	        Instant time = Instant.parse(sTime);
	        partition = time.toString(partitionFormatter);
    	} else {
    		partition = ((Integer) input.getValue().get(this.fieldName)).toString();
    	}
    	
        TableReference reference = new TableReference();
        reference.setProjectId(this.projectId);
        reference.setDatasetId(this.datasetId);
        reference.setTableId(this.partitionPrefix + partition);
        return new TableDestination(reference, null);
    }
}