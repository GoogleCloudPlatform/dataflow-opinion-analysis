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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface ControlPipelineOptions extends IndexerPipelineOptions {

	@Description("Whether to use a Pub/Sub topic for control")
	@Default.Boolean(true)
	boolean isControlPubsub();
	void setControlPubsub(boolean value);

	@Description("Whether to use a GCS path for control")
	@Default.Boolean(false)
	boolean isControlGCS();
	void setControlGCS(boolean value);
	
    @Description("Control Pub/Sub topic")
    String getControlPubsubTopic();
    void setControlPubsubTopic(String topic);
	
    @Description("Control GCS path")
    String getControlGCSPath();
    void setControlGCSPath(String path);    
    
    @Description("Value of autoscalingAlgorithm param for initiated Jobs")
    String getJobAutoscalingAlgorithm();
    void setJobAutoscalingAlgorithm(String value);    

    @Description("Value of maxNumWorkers param for initiated Jobs")
    Integer getJobMaxNumWorkers();
    void setJobMaxNumWorkers(Integer value);    

    @Description("Worker machine type for initiated Jobs")
    String getJobWorkerMachineType();
    void setJobWorkerMachineType(String value);    
    
    @Description("Size of PD Disks for initiated Jobs")
    Integer getJobDiskSizeGb();
    void setJobDiskSizeGb(Integer value);    
    
    @Description("Staging location for launched pipelines")
    String getJobStagingLocation();
    void setJobStagingLocation(String location);
    
}
