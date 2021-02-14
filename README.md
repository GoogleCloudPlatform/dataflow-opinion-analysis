# Sample: Opinion Analysis of News, Threaded Conversations, and User Generated Content
This sample uses Cloud Dataflow to build an opinion analysis processing pipeline for
news, threaded conversations in forums like Hacker News, Reddit, or Twitter and
other user generated content e.g. email.

Opinion Analysis can be used for lead generation purposes, user research, or 
automated testimonial harvesting.

## About the sample

This sample contains three types of artifacts:

* Cloud Dataflow pipelines for ingesting and indexing textual data from sources such as relational databases, files, BigQuery datasets, and Pub/Sub topics
* BigQuery dataset (with schema definitions and some metadata) to receive the results of the Dataflow Opinion Analysis pipelines, as well as additional transformations (via Materialized Views) to calculate trends
* Jupyter Notebooks for creating Tensorflow models that use Sirocco-based textual embeddings as features in prediction models

## Major Changes in current and past Releases

### Version 0.7 
- In this version we began the task of updating pipelines to more recent versions of Apache Beam SDK. Version 0.6 relied on Beam 2.2.0, version 0.7 bumps the Beam SDK to a more recent one.
- We moved away from orchestrating pipelines by using an AppEngine-based solution. Pipeline orchestration is best done with Airflow or Cloud Composer
- We also stopped calculating trends in BigQuery by running Dataflow pipelines using embedded SQL. BigQuery Materialized Views as well as BigQuery Scheduled Queries are the more modern solution to this task

## How to run the sample
The steps for configuring and running this sample are as follows:

- Setup your Google Cloud Platform project and permissions.
- Install tools necessary for compiling and deploying the code in this sample.
- Create and setup a Cloud Storage bucket and Cloud Pub/Sub topics.
- Create or verify a configuration for your project.
- Clone the sample code
- Create the BigQuery dataset
- Deploy the Dataflow pipelines
- Clean up

### Prerequisites

Setup your Google Cloud Platform project and permissions

* Select or Create a Google Cloud Platform project.
  In the [Google Cloud Console](https://console.cloud.google.com/project), select
  **Create Project**.

* [Enable billing](https://cloud.google.com/billing/docs/how-to/modify-project) for your project, if you haven't done so during the project creation.

* [Enable](https://console.cloud.google.com/flows/enableapi?apiid=dataflow,compute_component,logging,storage_component,storage_api,bigquery,pubsub) the Google Dataflow, Compute Engine, Google Cloud Storage, and other APIs necessary to run the example. 

Install tools necessary for compiling and deploying the code in this sample, if not already on your system, specifically git, Java and Maven:

* Install [`git`](https://git-scm.com/downloads). If you have Homebrew, the command is
```
brew install git
```

* Download and install the [Java Development Kit (JDK)](http://www.oracle.com/technetwork/java/javase/downloads/index.html) version 1.8 or later. Verify that the JAVA_HOME environment variable is set and points to your JDK installation.

* [Download](http://maven.apache.org/download.cgi) and [install](http://maven.apache.org/install.html) Apache Maven. With Homebrew, the command is:
```
brew install maven
```

Install the Google Cloud SDK

* [Download and install the Google Cloud SDK](http://cloud.google.com/sdk/).


Create and setup a Cloud Storage bucket and Cloud Pub/Sub topics

* [Create a Cloud Storage bucket](https://console.cloud.google.com/storage/browser) for your project. This bucket will be used for staging your code, as well as for temporary input/output files. For consistency with this sample, select Multi-Regional storage class and United States location.

* Create folders in this bucket `staging`, `input`, `output`, `temp`

* (Optional) [Create](https://console.cloud.google.com/cloudpubsub/topicList) the following Pub/Sub topic: `documents`. This topic can be used together with a streaming Dataflow pipeline. You can send textual documents to that topic, and the Dataflow Indexing pipeline will process these documents as they arrive.

(Optional) Create or verify a configuration for your project

By now you have already created a configuration, e.g. when you initiated the Google Cloud SDK. Now is another chance to change your mind and create a new configuration.

* Authenticate with the Cloud Platform. Run the following command to get [Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials).

  `gcloud auth application-default login`

* Create a new configuration for your project if it does not exist already

  `gcloud init`

Verify your configuration

* Verify that the active configuration is the one you want to use

  `gcloud config configurations list`

Important: This tutorial uses several billable components of Google Cloud Platform. New Cloud Platform users may be eligible for a [free trial](http://cloud.google.com/free-trial).


### Clone the sample code

Go to the directory where you typically store your git repos.

To clone the GitHub repository to your computer, run the following command:

```
git clone https://github.com/GoogleCloudPlatform/dataflow-opinion-analysis
```

Go to the `dataflow-opinion-analysis` directory. The exact path depends on where you placed the directory when you cloned the sample files from GitHub.

```
cd dataflow-opinion-analysis
```

### Activate gcloud configuration and set environment variables

Do this step before creating the BigQuery dataset and before running your demo Dataflow jobs every time you open a new shell. 

* Check what configurations are currently available on your machine

 `gcloud config configurations list`
 
* Activate the gcloud configuration for the project where your BigQuery dataset and your Dataflow jobs are or should be located

```
gcloud config configurations activate <config-name>
```

* [One Time Task] Go to the `dataflow-opinion-analysis/scripts` directory and make a copy of the `set_env_vars_template.sh` file

```
cd scripts
cp set_env_vars_template.sh set_env_vars_local.sh
chmod +x *.sh
```

* [One Time Task] Edit the `set_env_vars_local.sh` file in your favorite text editor, e.g. `nano`. Specifically, set the values of the variables used for parametarizing your Dataflow pipeline. Set the value of DATASET_ID to the name of a BigQuery dataset that you want to keep your analysis results in (this dataset does not have to exist yet, we will create it in later steps). A good DATASET_ID is "opinions". Set GCS_BUCKET to the name of the GCS bucket that you created previously. Note that the UNSUPPORTED_SDK_OVERRIDE_TOKEN variable should only be set once you have a real token to replace it with (see below for more info).

* Set environment variables for the rest of your shell session

Don't miss the dot at the beginning of this command!

```
. ./set_env_vars_local.sh
```

* Return to the root directory of the repo

```
cd ..
```


### Create the BigQuery dataset

* Go to the `bigquery` directory where the build scripts and schema files for BigQuery tables and views are located

  `cd bigquery`

* Make sure that the test scripts are executable

  `chmod +x *.sh`

* Run the `build_dataset.sh` script to create the dataset, tables, and views. The script will use the PROJECT_ID variable from your active gcloud configuration, and create a new dataset in BigQuery named 'opinions'. In this dataset it will create several tables and views necessary for this sample.

  `./build_dataset.sh`

* [optional] Later on, if you make changes to the table schema or views, you can update the definitions of these objects by running update commands:

  `./build_tables.sh update`
  
  `./build_views.sh update`

Table schema definitions are located in the *Schema.json files in the `bigquery` directory. View definitions are located in the shell script build_views.sh.

### Prepare your machine for Dataflow job submissions

Download and install [Sirocco](https://github.com/datancoffee/sirocco), a framework maintained by [@datancoffee](https://twitter.com/@datancoffee).

* Download the latest [Sirocco Java framework](https://github.com/datancoffee/sirocco/releases/) **jar** file.

* Download the latest [Sirocco model](https://github.com/datancoffee/sirocco-mo/releases/) **jar** file.

* Go to the directory where the downloaded sirocco-sa-x.y.z.jar and sirocco-mo-x.y.z.jar files are located.

* Install the Sirocco framework in your local Maven repository. Replace x.y.z with downloaded versions.

```
mvn install:install-file \
  -DgroupId=sirocco.sirocco-sa \
  -DartifactId=sirocco-sa \
  -Dpackaging=jar \
  -Dversion=x.y.z \
  -Dfile=sirocco-sa-x.y.z.jar \
  -DgeneratePom=true
```

* Install the Sirocco model file in your local Maven repository. Replace x.y.z with downloaded version.

```
mvn install:install-file \
  -DgroupId=sirocco.sirocco-mo \
  -DartifactId=sirocco-mo \
  -Dpackaging=jar \
  -Dversion=x.y.z \
  -Dfile=sirocco-mo-x.y.z.jar \
  -DgeneratePom=true
```


### Run demo jobs

You can use the included news articles (from Google's blogs) and movie reviews in the `src/test/resources/testdatasets` directory to run demo jobs. News articles are in TXT bag-of-properties format and movie reviews are in CSV format. More information about the format and the meaning of parameters is available in the [Sirocco repo](https://github.com/datancoffee/sirocco#running-included-test-datasets-and-your-own-tests)

* Upload the files in the `src/test/resources/testdatasets` directory into the GCS `input` bucket. Use the [Cloud Storage browser](https://console.cloud.google.com/storage/browser) to find the `input` directory you created in Prerequisites. Then, upload all files from your local `src/test/resources/testdatasets` directory.

We will run a demo job that processes movie reviews in CSV format. 

* Go back to the `dataflow-opinion-analysis` directory 

`cd dataflow-opinion-analysis`

* Build the executable jar. This command should create a bundled jar in the target directory, e.g. ./target/examples-opinionanalysis-bundled-x.y.z.jar

```
mvn clean package
```

* Run a command to deploy the control Dataflow pipeline to Cloud Dataflow. 

```
scripts/run_indexer_gcs_csv_to_bigquery.sh FULLINDEX SHALLOW SHORTTEXT 1 2 "gs://$GCS_BUCKET/input/kaggle-rotten-tomato/*.csv"
```

* (First Time Only) The first time you run the job, you will get an error from Dataflow 

```The workflow was automatically rejected by the service because it uses an unsupported SDK Google Cloud Dataflow SDK for Java 2.2.0. Please upgrade to the latest SDK version. To override the SDK version check temporarily, please provide an override token using the experiment flag '--experiments=unsupported_sdk_temporary_override_token=<token>'. Note that this token expires on <date>.```

This is because we are still working on upgrading our Beam dependecies to newer versions of Beam. To fix this error, modify your scripts/set_env_vars_local.sh script to set the UNSUPPORTED_SDK_OVERRIDE_TOKEN to the token that was returned. 

Set the shell variables again.
```
. scripts/set_env_vars_local.sh
```

Resubmit the job.
  
* In the [Dataflow Console](https://console.cloud.google.com/dataflow) observe how a new input job is created. 

* Once the Dataflow job successfully finishes, you can review the data it will write into your target BigQuery dataset. Use the [BigQuery console](https://console.cloud.google.com/bigquery) to review the dataset.

* Enter the following query to list new documents that were indexed by the Dataflow job. The sample query is using the Standard SQL dialect of BigQuery.

```
#standardSQL
SELECT d.CollectionItemId, s.* 
FROM opinions.sentiment s
    INNER JOIN opinions.document d ON d.DocumentHash = s.DocumentHash
WHERE SentimentTotalScore > 0
ORDER BY ProcessingDateId DESC, SentimentTotalScore DESC
LIMIT 1000;

```

### Issues Under Investigation

* The IndexerPipeline Dataflow job does not truncate existing content in BigQuery tables, even if --writeTruncate=true is specified
This is because the BigQuery tables are defined as partitioned tables. The workaround for truncating the content between job runs is to run the following script

```
DELETE FROM opinions.document WHERE 1=1;
DELETE FROM opinions.sentiment WHERE 1=1;
DELETE FROM opinions.webresource WHERE 1=1;
``` 

* Building the project on Apple M1 chip hardware results in an error
`Caused by: org.xerial.snappy.SnappyError: [FAILED_TO_LOAD_NATIVE_LIBRARY] no native library is found for os.name=Mac and os.arch=aarch64`

This is because we are using an older version of the Beam SDK, which in turn uses an older version of snappy-java. Snappy-java version [1.1.8.2](https://github.com/xerial/snappy-java/releases/tag/1.1.8.2) is supposed to work on Apple M1 chips, and we will fix the problem when we upgrade to newer versions of Beam. For the time being, build the project and submit jobs on pre-M1 Mac hardware.

* The IndexerPipeline Dataflow job is marked as 'Failed' although data gets successfuilly imported into BigQuery. This is because of the temporary BigQuery import files created in the GCS temp folder that are sometimes not cleaned up. The IndexerPipeline stages that write to BigQuery are marked as 'Failed' as well. Since data is successfully imported into BigQuery, this issue can be ignored for the time being, until we upgraded our Beam dependecies.

If you are seeing pipeline failures, see if you are getting the following errors in the pipeline logs
```
java.lang.RuntimeException: org.apache.beam.sdk.util.UserCodeException: java.io.IOException: Error executing batch GCS request
...
Caused by: java.util.concurrent.ExecutionException: com.google.api.client.http.HttpResponseException: 404 Not Found

<!DOCTYPE html>
<html lang=en>
  <meta charset=utf-8>
  <meta name=viewport content="initial-scale=1, minimum-scale=1, width=device-width">
  <title>Error 404 (Not Found)!!1</title>
  <style>
    *{margin:0;padding:0}html,code{font:15px/22px arial,sans-serif}html{background:#fff;color:#222;padding:15px}body{margin:7% auto 0;max-width:390px;min-height:180px;padding:30px 0 15px}* > body{background:url(//www.google.com/images/errors/robot.png) 100% 5px no-repeat;padding-right:205px}p{margin:11px 0 22px;overflow:hidden}ins{color:#777;text-decoration:none}a img{border:0}@media screen and (max-width:772px){body{background:none;margin-top:0;max-width:none;padding-right:0}}#logo{background:url(//www.google.com/images/logos/errorpage/error_logo-150x54.png) no-repeat;margin-left:-5px}@media only screen and (min-resolution:192dpi){#logo{background:url(//www.google.com/images/logos/errorpage/error_logo-150x54-2x.png) no-repeat 0% 0%/100% 100%;-moz-border-image:url(//www.google.com/images/logos/errorpage/error_logo-150x54-2x.png) 0}}@media only screen and (-webkit-min-device-pixel-ratio:2){#logo{background:url(//www.google.com/images/logos/errorpage/error_logo-150x54-2x.png) no-repeat;-webkit-background-size:100% 100%}}#logo{display:inline-block;height:54px;width:150px}
  </style>
  <a href=//www.google.com/><span id=logo aria-label=Google></span></a>
  <p><b>404.</b> <ins>That’s an error.</ins>
  <p>  <ins>That’s all we know.</ins> 
```

### Clean up

Now that you have tested the sample, delete the cloud resources you created to prevent further billing for them on your account.

* Stop the control Cloud Dataflow job in the [Dataflow Cloud Console](https://console.cloud.google.com/dataflow).


* Disable and delete the App Engine application as described in
    [Disable or delete your application](http://cloud.google.com/appengine/docs/adminconsole/applicationsettings#disable_or_delete_your_application)
    in the Google App Engine documentation.

* Delete the Cloud Pub/Sub topic.
    You can delete the topic and associated subscriptions from the Cloud Pub/Sub
    section of the [Cloud Console](https://console.cloud.google.com).


##License:

Copyright 2021 Google Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
