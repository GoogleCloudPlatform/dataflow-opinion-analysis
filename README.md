# Sample: Opinion Analysis of News, Threaded Conversations, and User Generated Content
This sample uses Cloud Dataflow to build an opinion analysis processing pipeline for
news, threaded conversations in forums like Hacker News, Reddit, or Twitter and
other user generated content e.g. email.

Opinion Analysis can be used for lead generation purposes, user research, or 
automated testimonial harvesting.

## About the sample

This sample contains three components:

* A task scheduler, built using Google App Engine Cron Service, Google Cloud Pub/Sub control topic and Google Cloud Dataflow in streaming mode
* Cloud Dataflow pipelines for importing bounded (batch) raw data from sources such as relational Google Cloud SQL databases (MySQL or PostgreSQL, via the JDBC connector) and files in Google Cloud Storage
* Additional ETL transformations in BigQuery enabled via Cloud Dataflow and embedded SQL statements


## How to run the sample
The steps for configuring and running this sample are as follows:

- Setup your Google Cloud Platform project and permissions.
- Install tools necessary for compiling and deploying the code in this sample.
- Create and setup a Cloud Storage bucket and Cloud Pub/Sub topics.
- Create or verify a configuration for your project.
- Clone the sample code
- Specify cron jobs for the App Engine scheduling  app
- Create the BigQuery dataset
- Deploy the Dataflow pipelines
- Clean up

### Prerequisites

Setup your Google Cloud Platform project and permissions

* Select or Create a Google Cloud Platform project.
  In the [Google Cloud Console](https://console.cloud.google.com/project), select
  **Create Project**.

* [Enable billing](https://support.google.com/cloud/answer/6293499#enable-billing) for your project.

* [Enable](https://console.cloud.google.com/flows/enableapi?apiid=dataflow,compute_component,logging,storage_component,storage_api,bigquery,pubsub,datastore) the Google Dataflow, Compute Engine, Google Cloud Storage, and other APIs necessary to run the example. 

Install tools necessary for compiling and deploying the code in this sample, if not already on your system, specifically git, Google Cloud SDK, Python (for orchestration scripts), Java and Maven (for Dataflow pipelines):

* Install [`git`](https://git-scm.com/downloads).

* [Download and install the Google Cloud SDK](http://cloud.google.com/sdk/).

* Install [Python 2.7](https://www.python.org/download/releases/2.7/).

* Install [Python `pip`](https://pip.pypa.io/en/latest/installing.html).

* Download and install the [Java Development Kit (JDK)](http://www.oracle.com/technetwork/java/javase/downloads/index.html) version 1.8 or later. Verify that the JAVA_HOME environment variable is set and points to your JDK installation.

* [Download](http://maven.apache.org/download.cgi) and [install](http://maven.apache.org/install.html) Apache Maven.


Create and setup a Cloud Storage bucket and Cloud Pub/Sub topics

* [Create a Cloud Storage bucket](https://console.cloud.google.com/storage/browser) for your project. This bucket will be used for staging your code, as well as for temporary input/output files. For consistency with this sample, select Multi-Regional storage class and United States location.

* Create folders in this bucket `staging`, `input`, `output`, `temp`, `indexercontrol`

* [Create](https://console.cloud.google.com/cloudpubsub/topicList) the following Pub/Sub topics: `indexercommands`, `documents`

Create or verify a configuration for your project

* Authenticate with the Cloud Platform. Run the following command to get [Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials).

  `gcloud auth application-default login`

* Create a new configuration for your project if it does not exist already

  `gcloud init`

* Verify your configurations

  `gcloud config configurations list`


Important: This tutorial uses several billable components of Google Cloud
Platform. New Cloud Platform users may be eligible for a [free trial](http://cloud.google.com/free-trial).



### Clone the sample code

To clone the GitHub repository to your computer, run the following command:

```
git clone https://github.com/GoogleCloudPlatform/dataflow-opinion-analysis
```

Go to the `dataflow-opinion-analysis` directory. The exact path
depends on where you placed the directory when you cloned the sample files from
GitHub.

```
cd dataflow-opinion-analysis
```

### Specify cron jobs for the App Engine scheduling  app

* In the [App Engine Console](https://console.cloud.google.com/appengine) create an App Engine app

* In shell, activate the configuration for the project where you want to deploy the app 

  `gcloud config configurations activate <config-name>`

* Include the Python API client in your App Engine app

  `pip install -t scheduler/lib/ google-api-python-client`
  
* Adjust the schedule for your ETL jobs and edit the `scheduler/cron.yaml` file. You define tasks for App Engine Task Scheduler in [YAML format](http://yaml.org/). For a complete description of how to use YAML to specify jobs for Cron Service, including the schedule format, see [Scheduled Tasks with Cron for Python] (https://cloud.google.com/appengine/docs/python/config/cron#Python_app_yaml_The_schedule_format).

* Update the control topic name `indexercommands` in the scheduler scripts to the name you used when you created the Pub/Sub topic. Edit the following files:
	scheduler/startjdbcimport.py
	scheduler/startsocialimport.py
	scheduler/startstatscalc.py

* Upload the scheduling application to App Engine.  

  `gcloud app deploy --version=1 scheduler/app.yaml scheduler/cron.yaml`

After you deploy the App Engine application, it uses the App Engine Cron Service
to schedule sending messages to the Cloud Pub/Sub control topics. If the control Cloud Pub/Sub topic
specified in your Python scripts (e.g. `startjdbcimport.py`) does not exist, the application creates it.

You can see the cron jobs under in the Cloud Console under:

  Compute > App Engine > Task queues > Cron Jobs

You can also see the control topic in the Cloud Console:

  Big Data > Pub/Sub


### Create the BigQuery dataset

* Make sure you've activated the gcloud configuration for the project where you want to create your BigQuery dataset

  `gcloud config configurations activate <config-name>`

* In shell, go to the `bigquery` directory where the build scripts and schema files for BigQuery tables and views are located

  `cd bigquery`

* Run the `build_dataset.sh` script to create the dataset, tables, and views. The script will use the PROJECT_ID variable from your active gcloud configuration, and create a new dataset in BigQuery named 'opinions'. In this dataset it will create several tables and views necessary for this sample.

  `./build_dataset.sh`

* [optional] Later on, if you make changes to the table schema or views, you can update the definitions of these objects by running update commands:

  `./build_tables.sh update`
  
  `./build_views.sh update`

Table schema definitions are located in the *Schema.json files in the `bigquery` directory. View definitions are located in the shell script build_views.sh.

### Deploy the Dataflow pipelines


#### [Optional] Download and install the Sirocco sentiment analysis packages

If you would like to use this sample for deep textual analysis, download and install [Sirocco](https://github.com/datancoffee/sirocco), a framework maintained by [@datancoffee](https://medium.com/@datancoffee).

* Download the [Sirocco Java framework](https://github.com/datancoffee/sirocco/releases/download/v1.0.0/sirocco-sa-1.0.0.jar) jar file.

* Download the [Sirocco model](https://gist.github.com/datancoffee/4df233243723ab00874192257687f32c/raw/c652516fe451556b6c6cf60214333ecd736a421b/sirocco-mo-1.0.0.jar) file.

* Go to the directory where the downloaded sirocco-sa-1.0.0.jar and sirocco-mo-1.0.0.jar files are located.

* Install the Sirocco framework in your local Maven repository

```
mvn install:install-file \
  -DgroupId=sirocco.sirocco-sa \
  -DartifactId=sirocco-sa \
  -Dpackaging=jar \
  -Dversion=1.0.0 \
  -Dfile=sirocco-sa-1.0.0.jar \
  -DgeneratePom=true
```

* Install the Sirocco model file in your local Maven repository

```
mvn install:install-file \
  -DgroupId=sirocco.sirocco-mo \
  -DartifactId=sirocco-mo \
  -Dpackaging=jar \
  -Dversion=1.0.0 \
  -Dfile=sirocco-mo-1.0.0.jar \
  -DgeneratePom=true
```

#### Build and Deploy your Controller pipeline to Cloud Dataflow


* Go to the `dataflow-opinion-analysis/scripts` directory and make a copy of the `run_controljob_template.sh` file

```
cd scripts
cp run_controljob_template.sh run_controljob.sh
```

* Edit the `run_controljob.sh` file in your favorite text editor, e.g. `nano`. Specifically, set the values of the variables used for parametarizing your control Dataflow pipeline. Set the values of the PROJECT_ID, DATASET_ID and other variables at the beginning of the shell script.

* Go back to the `dataflow-opinion-analysis` directory and run a command to deploy the control Dataflow pipeline to Cloud Dataflow.


```
cd ..
scripts/run_controljob.sh &
```

#### Run a verification job

You can use the included news articles (from Google's blogs) in the `src/test/resources/input` directory to run a test pipeline.

* Upload the files in the `src/test/resources/input` directory into the GCS `input` bucket. Use the [Cloud Storage browser](https://console.cloud.google.com/storage/browser) to find the `input` directory you created in Prerequisites. Then, upload all files from your local `src/test/resources/input` directory.

* Use the [Pub/Sub console](https://console.cloud.google.com/cloudpubsub/topicList) to send a command to start a file import job. Find the `indexercommand` topic in the Pub/Sub console. Click on its name.

* Click on "Publish Message" button. In the Message box, copy the following command and click "Publish.

```
command=start_gcs_import
```

* In the [Dataflow Console](https://console.cloud.google.com/dataflow) observe how a new input job is created. It will have a "-gcsdocimport" suffix.

* Once the Dataflow job successfully finishes, you can review the data it will write into your target BigQuery dataset. Use the [BigQuery console](https://bigquery.cloud.google.com/) to review the dataset.

* Enter the following query to list new documents that were indexed by the Dataflow job. The sample query is using the Standard SQL dialect of BigQuery.

```
SELECT * FROM opinions.document 
ORDER BY ProcessingTime DESC
LIMIT 100
```

### Clean up

Now that you have tested the sample, delete the cloud resources you created to
prevent further billing for them on your account.

* Stop the control Cloud Dataflow job in the [Dataflow Cloud Console](https://console.cloud.google.com/dataflow).


* Disable and delete the App Engine application as described in
    [Disable or delete your application](http://cloud.google.com/appengine/docs/adminconsole/applicationsettings#disable_or_delete_your_application)
    in the Google App Engine documentation.

* Delete the Cloud Pub/Sub topic.
    You can delete the topic and associated subscriptions from the Cloud Pub/Sub
    section of the [Cloud Console](https://console.cloud.google.com).


##License:

Copyright 2017 Google Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
