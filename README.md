# Sample: Opinion Analysis of News, Threaded Conversations, and User Generated Content
This sample uses Cloud Dataflow to build an opinion analysis processing pipeline for
news, threaded conversations in forums like Hacker News, Reddit, or Twitter and
other user generated content e.g. email.

Opinion Analysis can be used for lead generation purposes, user research, or 
automated testimonial harvesting.

## About the sample

This sample contains three components:

* A task orchestrator, built using Google App Engine Cron Service, Google Cloud Pub/Sub control topic and Google Cloud Dataflow in streaming mode
* Cloud Dataflow pipelines for importing bounded (batch) raw data from sources such as relational Google Cloud SQL databases (MySQL or PostgreSQL, via the JDBC connector) and files in Google Cloud Storage
* Additional ETL transformations in BigQuery enabled via Cloud Dataflow and embedded SQL statements


## How to run the sample
The overview for configuring and running this sample is as follows:

1. Create a project and other cloud resources.
2. Clone or download the sample code.
3. Modify settings for cron jobs in YAML files located in the scheduler directory.
4. Deploy the App Engine application responsible for orchestration.

### Prerequisites

* Select or Create a Google Cloud Platform project.
  In the [Google Cloud Console](https://console.cloud.google.com/project), select
  **Create Project**.

* [Enable billing](https://support.google.com/cloud/answer/6293499#enable-billing) for your project.

* [Enable](https://console.cloud.google.com/flows/enableapi?apiid=dataflow,compute_component,logging,storage_component,storage_api,bigquery,pubsub,datastore) the Google Dataflow, Compute Engine, Google Cloud Storage, and other APIs necessary to run the example. 

* [Create a Cloud Storage bucket](https://console.cloud.google.com/storage/browser) for your project. 

Install git, Google Cloud SDK, Python (for orchestration scripts), Java and Maven (for Dataflow pipelines), if not already on your system:

* Install [`git`](https://git-scm.com/downloads).

* [Download and install the Google Cloud SDK](http://cloud.google.com/sdk/).

* Authenticate with the Cloud Platform. Run the following command to get [Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials).
  `gcloud auth application-default login`

* Install [Python 2.7](https://www.python.org/download/releases/2.7/).

* Install [Python `pip`](https://pip.pypa.io/en/latest/installing.html).

* Download and install the [Java Development Kit (JDK)](http://www.oracle.com/technetwork/java/javase/downloads/index.html) version 1.8 or later. Verify that the JAVA_HOME environment variable is set and points to your JDK installation.

* [Download](http://maven.apache.org/download.cgi) and [install](http://maven.apache.org/install.html) Apache Maven.

Important: This tutorial uses several billable components of Google Cloud
Platform. New Cloud Platform users may be eligible for a [free trial](http://cloud.google.com/free-trial).



### Clone the sample code

To clone the GitHub repository to your computer, run the following command:

    $ git clone https://github.com/GoogleCloudPlatform/df-opinion-analysis

Change directories to the `df-opinion-analysis` directory. The exact path
depends on where you placed the directory when you cloned the sample files from
GitHub.

    $ cd df-opinion-analysis

### Specify cron jobs

App Engine Cron Service job descriptions are specified in `scheduler/cron.yaml`, a file in
the App Engine application. You define tasks for App Engine Task Scheduler
in [YAML format](http://yaml.org/). The following example
shows the syntax.

    cron:
      - description: <description of the job>
               url: /events/<topic name to publish to>
               schedule: <frequency in human-readable format>

For a complete description of how to use YAML to specify jobs for Cron Service,
including the schedule format, see
[Scheduled Tasks with Cron for Python](https://cloud.google.com/appengine/docs/python/config/cron#Python_app_yaml_The_schedule_format).

Leave the default cron.yaml as is for now to run through the sample.

### Upload the orchestration application to App Engine

In order for the App Engine application to schedule and relay your events,
you must upload it to a Developers Console project. This is the project
that you created in **Prerequisites**.

1. Configure the `gcloud` command-line tool to use the project you created in
    Prerequisites.

        $ gcloud config set project <your-project-id>

    Where you replace `<your-project-id>`  with the identifier of your cloud
    project.

2. Include the Python API client in your App Engine application.

        $ pip install -t scheduler/lib/ google-api-python-client

    Note: if you get an error and used Homebrew to install Python on OS X,
    see [this fix](https://github.com/Homebrew/homebrew/blob/master/share/doc/homebrew/Homebrew-and-Python.md#note-on-pip-install---user).

3. Create an App Engine application either in the CLI or in the Developer Console

		$ gcloud beta app create

4. Deploy the application to App Engine.

        $ gcloud app deploy --version=1 scheduler/app.yaml \
          scheduler/cron.yaml

After you deploy the App Engine application it uses the App Engine Cron Service
to schedule sending messages to the Cloud Pub/Sub control topics. If the control Cloud Pub/Sub topic
specified in your Python scripts (e.g. `startjdbcimport.py`) does not exist, the application creates it.

You can see the cron jobs under in the console under:

Compute > App Engine > Task queues > Cron Jobs

You can also see the auto-created topic (after about a minute) in the console:

Big Data > Pub/Sub

### Deploy the Dataflow pipelines



### Clean up

Now that you have tested the sample, delete the cloud resources you created to
prevent further billing for them on your account.

* Stop the Cloud Dataflow job in the [Dataflow Cloud Console](https://console.cloud.google.com/dataflow).


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
