# Deploying AIDA Insights

This document should detail all the steps necessary to deploy the AIDA Insights
application and any code that facilitates said deployment.

## Gitlab Pipelines

A pipeline is a group of jobs that get executed in stages (batches).
All of the jobs in a stage are executed in parallel
(if there are enough concurrent Runners), and if they all succeed,
the pipeline moves on to the next stage. If one of the jobs fails,
the next stage is not (usually) executed.
You can access the pipelines page in your project's Pipelines tab. - [from Gitlab](https://docs.gitlab.com/ee/ci/pipelines.html)

AIDA Insights uses Gitlab Pipelines to build, test, package, and deploy our code.
A pipeline is automatically kicked off when our code is pushed to a branch or merged
into another such as develop or master.

A pipeline will run the following steps automatically for AIDA Insights:

1. lint: Use Prospector to analyze code for potential errors.
2. test: Use pytest to run our suite of unit tests.
3. package: Zips up our application files and stores in Artifactory.

At the time of this writing, the last two steps are a manual process. The first job
will deploy our main application code to an Amazon S3 bucket
(s3://jornaya-dev-us-east-1-aida-insights/pyspark at the time of this writing) and the
second job will deploy the lambda code to another S3 bucket.

The deploy step currently has the following two options:

1. dev_deploy - deploy to the jornaya-dev environment
2. qa_deploy - deploy to the jornaya-qa environment

The deploy-lambda step has the following option:

1. dev-deploy-lambda

Based on the environment, a user who is ready to deploy the code should run both
the deploy and deploy-lambda steps for the respective environment.

## Configuration Options

AIDA Insights has several configuration options that allow the user to
specify event lookbacks, scoring thresholds, which columns are returned,
and running the application as if it were being run on a specified date.
In order to support all options effectively there are two levels of configuration:
application level configuration and per client configuration.

There are three columns that must be present in a csv configuration file in
order for AIDA Insights to function properly. Those columns are `option`, `config_abbrev`,
and `value`.

#### Event Lookback

Event lookback allows the user to configure a lookback window on a specific industry
for a specified number of days. For example, I could say I want a lookback window
for the Auto Sales industry of 60 days from the job run date. The application would
take the run date and subtract 60 days to get the window in which we would want
to evaluate any events. Any events that are not within that 60 day window are omitted
when calculating the industry score for that record.

The application defaults file has default values for each industry that Jornaya has predetermined.
A client can easily override these values by including an industry in the client configuration.

The following are valid column values:

    * option: event_lookback
    * config_abbrev: One of [auto_sales, education, insurance, financial_services, real_estate, jobs, legal, home_services, or other]
    * value: An integer value representing a number of days

#### Frequency Threshold

The frequency threshold configuration settings allow a user to specify at what frequency
do we determine whether a record has either high intent or low intent. For example if we set a
threshold of 4 for the Auto Sales industry and a record had 6 events in the auto sales industry
then that record would be scored as a high intent consumer. However if that record only had 3
events in the auto sales industry, it would be categorized as a low intent consumer.

The following are valid column values:

    * option: frequency_threshold
    * config_abbrev: One of [auto_sales, education, insurance, financial_services, real_estate, jobs, legal, home_services, or other]
    * value: An integer value representing the scoring threshold

#### As Of

This configuration value allows the user to run the file as of a specified date. For
example if a user specifed an as of date of 12/31/2017 12:00 then any events recorded
after that date would not be taken into consideration during scoring. In addition,
the as of date will be the date used when calculating event lookbacks.

An as of date is not required and default behavior is to take the current date
as the as of date.

The following are valid column values:

    * option: asof
    * config_abbrev: asof
    * value: A date in the format MM/DD/YY HH:MM or MM/DD/YYYY HH:MM

#### Industry Result

The industry result options allow a user to specify which columns are returned
to the client. The industry results are only applied to the external facing file
and are therefore part of the client overrides.

If no values are specified, by default no columns would be returned to the client.

The following are valid column values:

    * option: industry_result
    * config_abbrev: One of [auto_sales, education, insurance, financial_services, real_estate, jobs, legal, home_services, or other]
    * value: One of [auto_sales, education, insurance, financial_services, real_estate, jobs, legal, home_services, or other]

#### Schema Location

This option is strictly for internal use only and is used to specify where the
schema for certain tables are stored in S3.

The following are valid column values:

    * option: schema_location
    * config_abbrev: One of [hash_mapping, classif_lead, classif_subcategory, classif_set_elem_xref, consumer_view, lead_event, or publisher_permissions]
    * value: A string value with a valid S3 location.

### Application Defaults

The application level configuration is stored along with the main
application code under the /pyspark/config/{env}/ directories. The {env}
parameter corresponds to the current environment which is passed to the
application as a job parameter. The name of that file is application_defaults.csv
regardless of the deployed environment.

The following options are available at the application configuration level:

    - event_lookback
    - frequency_threshold
    - schema_location*

The options designated with an asterisk are only available at this configuration level.

#### Example:

    | option                 | config_abbrev    | value                                         |
    | -----------------------|------------------|-----------------------------------------------|
    | event_lookback         | auto_sales       |   90                                          |
    | frequency_threshold    | auto_sales       |   4                                           |
    | schema_location        | hash_mapping     |   s3://jornaya-dev-us-east-1-fdl/hash_mapping |

### Client Configuration

The client configuration files are stored within each respective client directory
and the name of the file is always client_config.csv
For example beestest configuration would be stored as
s3://jornaya-dev-us-east-1-aida-insights/app_data/beestest/client_config.csv.
During each run of our application, a copy of the client_config file is
copied into the run directory so we know exactly what the client configuration
parameters are at the time that a file was run for a client.

The following configuration options are available at the client config level:

    - event_lookback
    - frequency_threshold
    - industry_result*
    - asof*

The options designated with an asterisk are only available at this configuration level.

#### Example:

    | option                 | config_abbrev    | value                                         |
    | -----------------------|------------------|-----------------------------------------------|
    | event_lookback         | auto_sales       |   90                                          |
    | frequency_threshold    | auto_sales       |   4                                           |
    | industry_result        | auto_sales       |   auto_sales                                  |
    | asof                   | asof             |   09/15/2018 12:30                            |


## Manual Setup

The following items are required to be set up for AIDA Insights to function properly in
a production environment and unfortunately part of a manual set up process:

1. S3 - Create incoming bucket with client name subdirectory. The incoming bucket should
only ever need to be set up once and from there new client directories can be created as needed.
    - Beestest example: s3://jornaya-dev-us-east-1-aida-insights/incoming/beestest

2. S3 - Create a client directory under app_data with the client's configuration file.
    - Beestest example: s3://jornaya-dev-us-east-1-aida-insights/app_data/beestest
    - Beestest configuration: s3://jornaya-dev-us-east-1-aida-insights/app_data/beestest/client_config.csv

## Running the application

At this stage, once all of the previous steps have been complete, all that is
required of the user is to drop an input file into the incoming directory under the
appropriate client folder. **The CSV file MUST be same name as the client directory.**

    - Upload beestest file to: s3://jornaya-dev-us-east-1-aida-insights/incoming/beestest/beestest.csv

From here our AWS Lambda and Step Functions will take over and move the file from
incoming to our client directory under app_data. A new folder will be created under
the client directory that houses all of the files and information specific to that
particular file execution. The directory will take the client name and append the
current timestamp to create the file run directory.

    - Beestest example: s3://jornaya-dev-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02_12_00_00_000000/

The client config file will be copied to this location and the client csv will
be moved here and the AIDA Insights application will initiate a run.

The application will write any results to an output directory here but from there
our AWS Lambdas and Step Functions will take over again, zip up our output files and
move them to an outgoing directory under the aida-insights bucket

    - Beestest example: s3://jornaya-dev-us-east-1-aida-insights/outgoing/beestest/beestest_2018_01_05_15_34_21_325702.zip

As this writing this is where out automated processes end. If we want to deliver those results
to a client, someone needs to manually download the zip file and send it to a client.
