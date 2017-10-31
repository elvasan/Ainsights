# AIDA Insights EMR Deployment Script

This script launches an execution of the AIDA-insights spark job (code stored/deployed to S3) on EMR. EMR clusters launched using this script are "transient", meaning the cluster will terminate automatically after success or failure.

### Prerequisites
`jq` is used to parse the EMR-generated cluster ID and  poll the AWS CLI for the current status of the cluster, and is required to successfully run the script:

```brew install jq```

### Usage

```sh emr-deploy.sh -c {client} -e {env}```

There are two required arguments:

`-c` - client name

`-e` - environment (VPC) in which to execute

For example, to execute AIDA-Insights in `jornaya-dev` for the `beestest` client:

```sh emr-deploy.sh -c beestest -e dev```