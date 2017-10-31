#!/bin/bash
env=""
client_name=""
cluster_id=""

while getopts c:e:i: opt
do
  case ${opt} in
    c) client_name=${OPTARG};;
    e) env=${OPTARG};;
    i) cluster_id=${OPTARG};;
    *) return 1
  esac
done

aws emr add-steps \
    --profile=jornaya-${env} \
    --region=us-east-1 \
    --cluster-id ${cluster_id} \
    --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--py-files","s3://jornaya-'${env}'-us-east-1-aida-insights/pyspark/jobs.zip","s3://jornaya-'${env}'-us-east-1-aida-insights/pyspark/main.py","--job-args","environment='${env}'","client_name='${client_name}'"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"Spark application"}]' 

