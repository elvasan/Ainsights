#!/bin/bash
env=""
client_name=""
cluster_id=""

while getopts e: opt
do
  case ${opt} in
    e) env=${OPTARG};;
    *) return 1
  esac
done

provisionCluster() {
  cluster_id=$(aws emr create-cluster \
            --applications Name=Hadoop Name=Spark Name=Ganglia \
            --ec2-attributes https://s3.amazonaws.com/jornaya-${env}-us-east-1-aida-insights/pyspark/emr-ec2-attributes.json \
            --release-label emr-5.8.0 \
            --log-uri 's3n://aws-logs-794223901232-us-east-1/elasticmapreduce/' \
            --instance-groups https://s3.amazonaws.com/jornaya-${env}-us-east-1-aida-insights/pyspark/emr-instance-groups.json \
            --configurations https://s3.amazonaws.com/jornaya-${env}-us-east-1-aida-insights/pyspark/emr-config.json \
            --name 'aida-insights cluster' \
            --service-role EMR_Role \
            --region us-east-1 \
            --profile jornaya-${env} \
            --tags "Version: $(aws s3 cp s3://jornaya-${env}-us-east-1-aida-insights/pyspark/version.txt - --profile=jornaya-${env})" |
            jq -r .ClusterId)
}

echo "Starting EMR Cluster..."
provisionCluster
echo "Cluster ID: $cluster_id"
echo "AWS Console url: https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:$cluster_id"
