#!/bin/bash
classification_file=""
input_file=""
output_dir=""
cluster_id=""
previous_state=''
current_state=''
marks=( '/' '-' '\' '|' )

while getopts c:i:o: opt
do
  case ${opt} in
    c) classification_file=${OPTARG};;
    i) input_file=${OPTARG};;
    o) output_dir=${OPTARG};;
    *) return 1
  esac
done



provisionCluster() {
  cluster_id=$(aws emr create-cluster \
            --applications Name=Hadoop Name=Spark \
            --ec2-attributes '{"SubnetId":"subnet-600fda2b","EmrManagedSlaveSecurityGroup":"sg-a030edd2","EmrManagedMasterSecurityGroup":"sg-5f36eb2d"}' \
            --release-label emr-5.8.0 \
            --log-uri 's3n://aws-logs-794223901232-us-east-1/elasticmapreduce/' \
            --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--py-files","s3://jornaya-dev-us-east-1-aida-insights/pyspark/jobs.zip","s3://jornaya-dev-us-east-1-aida-insights/pyspark/main.py","--job-args","input_file_name='${input_file}'","classification_file='${classification_file}'","output_dir='${output_dir}'"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"Spark application"}]' \
            --instance-count 3 \
            --instance-type c4.large \
            --configurations '[{"Classification":"spark-env","Properties":{},"Configurations":[{"Classification":"export","Properties":{"PYSPARK_PYTHON":"python34"},"Configurations":[]}]}]' \
            --use-default-roles \
            --name 'aida-insights scripted cluster test' \
            --region us-east-1 \
            --profile jornaya-dev \
            --tags "Version: $(aws s3 cp s3://jornaya-dev-us-east-1-aida-insights/pyspark/version.txt - --profile=jornaya-dev)" \
            --auto-terminate |
            jq -r .ClusterId)
}

echo "Starting EMR Cluster..."
provisionCluster
echo "Cluster ID: $cluster_id"
echo "AWS Console url: https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:$cluster_id"

while true; do
    json=$(aws emr describe-cluster --cluster-id ${cluster_id} --region us-east-1  --profile=jornaya-dev)
    current_state=$(echo ${json} | jq -r '.Cluster.Status.State')

    if [[ ${current_state} != ${previous_state} ]]; then
        echo "Cluster $cluster_id state is $current_state."
        previous_state=${current_state}
    fi

    if [[ "TERMINATED" == ${current_state} ]]; then
        echo "EMR JOB SUCCESS"
        break
    fi

    if [[ "TERMINATED_WITH_ERRORS" == ${current_state} ]]; then
        echo "EMR JOB FAILURE"
        emr_failure=true
        break
    fi

    printf '%s\r' "${marks[i++ % ${#marks[@]}]}"
    sleep 1
done
