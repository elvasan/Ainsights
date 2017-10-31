#!/bin/bash
env=""
client_name=""
cluster_id=""
previous_state=''
current_state=''
marks=( '/' '-' '\' '|' )

while getopts c:e: opt
do
  case ${opt} in
    c) client_name=${OPTARG};;
    e) env=${OPTARG};;
    *) return 1
  esac
done

provisionCluster() {
  cluster_id=$(aws emr create-cluster \
            --applications Name=Hadoop Name=Spark \
            --ec2-attributes '{"SubnetId":"subnet-600fda2b","EmrManagedSlaveSecurityGroup":"sg-a030edd2","EmrManagedMasterSecurityGroup":"sg-5f36eb2d", "InstanceProfile":"EMR_EC2_Role"}' \
            --release-label emr-5.8.0 \
            --log-uri 's3n://aws-logs-794223901232-us-east-1/elasticmapreduce/' \
            --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--py-files","s3://jornaya-'${env}'-us-east-1-aida-insights/pyspark/jobs.zip","s3://jornaya-'${env}'-us-east-1-aida-insights/pyspark/main.py","--job-args","environment='${env}'","client_name='${client_name}'"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"Spark application"}]' \
            --instance-groups '[{"InstanceCount":4,"BidPrice":"2.15","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":1000,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"InstanceGroupType":"CORE","InstanceType":"r4.8xlarge","Name":"Core - 2"},{"InstanceCount":1,"BidPrice":"0.55","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":200,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"InstanceGroupType":"MASTER","InstanceType":"r4.2xlarge","Name":"Master - 1"}]' \
            --configurations '[{"Classification":"spark-env","Properties":{},"Configurations":[{"Classification":"export","Properties":{"PYSPARK_PYTHON":"python34"},"Configurations":[]}]},{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' \
            --name 'aida-insights select_classification' \
            --service-role EMR_Role \
            --region us-east-1 \
            --profile jornaya-${env} \
            --tags "Version: $(aws s3 cp s3://jornaya-${env}-us-east-1-aida-insights/pyspark/version.txt - --profile=jornaya-${env})" \
            --auto-terminate |
            jq -r .ClusterId)
}

echo "Starting EMR Cluster..."
provisionCluster
echo "Cluster ID: $cluster_id"
echo "AWS Console url: https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:$cluster_id"

while true; do
    json=$(aws emr describe-cluster --cluster-id ${cluster_id} --region us-east-1  --profile=jornaya-${env})
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
