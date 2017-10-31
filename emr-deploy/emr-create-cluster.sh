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
            --applications Name=Hadoop Name=Spark \
            --ec2-attributes '{"KeyName":"emr","SubnetId":"subnet-600fda2b","EmrManagedSlaveSecurityGroup":"sg-a030edd2","EmrManagedMasterSecurityGroup":"sg-5f36eb2d", "InstanceProfile":"EMR_EC2_Role"}' \
            --release-label emr-5.8.0 \
            --log-uri 's3n://aws-logs-794223901232-us-east-1/elasticmapreduce/' \
            --instance-groups '[{"InstanceCount":4,"BidPrice":"2.15","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":1000,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"InstanceGroupType":"CORE","InstanceType":"r4.8xlarge","Name":"Core - 2"},{"InstanceCount":1,"BidPrice":"0.55","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":200,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"InstanceGroupType":"MASTER","InstanceType":"r4.2xlarge","Name":"Master - 1"}]' \
            --configurations '[{"Classification":"spark-env","Properties":{},"Configurations":[{"Classification":"export","Properties":{"PYSPARK_PYTHON":"python34"},"Configurations":[]}]},{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' \
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
