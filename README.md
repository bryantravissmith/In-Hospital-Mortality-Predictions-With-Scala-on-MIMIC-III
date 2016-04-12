# In-Hospital-Mortality-Predictions-With-Scala-on-MIMIC-III
Using the MIMIC III Dataset to Create Tables, Fit a Model, and make Predictions of In-Hospital Mortality

You can create an Elastic Map-Reduce Cluster using the following command line tools.  This is the setup that successfully ran my script.
```bash
aws emr create-cluster --termination-protected \
--applications Name=Hadoop Name=Hive Name=Pig Name=Hue Name=Spark Name=Zeppelin-Sandbox \
--ec2-attributes '{"KeyName":"<-your-keypair-name>","InstanceProfile":"EMR_EC2_DefaultRole","EmrManagedSlaveSecurityGroup":"<-your-EMR-slave-security-group->","EmrManagedMasterSecurityGroup":"<-your-EMR-master-security-group->"}' \
--service-role EMR_DefaultRole \
--enable-debugging \
--release-label emr-4.5.0 \
--log-uri 's3n://aws-logs-924441742886-us-east-1/elasticmapreduce/' \
--name 'MIMIC 3 Cluster' \
--instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"r3.xlarge","Name":"Master instance group - 1"},{"InstanceCount":6,"InstanceGroupType":"CORE","InstanceType":"r3.xlarge","Name":"Core instance group - 2"}]' \
--region us-east-1
```

You can also set it up your cluster using the web-dashboard.  You will want to make sure that you use emr-4.5.0, and have Hive and Spark installed.   The other applications are optional for this script.

