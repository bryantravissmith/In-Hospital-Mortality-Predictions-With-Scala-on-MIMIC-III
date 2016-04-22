# In-Hospital Mortality Predictions with Scala & Spark on MIMIC-III

## Introduction

This project is a collection of scripts to reproduce a reserach project for [Georgia Tech's Online Masters in Computer Science (OMSCS)](http://www.omscs.gatech.edu/) course [Big Data For Health Infomatics](http://www.omscs.gatech.edu/cse-8803-special-topics-big-data-for-health-informatics).  The scripts are intended to run in the spark-shell on an [Elastic Map-Reduce(EMR) cluster](https://aws.amazon.com/elasticmapreduce/) hosted in [Amazon Web Services (AWS)](http://aws.amazon.com/) with the [Medical Info-Mart Intensive Care (MIMIC) III data](http://mimic.physionet.org/) hosted in an AWS S3 Bucket.

## Elastic Map-Reduce on Amazon Web Services

This code was successull run several times with the MIMIC III v1.3 hosted, uncompressed, in an S3 bucket.   The cluster that I used to run are scripts consisted of 1 r3.xlarge master node and 6 r3.xlarge worker nodes.   The following AWS Command Line Script is what I used to start the cluster each time I successfully ran the scripts.  

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

### Alternative to Commandline
You can also set it up your cluster using the web-dashboard.  You will want to make sure that you use emr-4.5.0, and have Hive and Spark installed.   The other applications are optional for the above script.  emr-4.6.0 also worked successfully with the scripts hosted in this repo.

## Code

All the code assumed a uri set to the S3 bucket location of of your MIMIC III files (uncompresssed) and the folder you want the generated files to be written to.  The bucket location in the files is a *private* bucket that you will not have access to.   You just gain access to the MIMIC III data and host it to your own bucket to successfully run these scripts

## Outline of Process to Reproduce Research

The first step is to pre-process the NOTEEVENTS.csv file.  The text is multilined, and to be consumable by sparksql we need to convert the text for each note to be single lined.   The python script will preprocess the NOTEEVENTS.csv to NOTEEVENTS_PROCESSED.csv.  This new file should upload to the S3 bucket.

To reporduce my results you will need to run the scripts in the following order:

1.  MakeServerityScoresScript.scala ( ~ 20 minutes )
2.  BuildAndSaveModels.scala ( ~ 20 minutes )
3.  CrossValidateRetrospective.scala ( ~ 1 hour )
4.  GetTimeVaryingAUCsOnTestData.scala ( ~ 20 minutes )
5.  TopWordsInLDAModel.scala ( ~ 5 minutes )

The first script will build and save the various serverity scores used in the research to an output folder you specificy in your S3 bucket.  The second script builds and save multiple text, topic, and logistic models to a folder you specify in your S3 bucket.  The third rebuilds and scores the models (without saving) an a 70%/30% train/test split 10-fold crosss validation.  The values are printed to the console.  The 4th script scores the time-varying performance for the three logisitic models saved out to your S3 bucket.  The results for all three models are printed out to the console.  The last script finds to the most important words in the LDA topic model.  The results are also printed out to the console.   

## Detailed Instructions

You will need to download the MIMIC III data and uncompress the files.   After they are uncompressed you will needed to run the python script in the same location as the files

```bash
python processnotes.py
```

Upload your uncompressed data to an S3 bucket, including the new file. to your S3 bucket.

Start your EMR cluster.

After your EMR cluster is running, I copy the scall files to the home directory of the master node.

```bash
scp -i <-your-keypair.pem-> location/to/repo/*.scala hadoop@ec2-xx-xx-xx-xx.computer-x.amazonaws.com:~/
```

You then login to your cluster:

```bash
ssh -i <-your-keypair.pem-> hadoop@ec2-xx-xx-xx-xx.computer-x.amazonaws.com:~/
```

Once you are logged into your master node you can start your spark-shell with the following commands.  There are a number of parameters set to increase the maximum size of the object.  Not using them left myself and others running into memory errors when building the LDA topic model.

```bash
SPARK_REPL_OPTS="-XX:MaxPermSize=10g" spark-shell --packages com.databricks:spark-csv_2.10:1.4.0 --conf spark.driver.maxResultSize=10g --conf spark.driver.memory=10g --conf spark.executor.memory=15g
```

Once the shell is loaded you can run the scipts using the :load command:

```scala
:load MakeSeverityScoreScript.scala
```

```scala
:load BuildAndSaveModels.scala
```

```scala
:load CrossValidateRetrospective.scala
```

```scala
:load GetTimeVaryingAUCsOnTestData.scala
````

```scala
:load TopWordsInLDAModel.scala
```
