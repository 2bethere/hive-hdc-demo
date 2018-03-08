This repo contains the files for HIVE on Hortonworks Cloud demo script

Data file location:(s3a://hw-sampledata/airline_orc/airline_ontime.db/)

You can load data by running the scripts in this depo based on their sequence numbers.

If you run into the follwing error
ERROR : FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message:org.apache.hadoop.fs.s3a.AWSClientIOException: doesBucketExist on hw-sampledata: com.amazonaws.AmazonClientException: No AWS Credentials provided by BasicAWSCredentialsProvider EnvironmentVariableCredentialsProvider SharedInstanceProfileCredentialsProvider : com.amazonaws.AmazonClientException: The requested metadata is not found at http://169.254.169.254/latest/meta-data/iam/security-credentials/: No AWS Credentials provided by BasicAWSCredentialsProvider EnvironmentVariableCredentialsProvider SharedInstanceProfileCredentialsProvider : com.amazonaws.AmazonClientException: The requested metadata is not found at http://1.2.3.4/latest/meta-data/iam/security-credentials/)

Add the following to hive-site.xml and restart
fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider

hive.llap.io.encode.enabled=true
hive.llap.cache.allow.synthetic.fileid=true;
hive.vectorized.use.vector.serde.deserialize=true;
hive.vectorized.use.row.serde.deserialize=true;
