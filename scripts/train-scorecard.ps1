$ErrorActionPreference = "Stop"

Write-Host "Training fraud scorecard model from S3..." -ForegroundColor Cyan

docker exec -it finstream-spark-master sh -lc '/opt/spark/bin/spark-submit --master spark://spark-master:7077 --executor-cores 2 --executor-memory 2G --driver-memory 3G --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider --conf spark.hadoop.fs.s3a.access.key="$AWS_ACCESS_KEY_ID" --conf spark.hadoop.fs.s3a.secret.key="$AWS_SECRET_ACCESS_KEY" --conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com /opt/spark/jobs/train_fraud_scorecard_from_s3.py'
