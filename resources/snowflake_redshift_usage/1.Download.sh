aws s3 cp --no-sign-request s3://redshift-downloads/redset/serverless/parts/ redset/serverless/parts/ --recursive
aws s3 cp --no-sign-request s3://redshift-downloads/redset/provisioned/parts/ redset/provisioned/parts/ --recursive

mkdir -p snowset
wget http://www.cs.cornell.edu/~midhul/snowset/snowset-main.parquet.tar.gz
tar -xzvf snowset-main.parquet.tar.gz
mv snowset-main.parquet snowset