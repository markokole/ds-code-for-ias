#!/bin/bash

mkdir datalake_files
tar -xvf datalake.tar -C datalake_files

# create main bucket
aws s3 mb s3://athena-trivago-auto/

# upload files to S3
aws s3 cp datalake_files/unique_selling_points.json s3://athena-trivago-auto/unique_selling_points/
aws s3 cp datalake_files/accommodation.json s3://athena-trivago-auto/accommodation/

# create database
aws athena start-query-execution --query-string "create database trivago_auto" --result-configuration "OutputLocation=s3://athena-trivago-auto/query_result_output/"

# create accommodation table
aws athena start-query-execution --query-string \
"CREATE EXTERNAL TABLE IF NOT EXISTS trivago_auto.accommodation (
  accommodation_id int,
  accommodation_ns int,
  locality_id int,
  date string 
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://athena-trivago-auto/accommodation/'
TBLPROPERTIES ('has_encrypted_data'='false');" \
--result-configuration "OutputLocation=s3://athena-trivago-auto/query_result_output/"

# create unique_selling_points table
aws athena start-query-execution --query-string \
"CREATE EXTERNAL TABLE IF NOT EXISTS trivago_auto.unique_selling_points (
  accommodation_id bigint,
  usp string,
  accommodation_ns int,
  date string 
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://athena-trivago-auto/unique_selling_points/'
TBLPROPERTIES ('has_encrypted_data'='false');" \
--result-configuration "OutputLocation=s3://athena-trivago-auto/query_result_output/"