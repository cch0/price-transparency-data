# athena, create external table

CREATE EXTERNAL TABLE processed_data(
    negotiated_rate FLOAT,
    description STRING
)
PARTITIONED BY (billing_code STRING)
STORED AS PARQUET
LOCATION "s3://transparency-data/cigna/processed/parquet/polars/"


MSCK REPAIR TABLE processed_data;

