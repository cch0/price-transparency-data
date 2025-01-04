import time
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import explode
from pyspark.sql import DataFrame


start_time = time.perf_counter()


def show_metadata(df: DataFrame, name:str):
    print(f"dataframe '{name}', schema")
    df.printSchema()
    print(f"dataframe '{name}', row count: {df.count()}, column count: {len(df.columns)}\n")


def show_records(df: DataFrame, count:int = 1):
    df.show(count)


os.environ['AWS_PROFILE'] = "aws-toolkit"

bucket='transparency-data'
base_prefix = 'cigna/preprocessed/base/'
provider_references_prefix = 'cigna/preprocessed/provider_references/'
in_network_prefix = 'cigna/preprocessed/in_network/'
processed_prefix = 'cigna/processed/aws_glue/parquet/spark/'


conf = SparkConf()

# ontain aws credential from aws profile
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
conf.set("spark.executor.memory", "8g")
conf.set("spark.memory.fraction", "0.8")
# conf.set("spark.memory.offHeap.enabled", "false")
# conf.set("spark.memory.offHeap.size", "8g")
conf.set("spark.executor.cores", "8")

conf.set("spark.driver.memory", "5g")
# conf.set("spark.driver.maxResultSize", "3g")
# conf.set("spark.kryoserializer.buffer.max", "1g")

# to be able to access s3
conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")

# create spark session using configuration
session = SparkSession.builder.config(conf=conf).getOrCreate()



print(f"spark.executor.memory: {session.conf.get("spark.executor.memory")}")
print(f"spark.memory.fraction: {session.conf.get("spark.memory.fraction")}")
# print(f"spark.memory.offHeap.enabled: {session.conf.get("spark.memory.offHeap.enabled")}")
# print(f"spark.memory.offHeap.size: {session.conf.get("spark.memory.offHeap.size")}")
print(f"spark.executor.cores: {session.conf.get("spark.executor.cores")}")
print(f"spark.driver.memory: {session.conf.get("spark.driver.memory")}")


# ==========================================================================================
# step 0 - read the preprocessed data

# base is stored as jsonparquet
base_df = session.read.parquet(f"s3a://{bucket}/{base_prefix}")
show_metadata(base_df, 'base_df')
show_records(base_df)

# root
#  |-- last_updated_on: string (nullable = true)
#  |-- reporting_entity_name: string (nullable = true)
#  |-- reporting_entity_type: string (nullable = true)
#  |-- version: string (nullable = true)



# provider_references files are in parquet format
provider_references_df = session.read.parquet(f"s3a://{bucket}/{provider_references_prefix}")
show_metadata(provider_references_df, 'provider_references_df')
show_records(provider_references_df, 5)

# root
#  |-- provider_group_id: long (nullable = true)
#  |-- location: string (nullable = true)

# in_network files are in parquet format
in_network_df = session.read.parquet(f"s3a://{bucket}/{in_network_prefix}")
show_metadata(in_network_df, 'in_network_df')
show_records(in_network_df, 5)


# root
#  |-- negotiation_arrangement: string (nullable = true)
#  |-- name: string (nullable = true)
#  |-- billing_code_type: string (nullable = true)
#  |-- billing_code_type_version: string (nullable = true)
#  |-- billing_code: string (nullable = true)
#  |-- description: string (nullable = true)
#  |-- negotiated_rates: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- negotiated_prices: array (nullable = true)
#  |    |    |    |-- element: struct (containsNull = true)
#  |    |    |    |    |-- additional_information: string (nullable = true)
#  |    |    |    |    |-- billing_class: string (nullable = true)
#  |    |    |    |    |-- billing_code_modifier: array (nullable = true)
#  |    |    |    |    |    |-- element: string (containsNull = true)
#  |    |    |    |    |-- expiration_date: string (nullable = true)
#  |    |    |    |    |-- negotiated_rate: double (nullable = true)
#  |    |    |    |    |-- negotiated_type: string (nullable = true)
#  |    |    |    |    |-- service_code: array (nullable = true)
#  |    |    |    |    |    |-- element: string (containsNull = true)
#  |    |    |-- provider_references: array (nullable = true)
#  |    |    |    |-- element: long (containsNull = true)


# ==========================================================================================
# step 1 - cross join base and provider_references dataframes
provider_references_joined_base_df = provider_references_df.crossJoin(base_df)
show_metadata(provider_references_joined_base_df, 'provider_references_joined_base_df')
show_records(provider_references_joined_base_df, 5)


# root
#  |-- provider_group_id: long (nullable = true)
#  |-- location: string (nullable = true)
#  |-- last_updated_on: string (nullable = true)
#  |-- reporting_entity_name: string (nullable = true)
#  |-- reporting_entity_type: string (nullable = true)
#  |-- version: string (nullable = true)


# ==========================================================================================
# step 2 - unnest array inside in_network
## step 2.1
# - unnest 'negotiated_rates' array field and replace it with 'exploded_negotiated_rates' field
in_network_df2 = in_network_df.select(
    in_network_df.billing_code,
    in_network_df.billing_code_type,
    in_network_df.billing_code_type_version,
    in_network_df.description,
    in_network_df.name,
    explode(in_network_df.negotiated_rates).alias('exploded_negotiated_rates'),
    in_network_df.negotiation_arrangement
 )

show_metadata(in_network_df2, 'in_network_df2')
show_records(in_network_df2, 5)


# root
#  |-- billing_code: string (nullable = true)
#  |-- billing_code_type: string (nullable = true)
#  |-- billing_code_type_version: string (nullable = true)
#  |-- description: string (nullable = true)
#  |-- name: string (nullable = true)
#  |-- exploded_negotiated_rates: struct (nullable = true)
#  |    |-- negotiated_prices: array (nullable = true)
#  |    |    |-- element: struct (containsNull = true)
#  |    |    |    |-- additional_information: string (nullable = true)
#  |    |    |    |-- billing_class: string (nullable = true)
#  |    |    |    |-- billing_code_modifier: array (nullable = true)
#  |    |    |    |    |-- element: string (containsNull = true)
#  |    |    |    |-- expiration_date: string (nullable = true)
#  |    |    |    |-- negotiated_rate: double (nullable = true)
#  |    |    |    |-- negotiated_type: string (nullable = true)
#  |    |    |    |-- service_code: array (nullable = true)
#  |    |    |    |    |-- element: string (containsNull = true)
#  |    |-- provider_references: array (nullable = true)
#  |    |    |-- element: long (containsNull = true)
#  |-- negotiation_arrangement: string (nullable = true)




## step 2.2
# - rename 'negotiated_prices' field under 'exploded_negotiated_rates' field as 'exploded_negotiated_rates_negotiated_prices'
# - unnest 'provider_references' array field under 'exploded_negotiated_rates' field and replace it
#   as 'exploded_negotiated_rates_provider_references' field.
in_network_df3 = in_network_df2.select(
    in_network_df2.billing_code,
    in_network_df2.billing_code_type,
    in_network_df2.billing_code_type_version,
    in_network_df2.description,
    in_network_df2.name,
    in_network_df2.exploded_negotiated_rates.negotiated_prices.alias(
        'exploded_negotiated_rates_negotiated_prices'),
    explode(in_network_df2.exploded_negotiated_rates.provider_references).alias(
        'exploded_negotiated_rates_provider_references'),
    in_network_df2.negotiation_arrangement
 )

show_metadata(in_network_df3, 'in_network_df3')
show_records(in_network_df3, 5)


# root
#  |-- billing_code: string (nullable = true)
#  |-- billing_code_type: string (nullable = true)
#  |-- billing_code_type_version: string (nullable = true)
#  |-- description: string (nullable = true)
#  |-- name: string (nullable = true)
#  |-- exploded_negotiated_rates_negotiated_prices: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- additional_information: string (nullable = true)
#  |    |    |-- billing_class: string (nullable = true)
#  |    |    |-- billing_code_modifier: array (nullable = true)
#  |    |    |    |-- element: string (containsNull = true)
#  |    |    |-- expiration_date: string (nullable = true)
#  |    |    |-- negotiated_rate: double (nullable = true)
#  |    |    |-- negotiated_type: string (nullable = true)
#  |    |    |-- service_code: array (nullable = true)
#  |    |    |    |-- element: string (containsNull = true)
#  |-- exploded_negotiated_rates_provider_references: long (nullable = true)
#  |-- negotiation_arrangement: string (nullable = true)






# ==========================================================================================
# step 3 - join provider_references and in_network dataframes
joined_df = provider_references_joined_base_df.join(in_network_df3,
                                        provider_references_joined_base_df.provider_group_id == in_network_df3.exploded_negotiated_rates_provider_references,
                                        'full_outer')

show_metadata(joined_df, 'joined_df')
show_records(joined_df, 5)


# root
#  |-- provider_group_id: long (nullable = true)
#  |-- location: string (nullable = true)
#  |-- last_updated_on: string (nullable = true)
#  |-- reporting_entity_name: string (nullable = true)
#  |-- reporting_entity_type: string (nullable = true)
#  |-- version: string (nullable = true)
#  |-- billing_code: string (nullable = true)
#  |-- billing_code_type: string (nullable = true)
#  |-- billing_code_type_version: string (nullable = true)
#  |-- description: string (nullable = true)
#  |-- name: string (nullable = true)
#  |-- exploded_negotiated_rates_negotiated_prices: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- additional_information: string (nullable = true)
#  |    |    |-- billing_class: string (nullable = true)
#  |    |    |-- billing_code_modifier: array (nullable = true)
#  |    |    |    |-- element: string (containsNull = true)
#  |    |    |-- expiration_date: string (nullable = true)
#  |    |    |-- negotiated_rate: double (nullable = true)
#  |    |    |-- negotiated_type: string (nullable = true)
#  |    |    |-- service_code: array (nullable = true)
#  |    |    |    |-- element: string (containsNull = true)
#  |-- exploded_negotiated_rates_provider_references: long (nullable = true)
#  |-- negotiation_arrangement: string (nullable = true)


# ==========================================================================================
# step 4 - unnest 'exploded_negotiated_rates_negotiated_prices' array field
joined_df2 = joined_df.select(
    joined_df.reporting_entity_name,
    joined_df.reporting_entity_type,
    joined_df.last_updated_on,
    joined_df.version,
    joined_df.provider_group_id,
    joined_df.billing_code,
    joined_df.billing_code_type,
    joined_df.billing_code_type_version,
    joined_df.description,
    joined_df.name,
    explode(joined_df.exploded_negotiated_rates_negotiated_prices).alias(
        'exploded_negotiated_rates_negotiated_prices'),
    joined_df.exploded_negotiated_rates_provider_references,
    joined_df.negotiation_arrangement
)

show_metadata(joined_df2, 'joined_df2')
show_records(joined_df2, 5)

# root
#  |-- reporting_entity_name: string (nullable = true)
#  |-- reporting_entity_type: string (nullable = true)
#  |-- last_updated_on: string (nullable = true)
#  |-- version: string (nullable = true)
#  |-- provider_group_id: long (nullable = true)
#  |-- billing_code: string (nullable = true)
#  |-- billing_code_type: string (nullable = true)
#  |-- billing_code_type_version: string (nullable = true)
#  |-- description: string (nullable = true)
#  |-- name: string (nullable = true)
#  |-- exploded_negotiated_rates_negotiated_prices: struct (nullable = true)
#  |    |-- additional_information: string (nullable = true)
#  |    |-- billing_class: string (nullable = true)
#  |    |-- billing_code_modifier: array (nullable = true)
#  |    |    |-- element: string (containsNull = true)
#  |    |-- expiration_date: string (nullable = true)
#  |    |-- negotiated_rate: double (nullable = true)
#  |    |-- negotiated_type: string (nullable = true)
#  |    |-- service_code: array (nullable = true)
#  |    |    |-- element: string (containsNull = true)
#  |-- exploded_negotiated_rates_provider_references: long (nullable = true)
#  |-- negotiation_arrangement: string (nullable = true)



# ==========================================================================================
# step 5 - select fields under 'exploded_negotiated_rates_negotiated_prices' struct field

joined_df3 = joined_df2.select(
    joined_df2.reporting_entity_name,
    joined_df2.reporting_entity_type,
    joined_df2.last_updated_on,
    joined_df2.version,
    joined_df2.provider_group_id,
    joined_df2.billing_code,
    joined_df2.billing_code_type,
    joined_df2.billing_code_type_version,
    joined_df2.description,
    joined_df2.name,
    joined_df2.exploded_negotiated_rates_negotiated_prices.additional_information.alias('additional_information'),
    joined_df2.exploded_negotiated_rates_negotiated_prices.billing_class.alias('billing_class'),
    joined_df2.exploded_negotiated_rates_negotiated_prices.billing_code_modifier.alias('billing_code_modifier'),
    joined_df2.exploded_negotiated_rates_negotiated_prices.expiration_date.alias('expiration_date'),
    joined_df2.exploded_negotiated_rates_negotiated_prices.negotiated_rate.alias('negotiated_rate'),
    joined_df2.exploded_negotiated_rates_negotiated_prices.negotiated_type.alias('negotiated_type'),
    joined_df2.exploded_negotiated_rates_negotiated_prices.service_code.alias('service_code'),
    joined_df2.exploded_negotiated_rates_provider_references,
    joined_df2.negotiation_arrangement
 )

show_metadata(joined_df3, 'joined_df3')
show_records(joined_df3, 5)


# root
#  |-- reporting_entity_name: string (nullable = true)
#  |-- reporting_entity_type: string (nullable = true)
#  |-- last_updated_on: string (nullable = true)
#  |-- version: string (nullable = true)
#  |-- provider_group_id: long (nullable = true)
#  |-- billing_code: string (nullable = true)
#  |-- billing_code_type: string (nullable = true)
#  |-- billing_code_type_version: string (nullable = true)
#  |-- description: string (nullable = true)
#  |-- name: string (nullable = true)
#  |-- additional_information: string (nullable = true)
#  |-- billing_class: string (nullable = true)
#  |-- billing_code_modifier: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- expiration_date: string (nullable = true)
#  |-- negotiated_rate: double (nullable = true)
#  |-- negotiated_type: string (nullable = true)
#  |-- service_code: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- exploded_negotiated_rates_provider_references: long (nullable = true)
#  |-- negotiation_arrangement: string (nullable = true)


# ==========================================================================================
# step 6 - repartition by 'billing_code'

print('apply repartition')
repartitioned_df = joined_df3.repartition('billing_code')

processed_s3_key = f"s3a://{bucket}/{processed_prefix}"

print('write back to s3')
repartitioned_df.write.format('parquet').mode("overwrite").partitionBy('billing_code').bucketBy(2, 'negotiated_type').sortBy('negotiated_type').saveAsTable('transparency_data', path = processed_s3_key)



# ==========================================================================================
# end of processing, calculate processing time

end_time = time.perf_counter()
execution_time = end_time - start_time
print(f"Execution time: {execution_time:.6f} seconds")

