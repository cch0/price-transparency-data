import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
from pyspark.sql.functions import explode

#create a dataframe of base objects - reporting_entity_name, reporting_entity_type, version, last_updated_on
#using the output of preprocessing step

base_df = spark.read.json('s3://yourbucket/ptd/preprocessed/base/')

#create a dataframe over provider_references objects using the output of preprocessing step
prvd_df = spark.read.json('s3://yourbucket/ptd/preprocessed/provider_references/')

#cross join dataframe of base objects with dataframe of provider_references
prvd_df = prvd_df.crossJoin(base_df)

#create a dataframe over in_network objects using the output of preprocessing step
in_ntwrk_df = spark.read.json('s3://yourbucket/ptd/preprocessed/in_network/')

#unnest and flatten negotiated_rates and provider_references from in_network objects
in_ntwrk_df2 = in_ntwrk_df.select(
 in_ntwrk_df.billing_code, in_ntwrk_df.billing_code_type, in_ntwrk_df.billing_code_type_version,
 in_ntwrk_df.covered_services, in_ntwrk_df.description, in_ntwrk_df.name,
 explode(in_ntwrk_df.negotiated_rates).alias('exploded_negotiated_rates'),
 in_ntwrk_df.negotiation_arrangement)


in_ntwrk_df3 = in_ntwrk_df2.select(
 in_ntwrk_df2.billing_code, in_ntwrk_df2.billing_code_type, in_ntwrk_df2.billing_code_type_version,
 in_ntwrk_df2.covered_services, in_ntwrk_df2.description, in_ntwrk_df2.name,
 in_ntwrk_df2.exploded_negotiated_rates.negotiated_prices.alias(
 'exploded_negotiated_rates_negotiated_prices'),
 explode(in_ntwrk_df2.exploded_negotiated_rates.provider_references).alias(
 'exploded_negotiated_rates_provider_references'),
 in_ntwrk_df2.negotiation_arrangement)

#join the exploded in_network dataframe with provider_references dataframe
jdf = prvd_df.join(
 in_ntwrk_df3,
 prvd_df.provider_group_id == in_ntwrk_df3.exploded_negotiated_rates_provider_references,"fullouter")

#un-nest and flatten attributes from rest of the nested arrays.
jdf2 = jdf.select(
 jdf.reporting_entity_name,jdf.reporting_entity_type,jdf.last_updated_on,jdf.version,
 jdf.provider_group_id, jdf.provider_groups, jdf.billing_code,
 jdf.billing_code_type, jdf.billing_code_type_version, jdf.covered_services,
 jdf.description, jdf.name,
 explode(jdf.exploded_negotiated_rates_negotiated_prices).alias(
 'exploded_negotiated_rates_negotiated_prices'),
 jdf.exploded_negotiated_rates_provider_references,
 jdf.negotiation_arrangement)

jdf3 = jdf2.select(
 jdf2.reporting_entity_name,jdf2.reporting_entity_type,jdf2.last_updated_on,jdf2.version,
 jdf2.provider_group_id,
 explode(jdf2.provider_groups).alias('exploded_provider_groups'),
 jdf2.billing_code, jdf2.billing_code_type, jdf2.billing_code_type_version,
 jdf2.covered_services, jdf2.description, jdf2.name,
 jdf2.exploded_negotiated_rates_negotiated_prices.additional_information.
 alias('additional_information'),
 jdf2.exploded_negotiated_rates_negotiated_prices.billing_class.alias(
 'billing_class'),
 jdf2.exploded_negotiated_rates_negotiated_prices.billing_code_modifier.
 alias('billing_code_modifier'),
 jdf2.exploded_negotiated_rates_negotiated_prices.expiration_date.alias(
 'expiration_date'),
 jdf2.exploded_negotiated_rates_negotiated_prices.negotiated_rate.alias(
 'negotiated_rate'),
 jdf2.exploded_negotiated_rates_negotiated_prices.negotiated_type.alias(
 'negotiated_type'),
 jdf2.exploded_negotiated_rates_negotiated_prices.service_code.alias(
 'service_code'), jdf2.exploded_negotiated_rates_provider_references,
 jdf2.negotiation_arrangement)

jdf4 = jdf3.select(jdf3.reporting_entity_name,jdf3.reporting_entity_type,jdf3.last_updated_on,jdf3.version,
 jdf3.provider_group_id,
 explode(jdf3.exploded_provider_groups.npi).alias('npi'),
 jdf3.exploded_provider_groups.tin.type.alias('tin_type'),
 jdf3.exploded_provider_groups.tin.value.alias('tin'),
 jdf3.billing_code, jdf3.billing_code_type,
 jdf3.billing_code_type_version, jdf3.covered_services,
 jdf3.description, jdf3.name, jdf3.additional_information,
 jdf3.billing_class, jdf3.billing_code_modifier,
 jdf3.expiration_date, jdf3.negotiated_rate,
 jdf3.negotiated_type, jdf3.service_code,
 jdf3.negotiation_arrangement)

#repartition by billing_code.
#Repartition changes the distribution of data on spark cluster.
#By repartition data we will avoid writing too many small files.
jdf5=jdf4.repartition("billing_code")

datasink_path = "s3://yourbucket/ptd/processed/billing_code_npi/parquet/"

#persist dataframe as parquet on S3 and catalog it
#Partition the data by billing_code. This enables analytical queries to skip data and improve performance of queries
#Data is also bucketed and sorted npi to improve query performance during analysis

jdf5.write.format('parquet').mode("overwrite").partitionBy('billing_code').bucketBy(2, 'npi').sortBy('npi').saveAsTable('ptdtable', path = datasink_path)