import time
import json
import polars as pl
import boto3
from polars import DataFrame
from typing import Any

start_time = time.perf_counter()

class SchemaEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> str:
        if isinstance(obj, type):
            return obj.__name__
        return super().default(obj)


bucket='transparency-data'
region='us-west-2'
base_prefix = 'cigna/preprocessed/base/'
provider_references_prefix = 'cigna/preprocessed/provider_references/'
in_network_prefix = 'cigna/preprocessed/in_network/'
processed_prefix = 'cigna/processed/parquet/polars/'

profile_name = "aws-toolkit"

session = boto3.session.Session(profile_name=profile_name)
credentials = session.get_credentials().get_frozen_credentials()


def print_df(name:str, df: DataFrame, count: int = 1):
    print(f"Printing dataframe: {name}")

    print(df.schema)

    schema: dict = df.schema.to_python()
    print(json.dumps(schema, indent=2, cls=SchemaEncoder))

    print(f"Row count: {df.height}, Column count: {df.width}")
    print(f"Output first {count} row(s)")
    print(df.head(count))
    print('')


def read_parquet(prefix:str) -> DataFrame:
    print(f"reading files with prefix {prefix}")
    df = pl.read_parquet(
        source=f"s3://{bucket}/{prefix}",
        storage_options={
            "aws_access_key_id": credentials.access_key,
            "aws_secret_access_key": credentials.secret_key,
            "aws_region": region,
            }
        )

    return df


def write_parquet(df_list: list[DataFrame], partition_column_name: str, s3_prefix: str):
    for partition in df_list:
        partition_column_value = partition[0, partition_column_name]

        s3_key = f"{s3_prefix}{partition_column_name}={partition_column_value}/{partition_column_value}.parquet"

        print(f"writing file with s3 prefix {s3_key}")

        partition.write_parquet(
            file = f"s3://{bucket}/{s3_key}",
            compression="snappy",
            storage_options={
                "aws_access_key_id": credentials.access_key,
                "aws_secret_access_key": credentials.secret_key,
                "aws_region": region,
            }
        )


# ==========================================================================================
# step 0 - read the preprocessed data

base_df = read_parquet(base_prefix)
print_df('base_df', base_df)

# Schema({'reporting_entity_name': String, 'reporting_entity_type': String, 'last_updated_on': String, 'version': String})

# {
#   "reporting_entity_name": "str",
#   "reporting_entity_type": "str",
#   "last_updated_on": "str",
#   "version": "str"
# }

# Row count: 1, Column count: 4

provider_references_df:DataFrame = read_parquet(provider_references_prefix)
print_df('provider_references_df', provider_references_df, 1)

# Schema({'provider_group_id': Int64, 'location': String})

# {
#   "provider_group_id": "int",
#   "location": "str"
# }

# Row count: 1302, Column count: 2

in_network_df = read_parquet(in_network_prefix)
print_df('in_network_df', in_network_df, 1)

# Schema({'negotiation_arrangement': String, 'name': String, 'billing_code_type': String, 'billing_code_type_version': String, 'billing_code': String, 'description': String, 'negotiated_rates': List(Struct({'negotiated_prices': List(Struct({'additional_information': String, 'billing_class': String, 'billing_code_modifier': List(String), 'expiration_date': String, 'negotiated_rate': Float64, 'negotiated_type': String, 'service_code': List(String)})), 'provider_references': List(Int64)}))})

# {
#   "negotiation_arrangement": "str",
#   "name": "str",
#   "billing_code_type": "str",
#   "billing_code_type_version": "str",
#   "billing_code": "str",
#   "description": "str",
#   "negotiated_rates": "list"
# }

# Row count: 287268, Column count: 7


# ==========================================================================================
# step 1 - join provider_references_df and base_df

provider_references_joined_base_df = provider_references_df.join(base_df, how='cross')
print_df('provider_references_joined_base_df', provider_references_joined_base_df, 1)

# Schema({'provider_group_id': Int64, 'location': String, 'reporting_entity_name': String, 'reporting_entity_type': String, 'last_updated_on': String, 'version': String})

# {
#   "provider_group_id": "int",
#   "location": "str",
#   "reporting_entity_name": "str",
#   "reporting_entity_type": "str",
#   "last_updated_on": "str",
#   "version": "str"
# }

# Row count: 1302, Column count: 6

# ==========================================================================================
# step 2 - unnest array inside in_network
## step 2.1
# - unnest 'negotiated_rates' array column and rename it to 'exploded_negotiated_rates' column

in_network_exploded_df = in_network_df.explode('negotiated_rates').rename({"negotiated_rates": "exploded_negotiated_rates"})
print_df('in_network_exploded_df', in_network_exploded_df)


# Schema({'negotiation_arrangement': String, 'name': String, 'billing_code_type': String, 'billing_code_type_version': String, 'billing_code': String, 'description': String, 'exploded_negotiated_rates': Struct({'negotiated_prices': List(Struct({'additional_information': String, 'billing_class': String, 'billing_code_modifier': List(String), 'expiration_date': String, 'negotiated_rate': Float64, 'negotiated_type': String, 'service_code': List(String)})), 'provider_references': List(Int64)})})

# {
#   "negotiation_arrangement": "str",
#   "name": "str",
#   "billing_code_type": "str",
#   "billing_code_type_version": "str",
#   "billing_code": "str",
#   "description": "str",
#   "exploded_negotiated_rates": "dict"
# }

# Row count: 2804056, Column count: 7

## step 2.2
# - select 'negotiated_prices' column from 'exploded_negotiated_rates' struct column and rename it to 'exploded_negotiated_rates_negotiated_prices'
# - select 'provider_references' column from 'exploded_negotiated_rates' struct column and rename it to 'exploded_negotiated_rates_provider_references'

in_network_df2 = in_network_exploded_df.select(
        "billing_code",
        "billing_code_type",
        "billing_code_type_version",
        "description",
        "name",
        pl.col("exploded_negotiated_rates").struct.field("negotiated_prices")
            .alias("exploded_negotiated_rates_negotiated_prices"),
        pl.col("exploded_negotiated_rates").struct.field("provider_references")
            .alias("exploded_negotiated_rates_provider_references"),
        "negotiation_arrangement"
    )

print_df('in_network_df2', in_network_df2)

# Schema({'billing_code': String, 'billing_code_type': String, 'billing_code_type_version': String, 'description': String, 'name': String, 'exploded_negotiated_rates_negotiated_prices': List(Struct({'additional_information': String, 'billing_class': String, 'billing_code_modifier': List(String), 'expiration_date': String, 'negotiated_rate': Float64, 'negotiated_type': String, 'service_code': List(String)})), 'exploded_negotiated_rates_provider_references': List(Int64), 'negotiation_arrangement': String})

# {
#   "billing_code": "str",
#   "billing_code_type": "str",
#   "billing_code_type_version": "str",
#   "description": "str",
#   "name": "str",
#   "exploded_negotiated_rates_negotiated_prices": "list",
#   "exploded_negotiated_rates_provider_references": "list",
#   "negotiation_arrangement": "str"
# }

# Row count: 2804056, Column count: 8



## step 2.3
# - unnest 'exploded_negotiated_rates_provider_references' array column

in_network_df3 = in_network_df2.explode('exploded_negotiated_rates_provider_references')
print_df('in_network_df3', in_network_df3)

# Schema({'billing_code': String, 'billing_code_type': String, 'billing_code_type_version': String, 'description': String, 'name': String, 'exploded_negotiated_rates_negotiated_prices': List(Struct({'additional_information': String, 'billing_class': String, 'billing_code_modifier': List(String), 'expiration_date': String, 'negotiated_rate': Float64, 'negotiated_type': String, 'service_code': List(String)})), 'exploded_negotiated_rates_provider_references': Int64, 'negotiation_arrangement': String})

# {
#   "billing_code": "str",
#   "billing_code_type": "str",
#   "billing_code_type_version": "str",
#   "description": "str",
#   "name": "str",
#   "exploded_negotiated_rates_negotiated_prices": "list",
#   "exploded_negotiated_rates_provider_references": "int",
#   "negotiation_arrangement": "str"
# }

# Row count: 5156266, Column count: 8


# ==========================================================================================
# step 3 - join provider_references and in_network dataframes

joined_df = provider_references_joined_base_df.join(
    in_network_df3,
    left_on="provider_group_id",
    right_on="exploded_negotiated_rates_provider_references",
    how="full"  # Full outer join
)

print_df('joined_df', joined_df)


# Schema({'provider_group_id': Int64, 'location': String, 'reporting_entity_name': String, 'reporting_entity_type': String, 'last_updated_on': String, 'version': String, 'billing_code': String, 'billing_code_type': String, 'billing_code_type_version': String, 'description': String, 'name': String, 'exploded_negotiated_rates_negotiated_prices': List(Struct({'additional_information': String, 'billing_class': String, 'billing_code_modifier': List(String), 'expiration_date': String, 'negotiated_rate': Float64, 'negotiated_type': String, 'service_code': List(String)})), 'exploded_negotiated_rates_provider_references': Int64, 'negotiation_arrangement': String})

# {
#   "provider_group_id": "int",
#   "location": "str",
#   "reporting_entity_name": "str",
#   "reporting_entity_type": "str",
#   "last_updated_on": "str",
#   "version": "str",
#   "billing_code": "str",
#   "billing_code_type": "str",
#   "billing_code_type_version": "str",
#   "description": "str",
#   "name": "str",
#   "exploded_negotiated_rates_negotiated_prices": "list",
#   "exploded_negotiated_rates_provider_references": "int",
#   "negotiation_arrangement": "str"
# }

# Row count: 5156266, Column count: 14


# ==========================================================================================
# step 4 - unnest 'exploded_negotiated_rates_negotiated_prices' array field

joined_exploded_df = joined_df.explode('exploded_negotiated_rates_negotiated_prices')

joined_df2 = joined_exploded_df.select([
    "reporting_entity_name",
    "reporting_entity_type",
    "last_updated_on",
    "version",
    "provider_group_id",
    "billing_code",
    "billing_code_type",
    "billing_code_type_version",
    "description",
    "name",
    "exploded_negotiated_rates_negotiated_prices",
    "exploded_negotiated_rates_provider_references",
    "negotiation_arrangement"
])

print_df('joined_df2', joined_df2)

# Schema({'reporting_entity_name': String, 'reporting_entity_type': String, 'last_updated_on': String, 'version': String, 'provider_group_id': Int64, 'billing_code': String, 'billing_code_type': String, 'billing_code_type_version': String, 'description': String, 'name': String, 'exploded_negotiated_rates_negotiated_prices': Struct({'additional_information': String, 'billing_class': String, 'billing_code_modifier': List(String), 'expiration_date': String, 'negotiated_rate': Float64, 'negotiated_type': String, 'service_code': List(String)}), 'exploded_negotiated_rates_provider_references': Int64, 'negotiation_arrangement': String})

# {
#   "reporting_entity_name": "str",
#   "reporting_entity_type": "str",
#   "last_updated_on": "str",
#   "version": "str",
#   "provider_group_id": "int",
#   "billing_code": "str",
#   "billing_code_type": "str",
#   "billing_code_type_version": "str",
#   "description": "str",
#   "name": "str",
#   "exploded_negotiated_rates_negotiated_prices": "dict",
#   "exploded_negotiated_rates_provider_references": "int",
#   "negotiation_arrangement": "str"
# }

# Row count: 6843583, Column count: 13




# ==========================================================================================
# step 5 - select fields under 'exploded_negotiated_rates_negotiated_prices' struct field

joined_df3 = joined_df2.select([
    "reporting_entity_name",
    "reporting_entity_type",
    "last_updated_on",
    "version",
    "provider_group_id",
    "billing_code",
    "billing_code_type",
    "billing_code_type_version",
    "description",
    "name",
    pl.col("exploded_negotiated_rates_negotiated_prices").struct.field("additional_information").alias("additional_information"),
    pl.col("exploded_negotiated_rates_negotiated_prices").struct.field("billing_class").alias("billing_class"),
    pl.col("exploded_negotiated_rates_negotiated_prices").struct.field("billing_code_modifier").alias("billing_code_modifier"),
    pl.col("exploded_negotiated_rates_negotiated_prices").struct.field("expiration_date").alias("expiration_date"),
    pl.col("exploded_negotiated_rates_negotiated_prices").struct.field("negotiated_rate").alias("negotiated_rate"),
    pl.col("exploded_negotiated_rates_negotiated_prices").struct.field("negotiated_type").alias("negotiated_type"),
    pl.col("exploded_negotiated_rates_negotiated_prices").struct.field("service_code").alias("service_code"),
    "exploded_negotiated_rates_provider_references",
    "negotiation_arrangement"
])


print_df('joined_df3', joined_df3)

# Schema({'reporting_entity_name': String, 'reporting_entity_type': String, 'last_updated_on': String, 'version': String, 'provider_group_id': Int64, 'billing_code': String, 'billing_code_type': String, 'billing_code_type_version': String, 'description': String, 'name': String, 'additional_information': String, 'billing_class': String, 'billing_code_modifier': List(String), 'expiration_date': String, 'negotiated_rate': Float64, 'negotiated_type': String, 'service_code': List(String), 'exploded_negotiated_rates_provider_references': Int64, 'negotiation_arrangement': String})

# {
#   "reporting_entity_name": "str",
#   "reporting_entity_type": "str",
#   "last_updated_on": "str",
#   "version": "str",
#   "provider_group_id": "int",
#   "billing_code": "str",
#   "billing_code_type": "str",
#   "billing_code_type_version": "str",
#   "description": "str",
#   "name": "str",
#   "additional_information": "str",
#   "billing_class": "str",
#   "billing_code_modifier": "list",
#   "expiration_date": "str",
#   "negotiated_rate": "float",
#   "negotiated_type": "str",
#   "service_code": "list",
#   "exploded_negotiated_rates_provider_references": "int",
#   "negotiation_arrangement": "str"
# }

# Row count: 6843583, Column count: 19


# ==========================================================================================
# step 6 - repartition by 'billing_code'

print('apply repartition')
repartitioned_df_list = joined_df3.partition_by("billing_code")

write_parquet(repartitioned_df_list, 'billing_code', processed_prefix)



# ==========================================================================================
# end of processing, calculate processing time

end_time = time.perf_counter()
execution_time = end_time - start_time
print(f"Execution time: {execution_time:.6f} seconds")


