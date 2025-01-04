import time
import ijson
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from s3pathlib import S3Path
from io import BytesIO
from s3pathlib import context

start_time = time.perf_counter()

context.attach_boto_session(
    boto3.session.Session(
        region_name="us-west-2",
        profile_name="aws-toolkit",
    )
)

bucket='transparency-data'
raw_file_s3_prefix = 'cigna/2024-12-01_cigna-health-life-insurance-company_colorado-cpop_in-network-rates.json.gz'
base_prefix = 'cigna/preprocessed/base/'
provider_references_prefix = 'cigna/preprocessed/provider_references/'
in_network_prefix = 'cigna/preprocessed/in_network/'


def lookup_item(key:str, file):
    file.seek(0)
    items = ijson.items(file, key, use_float=True)
    return next(items, None)


def lookup_items(key:str, file):
    file.seek(0)
    return ijson.items(file, key, use_float=True)


def dict_to_table(data: dict):
    schema = pa.schema([
        ('reporting_entity_name', pa.string()),
        ('reporting_entity_type', pa.string()),
        ('last_updated_on', pa.string()),
        ('version', pa.string())
    ])

    return pa.Table.from_pydict(data, schema= schema)


def list_to_table(data: list):
    return pa.Table.from_pylist(data)


def store_to_s3(table: pa.Table, s3_key):
    # Write to Parquet format in memory
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    s3.put_object(Bucket=bucket, Key=s3_key, Body=buf.getvalue())
    print(f"Stored file with s3_key: {s3_key}")


def extract_base(file):
    reporting_entity_name = lookup_item('reporting_entity_name', file)
    print('reporting_entity_name:', reporting_entity_name)

    reporting_entity_type = lookup_item('reporting_entity_type', file)
    print('reporting_entity_type:', reporting_entity_type)

    last_updated_on = lookup_item('last_updated_on', file)
    print('last_updated_on:', last_updated_on)

    version = lookup_item('version', file)
    print('version:', version)

    return {
        'reporting_entity_name' : [reporting_entity_name],
        'reporting_entity_type' : [reporting_entity_type],
        'last_updated_on' : [last_updated_on],
        'version' : [version]
    }


def store_base_to_s3(data: dict):
    print(data)
    table = dict_to_table(data)
    s3_key = base_prefix + 'base.parquet'
    store_to_s3(table, s3_key)


def store_items(iter, path_prefix:str, name_prefix:str):
    list=[]
    file_index=0

    for count, item in enumerate(iter):

        if count > 0 and count % 1000 == 0:
            prefix = path_prefix + name_prefix + '_{0}.parquet'.format(file_index)
            print(f"file index:{file_index}, file_preifx: {prefix}")
            file_index = file_index + 1
            table = list_to_table(list)
            store_to_s3(table, prefix)
            list=[]

        list.append(item)

    prefix = path_prefix + name_prefix + '_{0}.parquet'.format(file_index)
    print(f"file index:{file_index}, file_preifx: {prefix}")
    table = list_to_table(list)
    store_to_s3(table, prefix)
    list=[]
    print('finished storing items')

session = boto3.Session(profile_name='aws-toolkit')
s3 = session.client('s3')

file_s3_path = S3Path(bucket, raw_file_s3_prefix)


with file_s3_path.open("rb") as file:
    # base
    base_data = extract_base(file)
    store_base_to_s3(base_data)

    # provider_references
    provider_references = lookup_items('provider_references.item', file)
    store_items(provider_references, provider_references_prefix, 'provider_references')

    # in_network
    in_network = lookup_items('in_network.item', file)
    store_items(in_network, in_network_prefix, 'in_network')

    print('complete!')

    end_time = time.perf_counter()

    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.6f} seconds")

