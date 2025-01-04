import boto3
import urllib3
from io import BytesIO

def lambda_handler(event, context):
    # S3 client
    s3 = boto3.client('s3')

    # URL of the zip file to download
    zip_url = "https://d25kgz5rikkq4n.cloudfront.net/cost_transparency/mrf/in-network-rates/reporting_month=2024-12/2024-12-01_cigna-health-life-insurance-company_colorado-cpop_in-network-rates.json.gz?Expires=2053659599&Signature=SV-wDip7hv4yqXy~NePOxXvFxOtsdsIlCYGa8KLBLLU35OuxRALlw9MPSfe5SYmO0z7KU9p47gBUTaK6NQetCK4jgXYonBP1p1YQWmtWBHpOzQtg0INXXVXFHXKYTP2JqRQ692OpYqN06llrRI8~ob0aMdQcKJvQpf3xz9fIkwB461WSZEBJuda-ERj9OBYzEhGZX2zJLErixb35tKiiFGYWd40P9JyT5JYRUrvDp1sntMHgthbPrgePdGZbd9WviT913d83Hs-k6vsLEJu5n37Xek6~5-YQQxO4RwqNlMyoufQGemcmwiGtH0q1qyRhZBwfD5H-zM3kp-h6nOiKWw__&Key-Pair-Id=K1NVBEPVH9LWJP"

    # S3 bucket and key for storing the zip file
    bucket_name = "transparency-data"
    s3_key = "cigna/2024-12-01_cigna-health-life-insurance-company_colorado-cpop_in-network-rates.json.gz"

    try:
        # Download the zip file
        # response = requests.get(zip_url)
        http = urllib3.PoolManager()
        response = http.request('GET', zip_url)
        zip_content = BytesIO(response.data)

        # Upload to S3
        s3.upload_fileobj(zip_content, bucket_name, s3_key)

        return {
            'statusCode': 200,
            'body': f'Successfully downloaded and stored zip file in S3: s3://{bucket_name}/{s3_key}'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
