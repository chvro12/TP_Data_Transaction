import boto3
import botocore
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def init_s3_client():
    return boto3.client(
        's3',
        endpoint_url='http://127.0.0.1:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123',
        region_name='us-east-1',
        use_ssl=False,
        config=botocore.client.Config(signature_version='s3v4')
    )

def create_bucket(s3_client, bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except botocore.exceptions.ClientError as error:
        error_code = int(error.response['Error']['Code'])
        if error_code == 404:
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                print(f"Bucket '{bucket_name}' created successfully.")
            except botocore.exceptions.ClientError as error:
                print(f"Failed to create bucket: {error}")
        else:
            print(f"Error occurred: {error}")

s3_client = init_s3_client()
bucket_name = "warehouse-transaction"
create_bucket(s3_client, bucket_name)

data = {
    "id_transaction": [1],
    "type_transaction": ["sale"],
    "montant": [100.0],
    "devise": ["USD"],
    "date": ["2021-08-01"],
    "lieu": ["Paris"],
    "moyen_paiement": ["card"],
    "utilisateur": ["John Doe"]
}
df = pd.DataFrame(data)

table = pa.Table.from_pandas(df)
pq.write_table(table, 'temp.parquet')

with open('temp.parquet', 'rb') as data:
    s3_client.upload_fileobj(data, bucket_name, 'data/temp.parquet')
print(f"Data successfully written to s3://{bucket_name}/data/temp.parquet")
