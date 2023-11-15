from google.cloud import storage
from google.cloud import bigquery

bucket_name = 'bucket_name' 
def create_uri(blob_name):
    command = f"gs://{bucket_name}/{blob_name}"
    return command

def gcs_to_bigquery(request):
    gcs_client = storage.Client()
    bucket = gcs_client.get_bucket(bucket_name)
    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV
    )
    for blob in bucket.list_blobs():
        blob_name = blob.name
        uri = create_uri(blob_name)
        #modify the target table
        target = "mydataset.mytable"
        load_job = bq_client.load_table_from_uri(
        source_uris=uri, 
        destination=target, 
        job_config=job_config
        )
        load_job.result()
