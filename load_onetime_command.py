from google.cloud import storage
import subprocess

bucket_name = 'bucket_name' 
def create_load_command(blob_name):
    command = f"bq load --autodetect --source_format=CSV mydataset.mytable gs://{bucket_name}/{blob_name}"
    return command

def gcs_to_bigquery(request):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    for blob in bucket.list_blobs():
        blob_name = blob.name
        command = create_load_command(blob_name)
        subprocess.run(command, check=True, shell=True)
        # not recommanded to run script in cloud function
        # https://stackoverflow.com/questions/70802906/call-shell-script-from-within-google-cloud-function