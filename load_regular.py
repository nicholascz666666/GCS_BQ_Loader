from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from google.cloud import storage  

bucket_name = 'bucket_name' 
target_folders = []
# --autodetect auto detects the schema than specify it in the command. 
# if not right, you can have another function to come up with the schema
def create_load_command(blob_name):
    #modify the target table
    command = f"bq load --autodetect --source_format=CSV mydataset.mytable gs://{bucket_name}/{blob_name}"
    return command

default_args = {
    'start_date': datetime.today(),
}


with DAG(
    'gcs_to_bigquery_loader', 
    default_args=default_args,
    schedule_interval=None
) as dag:
    
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    for folder in target_folders:
        # https://cloud.google.com/storage/docs/samples/storage-list-files-with-prefix#storage_list_files_with_prefix-python
        for blob in bucket.list_blobs(prefix=folder):
            blob_name = blob.name
            blob_name_id = blob_name.replace("/", "_")
            create_load_command_task = PythonOperator(
                task_id=f'create_load_command_{blob_name_id}',
                python_callable=create_load_command,
                op_kwargs={'blob_name': blob_name},
                provide_context=True,
            )

            run_command = BashOperator(
                task_id=f'load_{blob_name_id}',
                bash_command="{{ task_instance.xcom_pull(task_ids='create_load_command_" + blob_name_id + "') }}",
            )

            create_load_command_task >> run_command