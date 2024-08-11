from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

download_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
download_dir = '/home/project/airflow/dags/finalassignment/'
default_args = {
    'owner': 'Sror',
    'start_date': days_ago(0),
    'email': ['abdelrahmanasror@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# Define the download and unzip task using BashOperator
download_and_unzip = BashOperator(
    task_id='download_and_unzip',
    bash_command=f'curl -o {download_dir}tolldata.tgz {download_url} && tar -xzf {download_dir}tolldata.tgz -C {download_dir}',
    dag=dag
)

# Define the extract task using BashOperator
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command=f"cut -d ',' -f 1,2,3,4 {download_dir}vehicle-data.csv > {download_dir}csv_data.csv",
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=f"cut -f 5,6,7 {download_dir}tollplaza-data.tsv | tr $'\t' ',' | sed 's/[[:space:]]*$//' > {download_dir}tsv_data.csv",
    dag=dag
)


# Define the extract task using BashOperator
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=f"awk '{{print $(NF-1), $NF}}' {download_dir}payment-data.txt | tr ' ' ',' > {download_dir}fixed_width_data.csv",
    dag=dag
)


# Task to combine extracted data from CSV and TSV and Fixed Width files
combine_data = BashOperator(
    task_id='consolidate_data',
    bash_command=(
        f"paste -d ',' {download_dir}csv_data.csv {download_dir}tsv_data.csv {download_dir}fixed_width_data.csv > {download_dir}extracted_data.csv"
    ),
    dag=dag,
)


transform_data = BashOperator(
    task_id='transform_data',
    bash_command=f"awk -F',' '{{ $4=toupper($4); OFS=\",\"; print $0 }}' {download_dir}extracted_data.csv > {download_dir}transform.csv",
    dag=dag,    
)

# Set the task dependencies
download_and_unzip >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> combine_data >> transform_data