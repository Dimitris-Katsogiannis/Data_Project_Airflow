from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from Json_Processor import JSONProcessor
from Assign_Schema import SchemaAssigner

# Set the default arguments for the DAG
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'ETL_dag',
    default_args=default_args,
    description='DAG for processing JSON data and loading into BigQuery',
    schedule_interval=None,  # Set the schedule interval as needed
)

# Task to load and process JSON data
def process_json():
    json_processor = JSONProcessor()
    json_processor.load_json_file('/Users/dimitriskatsogiannis/Downloads/analytics-engineer-test/etl.json')
    json_processor.process_json_data()
    subscriptions, subscription_items, item_tiers, coupons, customers = json_processor.create_tables()

    # Remove specific columns
    subscriptions = JSONProcessor.remove_columns(subscriptions, ['subscription_items', 'item_tiers', 'coupons', 'object'])
    subscription_items = JSONProcessor.remove_columns(subscription_items, ['object'])
    item_tiers = JSONProcessor.remove_columns(item_tiers, ['object'])
    coupons = JSONProcessor.remove_columns(coupons, ['object'])
    customers = JSONProcessor.remove_columns(customers, ['object', 'billing_address'])

    # Convert specific values to dates
    subscriptions = JSONProcessor.convert_to_dates(subscriptions, ['current_term_start', 'current_term_end', 'next_billing_at', 'created_at', 'started_at', 'activated_at', 'updated_at', 'due_since'])
    coupons = JSONProcessor.convert_to_dates(coupons, ['apply_till'])
    customers = JSONProcessor.convert_to_dates(customers, ['created_at', 'updated_at'])

    return subscriptions, subscription_items, item_tiers, coupons, customers

process_json_task = PythonOperator(
    task_id='process_json',
    python_callable=process_json,
    dag=dag,
)

# Task to upload DataFrames to BigQuery tables
def upload_to_bigquery(**kwargs):
    ti = kwargs['ti']
    subscriptions, subscription_items, item_tiers, coupons, customers = ti.xcom_pull(task_ids='process_json')

    # Define BigQuery destinations
    bq_destinations = {
        'subscriptions': 'klausproject.ETL_Tasks.subscriptions',
        'subscription_items': 'klausproject.ETL_Tasks.subscription_items',
        'item_tiers': 'klausproject.ETL_Tasks.item_tiers',
        'coupons': 'klausproject.ETL_Tasks.coupons',
        'customers': 'klausproject.ETL_Tasks.customers'
    }



    for table_name, df in [('subscriptions', subscriptions),
                           ('subscription_items', subscription_items),
                           ('item_tiers', item_tiers),
                           ('coupons', coupons),
                           ('customers', customers)]:


        # Initiate Schema assignment
        schema_assigner = SchemaAssigner()

        # For each DataFrame, call assign_schema method
        schema = schema_assigner.assign_schema(table_name)

        create_table_task = BigQueryCreateEmptyTableOperator(
            task_id=f'create_{table_name}_table',
            schema_fields=schema,
            dataset_id='RawData',  # Add dataset_id
            table_id=table_name,  # Add table_id
            bigquery_conn_id='BigQueryConnection',
            dag=dag,
        )
        create_table_task.execute(context=kwargs)


upload_to_bigquery_task = PythonOperator(
    task_id='upload_to_bigquery',
    python_callable=upload_to_bigquery,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
process_json_task >> upload_to_bigquery_task
