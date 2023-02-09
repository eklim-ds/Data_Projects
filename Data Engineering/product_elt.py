"""


"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# from helpers import bq
# from helpers import settings
from datetime import datetime

##############################################################################
# Task Settings
##############################################################################

myproject = 'ek_project'
today = datetime.today().date()
gcs_blob_path = ''

##############################################################################
# DAG Setup
##############################################################################

dag = DAG(
    'product_landing',

   schedule_interval="45 1 * * *",
    max_active_runs=1,
    catchup=False,
    default_args={
        'depends_on_past': False,
        'retries': 3
    }
)

##############################################################################
# DAG
##############################################################################

 # adding this step as we expect files to be archived after 30 days

t_check_gcs = BashOperator(
        task_id=f'check_gcs_for_product',
        on_failure_callback=None,
        bash_command=f"""gsutil -q stat {gcs_blob_path}""",
        dag=dag
    )
 
t_create_landing = BashOperator(
        task_id=f'create_landing_table',
        on_failure_callback=None,
        bash_command=f"""\
         bq --headless mk \\
         --force \\
         --time_partitioning_type=DAY \\
         'product_landing.product' \\
          '/home/airflow/gcs/dags/Data Engineering/product_landing/product.json' && \\
          bq --headless update \\
          --time_partitioning_type=DAY \\
          'product_landing.product' \\
          '/home/airflow/gcs/dags/Data Engineering/product_landing/product.json' && \\
    """,
    dag=dag
    )

t_load_from_gcs_to_bq = PythonOperator(
        task_id=f'load_landing_product',
        execution_timeout=timedelta(minutes=60),
        python_callable=python_bq.load_from_gcs_to_bq,
        op_kwargs={
            'dataset': 'product_landing',
            'table': f'{target_table}${target_partition}',
            'uri': f'{settings.GCS_STATIC_PATH}/{file_name}',
            'jobopts': file
        },
    dag=dag
    )

t_create_datamart = BashOperator(
        task_id=f'create_landing_table',
        on_failure_callback=None,
        bash_command=f"""
         
    """,
    dag=dag
    )

t_create_datamart = BashOperator(
        task_id=f'',
        on_failure_callback=None,
        bash_command=f"""
         
    """,
    dag=dag
    )

t_create_aggregated_table= BashOperator(
        task_id=f'',
        on_failure_callback=None,
        bash_command=f"""
         
    """,
    dag=dag
    )

t_create_pivoted_table= BashOperator(
        task_id=f'',
        on_failure_callback=None,
        bash_command=f"""
         
    """,
    dag=dag
    )

t_check_gcs >> t_create_landing >> t_load_from_gcs_to_bq
t_create_datamart >> t_create_aggregated_table >> t_create_pivoted_table