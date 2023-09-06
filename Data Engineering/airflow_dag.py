"""
This DAG creates a landing table from a CSV file with student grades. Then, it creates a summary datamart table
with selected fields for students whose average grade is above 60%.
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import textwrap
from datetime import date

##############################################################################
# DAG Setup
##############################################################################

dag = DAG(
    'student_grades_tracker',

    # This translates to 8:45 PM CST
    schedule_interval="45 1 * * *",

    catchup=False,
    max_active_runs=1,
    retries=3
)

##############################################################################
# DAG
##############################################################################

t_create_landing_table = BashOperator(
    task_id="create_landing_table",
    bash_command=textwrap.dedent(f"""\
    bq --headless load \\
    --source_format=CSV \\
    --field_delimiter='\\t' \\
    --skip_leading_rows=1 \\
    --autodetect \\
    --replace \\
    'grades.student_grades_lnd' \\
    'gs://file_location_here'
    """),
    dag=dag
)

t_create_datamart_table = BashOperator(
    task_id="create_dm_table",
    bash_command=textwrap.dedent(f"""\
        bq --headless query \\
        --nouse_legacy_sql \\
        '
        CREATE OR REPLACE TABLE grades.student_grades_dm AS 
        WITH grades_data AS (
        SELECT student_id, student_name, student_grade, avg(student_grade) as avg_student_grade
        FROM grades.student_grades_lnd
        GROUP BY 1,2,3
        )
        SELECT * FROM grades_data where avg_student_grade > 0.6
        '
        """),
    dag=dag
)
t_create_datamart_table.set_upstream(t_create_landing_table)
