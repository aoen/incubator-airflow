import os
from airflow.operators import BashOperator
from airflow.models import DAG
from datetime import datetime
from textwrap import dedent


DEFAULT_DATE = datetime(2016, 1, 1)

args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE,
}

dag = DAG(dag_id='test_no_impersonation', default_args=args)

test_command = dedent(
    """\
    if [ 0 -ne "$(id -u)" ]; then
        echo 'current uid is not 0 (superuser)!'
        exit 1
    fi
    """)

task = BashOperator(
    task_id='test_superuser',
    bash_command=test_command,
    dag=dag,
)
