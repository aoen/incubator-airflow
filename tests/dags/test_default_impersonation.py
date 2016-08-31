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

dag = DAG(dag_id='test_default_impersonation', default_args=args)

deelevated_user = 'airflow_test_user'

test_command = dedent(
    """\
    if [ '{user}' != "$(whoami)" ]; then
        echo current user is not {user}!
        exit 1
    fi
    """.format(user=deelevated_user))

task = BashOperator(
    task_id='test_deelevated_user',
    bash_command=test_command,
    dag=dag,
)

