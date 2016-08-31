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

dag = DAG(dag_id='test_impersonation', default_args=args)

run_as_user = 'airflow_test_user'

test_command = dedent(
    """\
    if [ '{user}' != "$(whoami)" ]; then
        echo current user is not {user}!
        exit 1
    fi
    """.format(user=run_as_user))

task = BashOperator(
    task_id='test_impersonated_user',
    bash_command=test_command,
    dag=dag,
    run_as_user=run_as_user,
)
