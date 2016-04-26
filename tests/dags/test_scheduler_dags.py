# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators import BashOperator, DummyOperator

# DAG tests backfill with pooled tasks
# Previously backfill would queue the task but never run it
dag1 = DAG(
    dag_id='test_start_date_scheduling',
    start_date=datetime(2100, 1, 1))
dag1_task1 = DummyOperator(
    task_id='dummy',
    dag=dag1,
    owner='airflow')

# DAG tests that queued tasks are run
# TODODAN do I need 2 days instead of 1 here
dag2 = DAG(
    dag_id='test_scheduled_queued_tasks',
    start_date=datetime(2016, 1, 1),
    schedule_interval='@daily'
)
dag2_task1 = BashOperator(
    task_id='test_queued_pool_task1',
    bash_command="echo 0",
    dag=dag2,
    owner='airflow',
    pool='test_queued_pool')
dag2_task1 = BashOperator(
    task_id='test_queued_pool_task2',
    bash_command="echo 0",
    dag=dag2,
    owner='airflow',
    pool='test_queued_pool')
