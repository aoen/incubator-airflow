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
# from __future__ import print_function
import os
import subprocess
import unittest

from airflow import jobs, models
from airflow.utils.state import State
from datetime import datetime

DEV_NULL = '/dev/null'
TEST_DAG_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'dags')
DEFAULT_DATE = datetime(2015, 1, 1)
TEST_USER = 'airflow_test_user'
SUPERUSER_UID = 0

if os.getuid() == SUPERUSER_UID:
    class ImpersonationTest(unittest.TestCase):
        def setUp(self):
            self.dagbag = models.DagBag(
                dag_folder=TEST_DAG_FOLDER,
                include_examples=False,
            )

        def run_backfill(self, dag_id, task_id, test_user=False):
            if test_user:
                subprocess.call(['useradd', '-m', TEST_USER])

            try:
                dag = self.dagbag.get_dag(dag_id)
                dag.clear()

                job = jobs.BackfillJob(
                    dag=dag,
                    start_date=DEFAULT_DATE,
                    end_date=DEFAULT_DATE).run()

                ti = models.TaskInstance(
                    task=dag.get_task(task_id),
                    execution_date=DEFAULT_DATE)
                ti.refresh_from_db()
                self.assertEqual(ti.state, State.SUCCESS)
            finally:
                if test_user:
                    subprocess.call(['userdel', '-r', TEST_USER])

        def test_impersonation(self):
            """
            Tests that impersonating a unix user works
            """
            self.run_backfill(
                'test_impersonation',
                'test_impersonated_user',
                test_user=True,
            )

        def test_no_impersonation(self):
            """
            If default_impersonation=None, tests that the job is run
            as the current user (which will be a sudoer)
            """
            self.run_backfill(
                'test_no_impersonation',
                'test_superuser',
            )

        def test_default_impersonation(self):
            """
            If default_impersonation=TEST_USER, tests that the job defaults
            to running as TEST_USER for a test without run_as_user set
            """
            os.environ['AIRFLOW__CORE__DEFAULT_IMPERSONATION'] = TEST_USER

            try:
                self.run_backfill(
                    'test_default_impersonation',
                    'test_deelevated_user',
                    test_user=True,
                )
            finally:
                del os.environ['AIRFLOW__CORE__DEFAULT_IMPERSONATION']
