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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import logging
import unittest

from airflow import AirflowException, settings
from airflow.bin import cli
from airflow.executors import DEFAULT_EXECUTOR
from airflow.jobs import BackfillJob, SchedulerJob
from airflow.models import DAG, DagBag, DagRun, Pool, TaskInstance as TI
from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow.utils.timeout import timeout

from airflow import configuration

configuration.test_mode()

DEV_NULL = '/dev/null'
DEFAULT_DATE = datetime.datetime(2016, 1, 1)


class SchedulerJobTest(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag()

    def test_scheduler_pooled_tasks(self):
        """
        Test that the scheduler handles queued tasks correctly
        """
        session = settings.Session()

        if not (
                session.query(Pool)
                .filter(Pool.pool == 'test_queued_pool')
                .first()):
            pool = Pool(pool='test_queued_pool', slots=1)
            session.merge(pool)
            session.commit()
        session.close()

        dag_id = 'test_scheduled_queued_tasks'
        dag = self.dagbag.get_dag(dag_id)
        dag.clear()

        scheduler = SchedulerJob(dag_id, num_runs=1)
        scheduler.run()

        # The DAG has two task instances, so the second one should be queued since the
        # pool only has one slot
        task_1 = dag.tasks[0]
        task_2 = dag.tasks[1]
        logging.info("Trying to find task {}".format(task_1))
        first_ti = TI(task_1, dag.start_date)
        second_ti = TI(task_2, dag.start_date)
        first_ti.refresh_from_db()
        second_ti.refresh_from_db()
        # TODODAN
        logging.info("LALA")
        logging.info(first_ti)
        logging.info(second_ti)
        try:
            with open('/home/travis/airflow/logs/example_bash_operator/runme_0/2015-01-01T00:00:00') as w:
                print (w.read())
                logging.info (w.read())
        except:
            pass
        logging.info("LOLO")
        self.assertEqual(first_ti.state, State.SUCCESS)
        self.assertEqual(second_ti.state, State.QUEUED)

        # Run the scheduler again to run the second task which was queued in the first run
        scheduler = SchedulerJob(dag_id, num_runs=1)
        scheduler.run()

        second_ti.refresh_from_db()
        self.assertEqual(second_ti.state, State.SUCCESS)
        dag.clear()
