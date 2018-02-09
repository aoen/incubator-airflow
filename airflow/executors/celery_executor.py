# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from builtins import object
from multiprocessing import Pool
import logging
import math
import subprocess
import time
import traceback

from celery import Celery
from celery import states as celery_states

from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow import configuration

PARALLELISM = configuration.get('core', 'PARALLELISM')

'''
To start the celery worker, run the command:
airflow worker
'''

DEFAULT_QUEUE = configuration.get('celery', 'DEFAULT_QUEUE')


class CeleryConfig(object):
    CELERY_ACCEPT_CONTENT = ['json', 'pickle']
    CELERY_EVENT_SERIALIZER = 'json'
    CELERY_RESULT_SERIALIZER = 'pickle'
    CELERY_TASK_SERIALIZER = 'pickle'
    CELERYD_PREFETCH_MULTIPLIER = 1
    CELERY_ACKS_LATE = True
    BROKER_URL = configuration.get('celery', 'BROKER_URL')
    CELERY_RESULT_BACKEND = configuration.get('celery', 'CELERY_RESULT_BACKEND')
    CELERYD_CONCURRENCY = configuration.getint('celery', 'CELERYD_CONCURRENCY')
    CELERY_DEFAULT_QUEUE = DEFAULT_QUEUE
    CELERY_DEFAULT_EXCHANGE = DEFAULT_QUEUE


app = Celery(
    configuration.get('celery', 'CELERY_APP_NAME'),
    config_source=CeleryConfig)


@app.task
def execute_command(command):
    logging.info("Executing command in Celery " + command)
    try:
        subprocess.check_call(command, shell=True)
    except subprocess.CalledProcessError as e:
        logging.error(e)
        raise AirflowException('Celery command failed')


class ExceptionWithTraceback(object):
    def __init__(self, exception, traceback):
        self.exception = exception
        self.traceback = traceback


def fetch_celery_task_state(celery_task):
    """
    Fetch and return the state of the given celery task. The scope of this function is
    global so that it can be called by subprocesses in the pool.

    :param celery_task: a tuple of the Celery task key and the async Celery object used
                        to fetch the task's state
    :type celery_task: (str, celery.result.AsyncResult)
    :return: a tuple of the Celery task key and the Celery state of the task
    :rtype: (str, str)
    """

    try:
        res = (celery_task[0], celery_task[1].state)
    except Exception as e:
        res = ExceptionWithTraceback(e, traceback.format_exc())
    return res


class CeleryExecutor(BaseExecutor):
    """
    CeleryExecutor is recommended for production use of Airflow. It allows
    distributing the execution of task instances to multiple worker nodes.

    Celery is a simple, flexible and reliable distributed system to process
    vast amounts of messages, while providing operations with the tools
    required to maintain such a system.
    """
    def __init__(self, sync_parallelism=None):
        super(CeleryExecutor, self).__init__()

        # Parallelize Celery requests here since Celery does not support it
        if sync_parallelism is None:
            # How many processes are created for checking celery task state
            self._sync_parallelism = configuration.getint('celery', 'SYNC_PARALLELISM')
        else:
            self._sync_parallelism = sync_parallelism

        if not self._sync_parallelism:
            raise ValueError(
                'Please provide a Celery Executor SYNC_PARALLELISM in airflow.cfg')

        self._sync_pool = None

    def start(self):
        self.logger.debug('Starting Celery Executor using {} processes for syncing'.format(
            self._sync_parallelism))
        self.tasks = {}
        self.last_state = {}

    def execute_async(self, key, command, queue=DEFAULT_QUEUE):
        self.logger.info("[celery] queuing {key} through celery, "
                         "queue={queue}".format(**locals()))
        self.tasks[key] = execute_command.apply_async(
            args=[command], queue=queue)
        self.last_state[key] = celery_states.PENDING

    def num_tasks_per_process(self):
        """
        How many Celery tasks should be sent to each worker process.

        :return: Number of tasks that should be used per process
        :rtype: int
        """
        return max(1,
                   int(math.ceil(1.0 * len(self.tasks) / self._sync_parallelism)))

    def sync(self):
        self.logger.debug("Inquiring about %s celery task(s)", len(self.tasks))

        # Recreate the process pool each sync in case processes in the pool die
        self._sync_pool = Pool(processes=self._sync_parallelism)

        # Use chunking instead of a work queue to reduce context switching since tasks are
        # roughly uniform in size
        chunksize = self.num_tasks_per_process()

        result = self._sync_pool.map_async(
            fetch_celery_task_state,
            self.tasks.items(),
            chunksize=chunksize)

        self.logger.debug("Waiting for inquiries to complete...")
        task_keys_to_states = result.get()
        self._sync_pool.close()
        self._sync_pool.join()
        self.logger.debug("Inquiries completed.")

        for key_and_state in task_keys_to_states:
            if isinstance(key_and_state, ExceptionWithTraceback):
                logging.error(
                    "Error fetching Celery task state, ignoring it:{}\n{}\n".format(
                        key_and_state.exception, key_and_state.traceback))
            else:
                key, state = key_and_state
                try:
                    if self.last_state[key] != state:
                        if state == celery_states.SUCCESS:
                            # TTODOTODOTODOTODOTODOTODOTODOTODOTODOTODOTODOTODOODO remove this  and the print
                            print "TASK SUCCEEDED!"
                            self.success(key)
                            del self.tasks[key]
                            del self.last_state[key]
                        elif state == celery_states.FAILURE:
                            self.fail(key)
                            del self.tasks[key]
                            del self.last_state[key]
                        elif state == celery_states.REVOKED:
                            self.fail(key)
                            del self.tasks[key]
                            del self.last_state[key]
                        else:
                            self.logger.info("Unexpected state: " + state)
                        self.last_state[key] = state
                except Exception as e:
                    logging.error("Error syncing the Celery executor, ignoring "
                                  "it:\n{}\n".format(e, traceback.format_exc()))

    def end(self, synchronous=False):
        if synchronous:
            while any([
                    async.state not in celery_states.READY_STATES
                    for async in self.tasks.values()]):
                time.sleep(5)

        if self._sync_pool:
            self._sync_pool.terminate()
        else:
            raise AirflowException("Tried to end executor before starting it.")
