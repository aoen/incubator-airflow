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
from datetime import datetime

from airflow.ti_deps.deps.base_ti_task_dep import BaseTITaskDep
from airflow.utils.db import provide_session
from airflow.utils.state import State


class NotInRetryPeriodDep(BaseTITaskDep):
    NAME = "Not In Retry Period"
    IGNOREABLE = True

    @provide_session
    def get_dep_statuses(self, ti, session, dep_context):
        super(NotInRetryPeriodDep, self).get_dep_statuses(ti, session, dep_context)

        if ti.state != State.UP_FOR_RETRY:
            yield self._passing_status(
                reason="The task instance was not marked for retrying.")
            raise StopIteration

        # Calculate the date first so that it is always smaller than the timestamp used by
        # ready_for_retry
        cur_date = datetime.now()
        next_task_retry_date = ti.end_date + ti.task.retry_delay
        if ti.is_premature:
            yield self._failing_status(
                reason="Task is not ready for retry yet but will be retried "
                       "automatically. Current date is {0} and task will be retried "
                       "at {1}.".format(cur_date.isoformat(),
                                        next_task_retry_date.isoformat()))
