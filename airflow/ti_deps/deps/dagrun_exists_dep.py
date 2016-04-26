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
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.db import provide_session


class DagrunExistsDep(BaseTIDep):
    NAME = "Dagrun Exists"

    @provide_session
    def get_dep_statuses(self, ti, session, dep_context):
        dag = ti.task.dag
        dag_run = ti.get_dagrun(session)
        if dag_run:
            yield self._passing_status(reason="Task instance has a dagrun")
            raise StopIteration

        if len(dag.get_running_dagruns(session)) >= dag.max_active_runs:
            reason = ("The maximum number of active dag runs ({0}) for this task "
                      "instance's DAG '{1}' has been reached.".format(
                          dag.max_active_runs,
                          ti.dag_id))
        else:
            reason = "Unknown reason"
        yield self._failing_status(
            reason="Task instance's dagrun did not exist: {0}".format(reason))
