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
from airflow.ti_deps.contexts.run_context import RunContext
from airflow.ti_deps.deps.dagrun_exists_dep import DagrunExistsDep
from airflow.ti_deps.deps.dag_unpaused_dep import DagUnpausedDep
from airflow.ti_deps.deps.exec_date_after_start_date_dep import ExecDateAfterStartDateDep


class SchedulerEndToEndContext(RunContext):
    """
    Context to get the dependencies that need to be met for a given task instance to get
    scheduled by the scheduler, then queued by the scheduler, then run by an executor.
    """

    def get_ignoreable_deps(self, ti):
        # TODO(aoen): The dependencies below are not coupled to actual execution in any
        # way and could easily be modified or removed from the scheduler causing
        # this dependency to become outdated and incorrect. This coupling should be
        # created (e.g. via a dag_deps analog of ti_deps) to ensure that the logic here is
        # equivalent to the logic in the scheduler.
        return super(SchedulerEndToEndContext, self).get_ignoreable_deps(ti) | {
            DagrunExistsDep(),
            DagUnpausedDep(),
            ExecDateAfterStartDateDep(),
        }
