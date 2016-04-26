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
from airflow.utils.state import State


class RunnableStateDep(BaseTIDep):
    NAME = "Task Instance State"

    @provide_session
    def get_dep_statuses(self, ti, session, dep_context):
        if ti.state in State.runnable():
            raise StopIteration

        if ti.state in [State.FAILED, State.SUCCESS]:
            reason = ("Task previously completed with status '{0}' on {1}. It must be "
                      "cleared to be rerun.".format(ti.state, ti.end_date))
        else:
            reason = ("Task is in the '{0}' state which is not a runnable "
                      "state.".format(ti.state))

        yield self._failing_status(reason=reason)
