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


class BaseTITaskDep(BaseTIDep):
    """
    Abstract base class for task instance dependencies that are specified at the task
    level (as opposed to e.g. an infrastructure dependency such as available slots for a
    task instance).
    """

    @provide_session
    def get_dep_statuses(self, ti, session, dep_context):
        """
        Extends the super method with support for recognizing the ignore_ti_state context
        option.

        :param ti: the task instance to get the dependency status for
        :type ti: TaskInstance
        :param session: database session
        :type session: Session
        :param dep_context: the context for which this dependency should be evaluated for
        :type dep_context: DepContext
        """
        super(BaseTITaskDep, self).get_dep_statuses(ti, session, dep_context)

        if dep_context.ignore_ti_state:
            yield self._passing_status(
                reason="Context specified all task dependencies should be ignored.")
            raise StopIteration
