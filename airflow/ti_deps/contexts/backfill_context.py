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


class BackfillContext(RunContext):
    """
    The main difference between backfilling and a regular task run's dependencies is
    that a backfill ignores a task instance's previous state (e.g. if the task succeeded
    already).
    """

    def __init__(
            self,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False):
        """
        :param ignore_all_deps: Whether or not the context should ignore all ignoreable
            dependencies. Overrides the other ignore_* parameters
        :type ignore_all_deps: boolean
        :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs (e.g. for
            Backfills)
        :type ignore_depends_on_past: boolean
        :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past
               and trigger rule
        :type ignore_task_deps: boolean
        """
        super(BackfillContext, self).__init__(
            flag_upstream_failed=True,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=True)
