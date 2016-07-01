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
from airflow.ti_deps.contexts.min_exec_context import MinExecContext
from airflow.ti_deps.deps.valid_state_dep import ValidStateDep
from airflow.utils.state import State


class QueueContext(MinExecContext):
    """
    Context to get the dependencies that need to be met for a given task instance to be
    able to get queued for execution regardless of available resources.
    """

    # Valid states a task can have in order for it to be able to get queued
    QUEUEABLE_STATES = {
        State.NONE,
        State.QUEUED,
        State.SKIPPED,
        State.UPSTREAM_FAILED,
        State.UP_FOR_RETRY,
    }

    def __init__(
            self,
            flag_upstream_failed=False,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False):
        """
        :param flag_upstream_failed: This is a hack to generate the upstream_failed state
            creation while checking to see whether the task instance is runnable. It was
            the shortest path to add the feature. This is bad since the methods in this
            class should be pure.
        :type flag_upstream_failed: boolean
        :param ignore_all_deps: Whether or not the context should ignore all ignoreable
            dependencies. Overrides the other ignore_* parameters
        :type ignore_all_deps: boolean
        :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs (e.g. for
            Backfills)
        :type ignore_depends_on_past: boolean
        :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past
            and trigger rule
        :type ignore_task_deps: boolean
        :param ignore_ti_state: Ignore the task instance's previous failure/success
        :type ignore_ti_state: boolean
        """
        super(QueueContext, self).__init__(
            flag_upstream_failed=flag_upstream_failed,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state)

    def get_ignoreable_deps(self, ti):
        deps = super(QueueContext, self).get_ignoreable_deps(ti)

        if not self._ignore_ti_state:
            deps.add(
                ValidStateDep(self.QUEUEABLE_STATES),
            )

        return deps
