from collections import namedtuple
from datetime import datetime
from sqlalchemy import case, func

import airflow
from airflow.utils.db import provide_session
from airflow.utils.state import State


class BaseTIDep(object):
    """
    Abstract base class for dependencies that must be satisfied in order for task
    instances to run. For example, a task can only run if a certain number of its upstream
    tasks succeed. This is an abstract class and must be subclassed to be used.
    """

    # TODODAN docstring
    def __init__(self, name=None):
        # By default use the classname for the dependency name
        if name is None:
            self._name = self.__class__.__name__

    # TODODAN docstring
    @property
    def name(self):
        return self._name

    def get_dep_statuses(self, ti, session, exec_context):
        """
        Returns an iterable of TIDepStatus objects that describe whether the given task
        instance has this dependency met.

        For example a subclass could return an iterable of TIDepStatus objects, each one
        representing if each of the passed in task's upstream tasks succeeded or not.

        :param ti: the task instance to get the dependency status for
        :type ti: TaskInstance
        """
        raise NotImplementedError

    def is_met(self, ti, session, exec_context):
        """
        Returns whether or not this dependency is met for a given task instance. A
        dependency is considered met if all of the dependency statuses it reports are
        passing.

        :param ti: the task instance to see if this dependency is met for
        :type ti: TaskInstance
        """
        return all(status.passed for status in
                   self.get_dep_statuses(ti, session, exec_context))

    def get_failure_reasons(self, ti, session, exec_context):
        """
        Returns an iterable of strings that explain why this dependency wasn't met.

        :param ti: the task instance to get the dependency failure reasons for
        :type ti: TaskInstance
        """
        for dep_status in self.get_dep_statuses(ti, session, exec_context):
            if not dep_status.passed:
                yield dep_status.reason

    def passing_status(self, dep_name=None, reason=''):
        if dep_name is None:
            dep_name = self.name
        return TIDepStatus(dep_name, True, reason)

    def failing_status(self, dep_name=None, reason=''):
        if dep_name is None:
            dep_name = self.name
        return TIDepStatus(dep_name, False, reason)


"""
Status of a task instance dependency is met and reasons why it isn't met.
"""
TIDepStatus = namedtuple('TIDepStatus', ['dep_name', 'passed', 'reason'])


class ExecDateDep(BaseTIDep):
    def get_dep_statuses(self, ti, session, exec_context):
        cur_date = datetime.now()

        if ti.execution_date > cur_date:
            yield self.failing_status(
                reason="Execution date {0} is in the future (the current "
                       "date is {1}).".format(ti.execution_date.isoformat(),
                                              cur_date.isoformat()))

        if ti.task.end_date and ti.execution_date > ti.task.end_date:
            yield self.failing_status(
                reason="The execution date is {0} but this is after the task's end date "
                "{1}.".format(
                    ti.task.end_date.isoformat(),
                    ti.execution_date().isoformat()))


class InRunnableStateDep(BaseTIDep):
    def get_dep_statuses(self, ti, session, exec_context):

        # Ignore tasks that are already running since these tasks are handled in the
        # NotRunningDep dependency
        if ti.state == State.RUNNING or ti.state in State.runnable():
            return

        if ti.state in [State.FAILED, State.SUCCESS]:
            reason = ("Task previously completed with status '{0}' on {1}. It must be "
                      "cleared to be rerun.".format(ti.state, ti.end_date))
        else:
            reason = ("Task is in the '{0}' state which is not a runnable "
                      "state.".format(ti.state))

        yield self.failing_status(reason=reason)


class NotRunningDep(BaseTIDep):
    def get_dep_statuses(self, ti, session, exec_context):
        if ti.state == State.RUNNING:
            yield self.failing_status(
                reason="Task is already running, it started on {0}.".format(
                    ti.start_date))


class DagUnpausedDep(BaseTIDep):
    def get_dep_statuses(self, ti, session, exec_context):
        if ti.task.dag.is_paused:
            yield self.failing_status(
                reason="Task's DAG '{0}' is paused.".format(ti.dag_id))


class MaxConcurrencyDep(BaseTIDep):
    def get_dep_statuses(self, ti, session, exec_context):
        if ti.task.dag.concurrency_reached:
            yield self.failing_status(
                reason="The maximum number of running tasks ({0}) for this task's DAG "
                       "'{1}' has been reached.".format(ti.dag_id,
                                                        ti.task.dag.concurrency))


class MaxDagrunsDep(BaseTIDep):
    def get_dep_statuses(self, ti, session, exec_context):
        if ti.task.dag.concurrency_reached:
            yield self.failing_status(
                reason="The maximum number of active dag runs ({0}) for this task's DAG "
                       "'{1}' has been reached.".format(ti.dag_id,
                                                        ti.task.dag.max_active_runs))


class NotQueuedDep(BaseTIDep):
    def get_dep_statuses(self, ti, session, exec_context):
        if ti.state == State.QUEUED:
            yield self.failing_status(
                reason="The task instance has already been queued and will run shortly.")


class NotInRetryPeriodDep(BaseTIDep):
    def get_dep_statuses(self, ti, session, exec_context):
        if ti.state != State.UP_FOR_RETRY:
            return

        # Calculate the date first so that it is always smaller than the timestamp used by
        # ready_for_retry
        cur_date = datetime.now()
        next_task_retry_date = ti.end_date + ti.task.retry_delay
        if not ti.ready_for_retry():
            yield self.failing_status(
                reason="Task is not ready for retry yet but will be retried "
                        "automatically. Current date is {0} and task will be retried "
                        "at {1}.".format(cur_date.isoformat(),
                                        next_task_retry_date.isoformat()))


class NotSkippedDep(BaseTIDep):
    def get_dep_statuses(self, ti, session, exec_context):
        if ti.state == State.SKIPPED:
            yield self.failing_status(reason="The task instance has been skipped.")


class PastDagrunDep(BaseTIDep):
    """
    Is the past dagrun in a state that allows this task instance to run, e.g. did
    this task instance's task in the previous dagrun complete if we are depending on past
    """

    def get_dep_statuses(self, ti, session, exec_context):
        # TODODAN probably need logging for the returns or first yielding a success status
        if exec_context.ignore_depends_on_past:
            return

        if not ti.task.depends_on_past:
            return

        # Don't depend on the previous task instance if we are the first task
        if ti.execution_date == ti.task.start_date:
            return

        previous_ti = ti.previous_ti
        if not previous_ti:
            yield self.failing_status(
                reason="depends_on_past is true for this task, but the previous task "
                        "instance has not run yet.")

        if previous_ti.state not in [State.SUCCESS, State.SKIPPED]:
            yield self.failing_status(
                reason="depends_on_past is true for this task, but the previous task "
                        "instance is in the state '{0}' which is not a successful "
                        "state.".format(previous_ti.state))

        previous_ti.task = ti.task
        if (ti.task.wait_for_downstream and
                not previous_ti.are_dependents_done(session=session)):
            yield self.failing_status(
                reason="The tasks downstream of the previous task instance haven't "
                        "completed.")


class PoolHasSpaceDep(BaseTIDep):
    def get_dep_statuses(self, ti, session, exec_context):
        if ti.pool_full():
            yield self.failing_status(reason="Task's pool '{0}' is full.".format(ti.pool))


class TriggerRuleDep(BaseTIDep):
    """
    Determines if a task's upstream tasks are in a state that allows a given task instance
    to run.
    """

    def get_dep_statuses(self, ti, session, exec_context):
        TI = airflow.models.TaskInstance
        TR = airflow.models.TriggerRule

        # Checking that all upstream dependencies have succeeded
        if not ti.task.upstream_list or ti.task.trigger_rule == TR.DUMMY:
            return

        qry = (
            session
            .query(
                func.coalesce(func.sum(
                    case([(TI.state == State.SUCCESS, 1)], else_=0)), 0),
                func.coalesce(func.sum(
                    case([(TI.state == State.SKIPPED, 1)], else_=0)), 0),
                func.coalesce(func.sum(
                    case([(TI.state == State.FAILED, 1)], else_=0)), 0),
                func.coalesce(func.sum(
                    case([(TI.state == State.UPSTREAM_FAILED, 1)], else_=0)), 0),
                func.count(TI.task_id),
            )
            .filter(
                TI.dag_id == ti.dag_id,
                TI.task_id.in_(ti.task.upstream_task_ids),
                TI.execution_date == ti.execution_date,
                TI.state.in_([
                    State.SUCCESS, State.FAILED,
                    State.UPSTREAM_FAILED, State.SKIPPED]),
            )
        )

        successes, skipped, failed, upstream_failed, done = qry.first()

        for dep_status in self._evaluate_trigger_rule(
                              ti=ti,
                              successes=successes,
                              skipped=skipped,
                              failed=failed,
                              upstream_failed=upstream_failed,
                              done=done,
                              flag_upstream_failed=exec_context.flag_upstream_failed,
                              session=session):
            yield dep_status


    @provide_session
    def _evaluate_trigger_rule(
            self,
            ti,
            successes,
            skipped,
            failed,
            upstream_failed,
            done,
            flag_upstream_failed,
            session):
        """
        TODODAN: this docstring was from the old stuff, make it work with the new stuff
        :param flag_upstream_failed: This is a hack to generate
            the upstream_failed state creation while checking to see
            whether the task instance is runnable. It was the shortest
            path to add the feature
        :type flag_upstream_failed: boolean
        :param successes: Number of successful upstream tasks
        :type successes: boolean
        :param skipped: Number of skipped upstream tasks
        :type skipped: boolean
        :param failed: Number of failed upstream tasks
        :type failed: boolean
        :param upstream_failed: Number of upstream_failed upstream tasks
        :type upstream_failed: boolean
        :param done: Number of completed upstream tasks
        :type done: boolean
        """

        TR = airflow.models.TriggerRule

        task = ti.task
        upstream = len(task.upstream_task_ids)
        tr = task.trigger_rule
        upstream_done = done >= upstream

        # handling instant state assignment based on trigger rules
        # TODODAN TODO(aoen): trigger rules should probably be rewritten as a subclass of
        # BaseTIDep or contain a BaseTIDep, and then the logic could be broken up per each
        # trigger rule class
        if flag_upstream_failed:
            if tr == TR.ALL_SUCCESS:
                if upstream_failed or failed:
                    ti.set_state(State.UPSTREAM_FAILED, session)
                elif skipped:
                    ti.set_state(State.SKIPPED, session)
            elif tr == TR.ALL_FAILED:
                if successes or skipped:
                    ti.set_state(State.SKIPPED, session)
            elif tr == TR.ONE_SUCCESS:
                if upstream_done and not successes:
                    ti.set_state(State.SKIPPED, session)
            elif tr == TR.ONE_FAILED:
                if upstream_done and not (failed or upstream_failed):
                    ti.set_state(State.SKIPPED, session)

        if tr == TR.ONE_SUCCESS:
            if successes <= 0:
                yield self.failing_status(
                    reason="Task's trigger rule '{0}' requires one upstream task "
                           "success, but none were found.".format(tr))
        elif tr == TR.ONE_FAILED:
            if not failed and not upstream_failed:
                yield self.failing_status(
                    reason="Task's trigger rule '{0}' requires one upstream task failure "
                           "but none, were found.").format(tr)
        elif tr == TR.ALL_SUCCESS:
            num_failures = upstream - successes
            if num_failures > 0:
                yield self.failing_status(
                    reason="Task's trigger rule '{0}' requires all upstream tasks to "
                           "have succeeded, but found {1} non-success(es)."
                           .format(tr, num_failures))
        elif tr == TR.ALL_FAILED:
            num_successes = upstream - failed - upstream_failed
            if num_successes > 0:
                yield self.failing_status(
                    reason="Task's trigger rule '{0}' requires all upstream tasks to "
                           "have failed, but found {1} non-faliure(s)."
                           .format(tr, num_successes))
        elif tr == TR.ALL_DONE:
            if not upstream_done:
                yield self.failing_status(
                    reason="Task's trigger rule '{0}' requires all upstream tasks to "
                           "have completed, but found '{1}' task(s) that weren't done."
                           .format(tr, upstream - done))
        else:
            yield self.failing_status(
                reason="No strategy to evaluate trigger rule '{0}'.".format(tr))
