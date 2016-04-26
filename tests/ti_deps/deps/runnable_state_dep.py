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

import unittest
from datetime import datetime

from airflow.ti_deps.deps.runnable_state_dep import RunnableStateDep
from airflow.utils.state import State
from fake_models import FakeTI


class RunnableStateDepTest(unittest.TestCase):

    def test_runnable_state(self):
        """
        Runnable state should pass this dep
        """
        ti = FakeTI(state=State.QUEUED, end_date=datetime(2016, 1, 1))

        self.assertTrue(RunnableStateDep().is_met(ti=ti, dep_context=None))

    def test_non_runnable_state(self):
        """
        Non-runnable state should fail this dep
        """
        ti = FakeTI(state=State.SUCCESS, end_date=datetime(2016, 1, 1))

        self.assertFalse(RunnableStateDep().is_met(ti=ti, dep_context=None))
