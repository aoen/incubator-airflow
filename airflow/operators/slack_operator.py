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

import os
import random
from slackclient import SlackClient
from airflow.models import BaseOperator, Connection
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import json
import logging
from airflow import settings

CONN_ENV_PREFIX = 'AIRFLOW_CONN_'


class SlackAPIOperator(BaseOperator):
    """
    Base Slack Operator
    The SlackAPIPostOperator is derived from this operator.
    In the future additional Slack API Operators will be derived from this class as well

    :param token: Slack API token (https://api.slack.com/web)
    :type token: string
    :param method: The Slack API Method to Call (https://api.slack.com/methods)
    :type method: string
    :param api_params: API Method call parameters (https://api.slack.com/methods)
    :type api_params: dict
    """

    @apply_defaults
    def __init__(self,
                 slack_conn_id='unset',
                 token='unset',
                 method='unset',
                 api_params=None,
                 *args, **kwargs):
        super(SlackAPIOperator, self).__init__(*args, **kwargs)
        self.token = self.__get_token(token, slack_conn_id)
        self.method = method
        self.api_params = api_params

    def __get_token(self, token, slack_conn_id):
        if token == 'unset' and slack_conn_id == 'unset':
            logging.error('No valid Slack token nor slack_conn_id supplied.')
            raise AirflowException('No valid Slack token supplied.')
        if token != 'unset' and slack_conn_id != 'unset':
            logging.error('Cannot determine Slack credential when both token and slack_conn_id are supplied.')
            raise AirflowException('Cannot determine Slack credential when both token and slack_conn_id are supplied.')
        if token != 'unset':
            return token
        else:
            conn = self.__get_connection(slack_conn_id)

            if not hasattr(conn, 'password') or not getattr(conn, 'password'):
                raise AirflowException('Missing token(password) in Slack connection')
            return conn.password

    # TODO move connection related logic into a new slack hook.
    def __get_connection(self, conn_id):
        environment_uri = os.environ.get(CONN_ENV_PREFIX + conn_id.upper())
        if environment_uri:
            conn = Connection(conn_id=conn_id, uri=environment_uri)
        else:
            conn = random.choice(self.__get_connections(conn_id))
        return conn

    def __get_connections(self, conn_id):
        session = settings.Session()
        conns = (
            session.query(Connection)
                .filter(Connection.conn_id == conn_id)
                .all()
        )
        session.expunge_all()
        session.close()
        if not conns:
            raise AirflowException(
                "The conn_id `{0}` isn't defined".format(conn_id))
        return conns

    def construct_api_call_params(self):
        """
        Used by the execute function. Allows templating on the source fields of the api_call_params dict before construction

        Override in child classes.
        Each SlackAPIOperator child class is responsible for having a construct_api_call_params function
        which sets self.api_call_params with a dict of API call parameters (https://api.slack.com/methods)
        """

        pass

    def execute(self, **kwargs):
        """
        SlackAPIOperator calls will not fail even if the call is not unsuccessful.
        It should not prevent a DAG from completing in success
        """
        if not self.api_params:
            self.construct_api_call_params()
        sc = SlackClient(self.token)
        rc = sc.api_call(self.method, **self.api_params)
        if not rc['ok']:
            logging.error("Slack API call failed ({})".format(rc['error']))
            raise AirflowException("Slack API call failed: ({})".format(rc['error']))


class SlackAPIPostOperator(SlackAPIOperator):
    """
    Posts messages to a slack channel

    :param channel: channel in which to post message on slack name (#general) or ID (C12318391)
    :type channel: string
    :param username: Username that airflow will be posting to Slack as
    :type username: string
    :param text: message to send to slack
    :type text: string
    :param icon_url: url to icon used for this message
    :type icon_url: string
    :param attachments: extra formatting details - see https://api.slack.com/docs/attachments
    :type attachments: array of hashes
    """

    template_fields = ('username', 'text', 'attachments')
    ui_color = '#FFBA40'

    @apply_defaults
    def __init__(self,
                 channel='#general',
                 username='Airflow',
                 text='No message has been set.\n'
                      'Here is a cat video instead\n'
                      'https://www.youtube.com/watch?v=J---aiyznGQ',
                 icon_url='https://raw.githubusercontent.com/airbnb/airflow/master/airflow/www/static/pin_100.png',
                 attachments=None,
                 *args, **kwargs):
        self.method = 'chat.postMessage'
        self.channel = channel
        self.username = username
        self.text = text
        self.icon_url = icon_url
        self.attachments = attachments
        super(SlackAPIPostOperator, self).__init__(method=self.method,
                                                   *args, **kwargs)

    def construct_api_call_params(self):
        self.api_params = {
            'channel': self.channel,
            'username': self.username,
            'text': self.text,
            'icon_url': self.icon_url,
            'attachments': json.dumps(self.attachments),
        }
