# Copyright 2019 Hayo van Loon
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
from datetime import timezone
from functools import wraps
from typing import Union

from flask import Flask, Response, make_response, redirect, request, session
from google.cloud import bigquery as bq

import auth
import demo
from utils import days_since_epoch

"""
This App requires quite a few environment parameters to operate. 

GOOGLE_CLOUD_PROJECT: 
The Google cloud project id (required) 

DEBUG_AUTH:
A flag indicating whether to use a full OAuth2 authentication flow on a local 
development server as well. Useful when testing OAuth2; set to non-false-y value 
to activate.

APPENGINE_CLIENT_ID: 
The app engine id which generated from https://console.cloud.google.com/apis/credentials
Only required when authentication is active.

APPENGINE_CLIENT_SECRET:
The secret generated along with the id. Never store it in a public location 
(like a repository). Only required when authentication is active.

APPENGINE_USERS:
A comma-separated list of email addresses. Only required when authentication is
active, otherwise 'test@example.com' will be used.
"""

PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT')

app = Flask(__name__)
app.secret_key = auth.CLIENT_SECRET

client = bq.Client()

DATASET = bq.DatasetReference(PROJECT, 'bobs_knob_shops')
DAILIES = [bq.TableReference(DATASET, 'events_0'),
           bq.TableReference(DATASET, 'events_1')]
SESSIONS = bq.TableReference(DATASET, 'sessions')

TZ = timezone.utc

USERS = os.getenv('APPENGINE_USERS', '').split(',')
if not USERS and not os.getenv('DEBUG_AUTH'):
    USERS = ['test@example.com']


def for_query(x: Union[str, bq.DatasetReference, bq.TableReference]) -> str:
    try:
        dataset = x.project + '.' + x.dataset_id
    except AttributeError:
        return x
    try:
        return dataset + '.' + x.table_id
    except AttributeError:
        return dataset


# noinspection SqlNoDataSourceInspection,SqlDialectInspection
AGGREGATION_QUERY = """
INSERT INTO `{}` (
  channel_id, visitor_id, timezone_offset, start_time, end_time, hit_count, hits
)
SELECT
  channel_id,
  visitor_id,
  MAX(timezone_offset) timezone_offset,
  MIN(timestamp) start_time,
  MAX(timestamp) end_time,
  COUNT(1) hit_count,
  ARRAY_AGG(STRUCT(
    timestamp AS timestamp,
    url AS url,
    referrer_url AS referrer_url
  ) ORDER BY timestamp) hits
FROM (
  SELECT
    *,
    MAX(timestamp) OVER (PARTITION BY visitor_id) last_timestamp
  FROM
    `{}.events_*` )
WHERE
  last_timestamp <= TIMESTAMP(CURRENT_DATE())
GROUP BY
  channel_id,
  visitor_id
""".format(for_query(SESSIONS), for_query(DATASET))

# noinspection SqlNoDataSourceInspection,SqlDialectInspection
LEFTOVER_QUERY = """
INSERT INTO `{}` (
  channel_id, visitor_id, timestamp, timezone_offset,
  url, referrer_url
)
SELECT
  channel_id,
  visitor_id,
  timestamp,
  timezone_offset,
  url,
  referrer_url
FROM (
  SELECT
    *,
    MAX(timestamp) OVER (PARTITION BY visitor_id) last_timestamp
  FROM
    `{}.events_*` )
WHERE
  timestamp < TIMESTAMP(CURRENT_DATE())
  AND last_timestamp >= TIMESTAMP(CURRENT_DATE())
"""

# noinspection SqlNoDataSourceInspection,SqlDialectInspection
CREATE_SESSIONS_QUERY = """
CREATE TABLE `{}` (
  channel_id STRING,
  visitor_id STRING,
  timezone_offset INT64,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  hit_count INT64,
  hits ARRAY<STRUCT< 
    timestamp TIMESTAMP,
    url STRING,
    referrer_url STRING 
  >>)
PARTITION BY
  DATE(start_time)
CLUSTER BY
  channel_id,
  visitor_id
""".format(for_query(SESSIONS))

# noinspection SqlNoDataSourceInspection,SqlDialectInspection
CREATE_EVENTS_QUERY = """
CREATE TABLE `{}` ( 
  channel_id STRING,
  visitor_id STRING,
  timestamp TIMESTAMP,
  timezone_offset INT64,
  url STRING,
  referrer_url STRING
)
"""


def require_oauth2(view_func):
    """
    Decorator enforcing restricted access.
    """

    @wraps(view_func)
    def decorated_view():
        _, decoded = auth.from_request(request)
        if decoded:
            if decoded['email'] in USERS:
                return view_func()
            else:
                return Response('Forbidden', status=403)
        else:
            return redirect(f'/auth/login?next={request.url}')

    return decorated_view


@app.route('/')
def root():
    """
    Just for dev server sanity check.
    """
    return 'hic sunt dracones'


@app.route('/init')
@require_oauth2
def init():
    """
    Sets up the necessary tables for convenience.
    """
    client.create_dataset(DATASET)
    client.query(CREATE_SESSIONS_QUERY)
    for t in DAILIES:
        client.query(CREATE_EVENTS_QUERY.format(for_query(t)))
    return 'OK'


@app.route('/demo_init')
@require_oauth2
def demo_init():
    """
    Sets up the necessary tables and adds some random rows.
    """
    init()
    demo.init_demo(client, DAILIES, TZ)
    return 'OK'


@app.route('/auth/login')
def login():
    url = auth.handle_login(request, session)
    return redirect(url)


@app.route('/auth/redirect')
def auth_redirect():
    """
    Endpoint for handling the oauth2 callback (as specified via
    https://console.cloud.google.com/apis/credentials)
    """
    try:
        next_path, id_token = auth.handle_redirect(request, session)
    except ValueError:
        return Response('Unauthorised', status=401)

    resp = make_response(redirect(next_path))
    resp.set_cookie(auth.ID_COOKIE, id_token)
    return resp


@app.route('/events', methods=['POST'])
@require_oauth2
def insert():
    """
    Inserts a new event into the current daily table.
    """
    row = request.json
    if type(row) == list:
        rows = row
    else:
        rows = [row]

    table = client.get_table(DAILIES[days_since_epoch() % 2])
    errors = client.insert_rows(table, rows)
    return 'OK' if not errors else str(errors)


@app.route('/events/aggregation', methods=['GET'])
@require_oauth2
def aggregate():
    """
    Handles daily aggregation flow.
    """
    # insert all closed sessions
    client.query(AGGREGATION_QUERY, job_id_prefix='aggregate-events-')

    # copy the rest to current event table
    current = DAILIES[days_since_epoch() % 2]
    client.query(LEFTOVER_QUERY.format(for_query(current), for_query(DATASET)),
                 job_id_prefix='leftover-events-')

    # truncate old table
    previous = DAILIES[(days_since_epoch() + 1) % 2]
    client.delete_table(previous)
    client.query(CREATE_EVENTS_QUERY.format(for_query(previous)), job_id_prefix='create-table')
    return 'OK'


if __name__ == '__main__':
    app.run(host='localhost', port=8080, debug=True)
