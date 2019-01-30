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
import hashlib
import os
import random
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Union

import requests
from flask import Flask, redirect, request, session
from google.cloud import bigquery as bq

import auth

PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT')

app = Flask(__name__)
app.secret_key = auth.CLIENT_SECRET

client = bq.Client()

DATASET = bq.DatasetReference(PROJECT, 'bobs_knob_shops')
DAILIES = [bq.TableReference(DATASET, 'events_0'),
           bq.TableReference(DATASET, 'events_1')]
SESSIONS = bq.TableReference(DATASET, 'sessions')

TZ = timezone.utc


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
  channel_id, visitor_id, session_id, timezone_offset, start_time, end_time, hit_count, hits
)
SELECT
  channel_id,
  visitor_id,
  session_id,
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
    MAX(timestamp) OVER (PARTITION BY session_id) last_timestamp
  FROM
    `{}.events_*` )
WHERE
  last_timestamp <= TIMESTAMP(CURRENT_DATE())
GROUP BY
  channel_id,
  visitor_id,
  session_id
""".format(for_query(SESSIONS), for_query(DATASET))

# noinspection SqlNoDataSourceInspection,SqlDialectInspection
LEFTOVER_QUERY = """
INSERT INTO `{}` (
  channel_id, visitor_id, session_id, timestamp, timezone_offset,
  url, referrer_url
)
SELECT
  channel_id,
  visitor_id,
  session_id,
  timestamp,
  timezone_offset,
  url,
  referrer_url
FROM (
  SELECT
    *,
    MIN(timestamp) OVER (PARTITION BY session_id) first_timestamp,
    MAX(timestamp) OVER (PARTITION BY session_id) last_timestamp
  FROM
    `{}.events_*` )
WHERE
  first_timestamp < TIMESTAMP(CURRENT_DATE())
  AND last_timestamp >= TIMESTAMP(CURRENT_DATE())
"""

# noinspection SqlNoDataSourceInspection,SqlDialectInspection
CREATE_SESSIONS_QUERY = """
CREATE TABLE `{}` (
  channel_id STRING,
  visitor_id STRING,
  session_id STRING,
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
  session_id STRING,
  timestamp TIMESTAMP,
  timezone_offset INT64,
  url STRING,
  referrer_url STRING
)
"""


def days_since_epoch(dt: datetime = None):
    if not dt:
        dt = datetime.now(tz=TZ)
    return (dt - datetime(1970, 1, 1, tzinfo=TZ)).days


def custom_oauth2(view_func):
    @wraps(view_func)
    def decorated_view():
        if 'token' in session:
            token, _ = auth.verify_token(session['token'])
        else:
            token = auth.from_auth_header(request)

        if not token:
            return redirect('/auth/login?next=/')
        return view_func()

    return decorated_view


@app.route('/')
def root():
    """
    Just for dev server sanity check.
    """
    return 'hic sunt dracones'


@app.route('/init')
def init():
    """
    Sets up the necessary tables. Intended as convenience for demo.
    """
    client.create_dataset(DATASET)
    client.query(CREATE_SESSIONS_QUERY)
    for t in DAILIES:
        client.query(CREATE_EVENTS_QUERY.format(for_query(t)))
    return 'OK'


@app.route('/demo_init')
def demo_init():
    """
    Sets up the necessary tables and adds some random rows.
    """

    def random_insert(start, length, prefix='visitor'):
        visitor_id = prefix + '-' + str(random.randint(0, 9999))
        session_id = visitor_id + str(random.randint(0, 30))

        def create_row(time):
            return {
                'channel_id': 'bobs-door-knobs',
                'visitor_id': visitor_id,
                'session_id': session_id,
                'timestamp': time,
                'timezone_offset': -60,
                'url': f'https://door.knobsfrombobs.za/product/p{random.randint(10, 300)}',
                'referrer_url': 'https://door.knobsfrombobs.za/lister/cellar'
            }

        rows = {}
        for k in range(0, length):
            ts = start.timestamp() + k * 1800
            day = days_since_epoch(datetime.fromtimestamp(ts, tz=TZ)) % 2
            xs = rows.get(day, [])
            xs.append(create_row(ts))
            rows[day] = xs

        for day in rows:
            table_ref = client.get_table(DAILIES[day])
            client.insert_rows(table_ref, rows[day])

    init()

    d = datetime.now(tz=TZ).date()
    d1 = d - timedelta(days=1)

    for i in range(0, 4):
        random_insert(datetime(d1.year, d1.month, d1.day, 12, 0, i, tzinfo=TZ),
                      5 + random.randint(0, 12))
    random_insert(datetime(d1.year, d1.month, d1.day, 22, 0, tzinfo=TZ), 3, 'previous day')
    random_insert(datetime(d1.year, d1.month, d1.day, 23, 0, tzinfo=TZ), 4, 'not closed')
    for i in range(0, 4):
        random_insert(datetime(d.year, d.month, d.day, 12, 0, i, tzinfo=TZ),
                      5 + random.randint(0, 12))

    return 'OK'


@app.route('/events', methods=['POST'])
def insert():
    """
    Inserts a new event into the current daily table.
    """
    row = request.json
    try:
        rows = [r for r in row]
    except TypeError:
        rows = [row]

    t = client.get_table(DAILIES[days_since_epoch() % 2])
    client.insert_rows(t, rows)
    return 'OK'


@app.route('/events/aggregation', methods=['GET'])
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


@app.route('/test')
@custom_oauth2
def root():
    """
    OAth2 test implementation endpoint
    """
    return 'hic sunt dracones'


@app.route('/auth/login')
def login():
    next_path = request.args.get('next', '/')
    state = '%s$%s' % (hashlib.sha256(os.urandom(1024)).hexdigest(), next_path)
    session['state'] = state
    url = auth.create_auth_url(state)
    return redirect(url)


@app.route('/auth/redirect')
def auth_redirect():
    code = request.args.get('code')
    state = request.args.get('state')

    if state != session.get('state'):
        raise Exception('bad state')

    token_resp = requests.post(auth.TOKEN_ENDPOINT, auth.create_token_params(code))
    id_token, decoded = auth.verify(token_resp.json())

    session['token'] = id_token
    next_path = session['state'].split('$')[1]
    return redirect(next_path)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
