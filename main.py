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
import subprocess
from datetime import datetime

from flask import Flask, request
from google.cloud import bigquery as bq

app = Flask(__name__)
client = bq.Client()

# TODO: include switch or make it a simple string again
PROJECT = subprocess.run(['gcloud', 'config', 'get-value', 'project'],
                         stdout=subprocess.PIPE).stdout.decode('utf-8').replace('\n', '')

DATASET = bq.dataset.DatasetReference(PROJECT, 'bobs_knob_shop')
DAILIES = [bq.TableReference(DATASET, 'events_0'),
           bq.TableReference(DATASET, 'events_1')]
SESSIONS = bq.TableReference(DATASET, 'sessions')

# noinspection SqlNoDataSourceInspection,SqlDialectInspection
AGGREGATION_QUERY = """
INSERT INTO `{}.sessions` (
  channel_id, visitor_id, session_id, start_time,
  end_time, hit_count, hits
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
  )) hits
FROM (
  SELECT
    *,
    MAX(timestamp) OVER (PARTITION BY session_id) last_timestamp
  FROM
    `bobs_knob_shops.events_*` )
WHERE
  last_timestamp <= TIMESTAMP(CURRENT_DATE())
GROUP BY
  channel_id,
  visitor_id,
  session_id
""".format(DATASET.dataset_id)

# noinspection SqlNoDataSourceInspection,SqlDialectInspection
LEFTOVER_QUERY = """
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
""".format(DATASET.dataset_id)

# noinspection SqlNoDataSourceInspection,SqlDialectInspection
CREATE_SESSIONS_QUERY = """
CREATE TABLE `{}.sessions` (
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
""".format(DATASET.dataset_id)

# noinspection SqlNoDataSourceInspection,SqlDialectInspection
CREATE_EVENTS_QUERY = """
CREATE TABLE `{}.{}` ( 
  channel_id STRING,
  visitor_id STRING,
  session_id STRING,
  timestamp TIMESTAMP,
  timezone_offset INT64,
  url STRING,
  referrer_url STRING
)
"""


def days_since_epoch():
    return (datetime.utcnow() - datetime.utcfromtimestamp(0)).days


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
        client.query(CREATE_EVENTS_QUERY.format(t.dataset_id, t.table_id))
    return 'OK'


@app.route('/events', methods=['POST'])
def insert():
    """
    Inserts a new event into the current daily table.
    """
    row = request.json
    t = client.get_table(DAILIES[days_since_epoch() % 2])
    client.insert_rows(t, row)
    return 'OK'


@app.route('/events/aggregation', methods=['GET'])
def aggregate():
    """
    Handles daily aggregation flow.
    """
    # insert all closed sessions
    client.query(AGGREGATION_QUERY)

    # truncate old table
    t = DAILIES[(days_since_epoch() + 1) % 2]
    client.delete_table(t)
    client.query(CREATE_EVENTS_QUERY.format(t.dataset_id, t.table_id))
    return 'OK'


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
