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
import random
from datetime import datetime, timedelta

from utils import days_since_epoch


def init_demo(client, tables, tz):
    """
    Sets up the necessary tables and adds some random rows.

    :param client: BigQuery client
    :param tables: list of references to daily tables
    :param tz: timezone
    :return:
    """

    def random_insert(start, length, prefix='visitor'):
        visitor_id = prefix + '-' + str(random.randint(0, 9999))

        def create_row(time):
            return {
                'channel_id': 'bobs-door-knobs',
                'visitor_id': visitor_id,
                'timestamp': time,
                'timezone_offset': -60,
                'url': f'https://door.knobsfrombobs.za/product/p{random.randint(10, 300)}',
                'referrer_url': 'https://door.knobsfrombobs.za/lister/cellar'
            }

        rows = {}
        for k in range(0, length):
            ts = start.timestamp() + k * 1800
            day = days_since_epoch(datetime.fromtimestamp(ts, tz=tz)) % 2
            xs = rows.get(day, [])
            xs.append(create_row(ts))
            rows[day] = xs

        for day in rows:
            table_ref = client.get_table(tables[day])
            client.insert_rows(table_ref, rows[day])

    d = datetime.now(tz=tz).date()
    d1 = d - timedelta(days=1)

    for i in range(0, 4):
        random_insert(datetime(d1.year, d1.month, d1.day, 12, 0, i, tzinfo=tz),
                      5 + random.randint(0, 12))
    random_insert(datetime(d1.year, d1.month, d1.day, 22, 0, tzinfo=tz), 3, 'previous day')
    random_insert(datetime(d1.year, d1.month, d1.day, 23, 0, tzinfo=tz), 4, 'not closed')
    for i in range(0, 4):
        random_insert(datetime(d.year, d.month, d.day, 12, 0, i, tzinfo=tz),
                      5 + random.randint(0, 12))

    return 'OK'
