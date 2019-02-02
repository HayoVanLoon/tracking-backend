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
from datetime import datetime, timezone


def days_since_epoch(dt: datetime = None, tz: timezone = timezone.utc) -> int:
    """
    Calculates the days since epoch (1970-01-01) for the given datetime.

    :param dt: non-naive datetime, default is 'now'
    :param tz: timezone, default is UTC
    :return: number of days since epoch
    """
    if not dt:
        dt = datetime.now(tz=tz)
    return (dt - datetime(1970, 1, 1, tzinfo=tz)).days


def get_own_scheme_authority(service: str = None):
    """
    Returns scheme and host for the current application instance

    :param service: service if not default app
    :return: the app's host address
    """
    service_prefix = '' if not service else service + '-dot-'
    if is_production_server():
        return 'https://' + service_prefix + os.getenv('GOOGLE_CLOUD_PROJECT') + '.appspot.com'
    else:
        return 'http://localhost:8080'


def is_production_server():
    """
    Whether current instance is running locally

    Taken from AppEngine documentation
    :return: true if the app is running in production
    """
    return os.getenv('GAE_ENV', '').startswith('standard')
