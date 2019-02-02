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
"""
A script to illustrate / test using ID token credentials generated from the
standard service account credential json file.
"""

import json
import os

from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import IDTokenCredentials


APP_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
APPENGINE_CLIENT_ID = os.getenv('APPENGINE_CLIENT_ID')

credentials = IDTokenCredentials.from_service_account_file(APP_CREDENTIALS,
                                                           target_audience=APPENGINE_CLIENT_ID)

authed_session = AuthorizedSession(credentials)

with open('example.json', 'r') as f:
    payload = json.loads(f.read())

resp = authed_session.post('http://localhost:8080/events', json=payload)

print(resp, resp.content)
