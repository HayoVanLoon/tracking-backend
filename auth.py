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
Implements helpers for the server flow described in:
https://developers.google.com/identity/protocols/OpenIDConnect

Work in progress
"""
import os
from datetime import datetime

import requests

PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT')

# Never store the application (client) secret in a repository
CLIENT_ID = os.getenv('APPENGINE_CLIENT_ID')
CLIENT_SECRET = os.getenv('APPENGINE_CLIENT_SECRET')

# Assumes the app is the default service, accessed via appspot.
if os.getenv('GAE_ENV', '').startswith('standard'):
    HOST = PROJECT + '.appspot.com'
else:
    HOST = 'localhost:8080'

REDIRECT_URL = f'http://{HOST}/auth/redirect'

SCOPE = '%20'.join(['openid', 'email', 'profile'])

# OpenID discovery service
DISC_DOC_URL = 'https://accounts.google.com/.well-known/openid-configuration'
DISC = requests.get(DISC_DOC_URL).json()

AUTHORIZATION_ENDPOINT = DISC.get('authorization_endpoint')
USER_INFO_ENDPOINT = DISC.get('userinfo_endpoint')
TOKEN_ENDPOINT = DISC.get('token_endpoint')
TOKEN_INFO_ENDPOINT = 'https://www.googleapis.com/oauth2/v3/tokeninfo'


def create_auth_url(state):
    authorization_params = {'client_id': CLIENT_ID,
                            'response_type': 'code',
                            'scope': SCOPE,
                            'redirect_uri': REDIRECT_URL,
                            'state': state,
                            'nonce': 'TODO'}
    as_params = '&'.join([f'{k}={v}' for k, v in authorization_params.items()])

    return f'{AUTHORIZATION_ENDPOINT}?{as_params}'


def create_token_params(code):
    token_params = {'code': code,
                    'client_id': CLIENT_ID,
                    'client_secret': CLIENT_SECRET,
                    'redirect_uri': REDIRECT_URL,
                    'grant_type': 'authorization_code'}

    return token_params


def verify_token(id_token):
    # TODO: replace verification call to Google by library
    decoded = requests.post(TOKEN_INFO_ENDPOINT, {'id_token': id_token}).json()
    if decoded.get('iss') != DISC.get('issuer'):
        return None, None
    if decoded.get('aud') != CLIENT_ID:
        return None, None

    exp_seconds = int(decoded.get('exp'))
    if datetime.utcnow() > datetime.utcfromtimestamp(exp_seconds):
        return None, None
    return id_token, decoded


def verify(resp_body):
    access_token = resp_body.get('access_token')
    id_token = resp_body.get('id_token')
    expires_in = int(resp_body.get('expires_in'))
    token_type = resp_body.get('token_type')

    return verify_token(id_token)


def from_auth_header(req):
    for name in ['Authorisation', 'Authorisation'.lower()]:
        if name in req.headers:
            value = req.headers[name]
            token = value[len('Bearer '):]
            # TODO: validate token
            return token
    return None
