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

The implementation is pretty bare-bones.
"""
import hashlib
import logging
import os
import uuid
from datetime import datetime

import requests
from google.auth import jwt
from jwcrypto.jwk import JWKSet

import utils

PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT')

# Never store the application (client) secret in a repository
CLIENT_ID = os.getenv('APPENGINE_CLIENT_ID')
CLIENT_SECRET = os.getenv('APPENGINE_CLIENT_SECRET')

REDIRECT_URL = f'{utils.get_own_scheme_authority()}/auth/redirect'

SCOPE = '%20'.join(['openid', 'email', 'profile'])

# OpenID discovery service
DISC_DOC_URL = 'https://accounts.google.com/.well-known/openid-configuration'
DISC = requests.get(DISC_DOC_URL).json()

AUTHORIZATION_ENDPOINT = DISC.get('authorization_endpoint')
USER_INFO_ENDPOINT = DISC.get('userinfo_endpoint')
TOKEN_ENDPOINT = DISC.get('token_endpoint')
JWKS_URI_ENDPOINT = DISC.get('jwks_uri')
TOKEN_INFO_ENDPOINT = 'https://www.googleapis.com/oauth2/v3/tokeninfo'

AUTH_HEADERS = ['Authorization', 'HTTP_AUTHORIZATION']
ID_COOKIE = 'CID'
APPID_HEADER = 'X-Appengine-Inbound-Appid'
TASK_HEADER = 'X-AppEngine-QueueName'
CRON_HEADER = 'X-AppEngine-Cron'    # TODO: use

CACHED_CERTS = None
WARNED_FOR_FAKE_AUTH = False
FAKE_USER = 'test@example.com'


def create_auth_url(state):
    authorization_params = {'client_id': CLIENT_ID,
                            'response_type': 'code',
                            'scope': SCOPE,
                            'redirect_uri': REDIRECT_URL,
                            'state': state,
                            'nonce': uuid.uuid4().hex}
    as_params = '&'.join([f'{k}={v}' for k, v in authorization_params.items()])

    return f'{AUTHORIZATION_ENDPOINT}?{as_params}'


def create_token_params(code):
    token_params = {'code': code,
                    'client_id': CLIENT_ID,
                    'client_secret': CLIENT_SECRET,
                    'redirect_uri': REDIRECT_URL,
                    'grant_type': 'authorization_code'}

    return token_params


def _fetch_certs():
    """
    Fetches and caches certs
    :return: Google's OAuth2 server certs
    """
    global CACHED_CERTS
    if not CACHED_CERTS:
        resp = requests.get(JWKS_URI_ENDPOINT)
        key_set = JWKSet.from_json(resp.content)
        CACHED_CERTS = [k.export_to_pem() for k in key_set]
    return CACHED_CERTS


def verify_token(id_token):
    try:
        decoded = jwt.decode(id_token, certs=_fetch_certs())
    except ValueError as ex:
        logging.warning('error decoding jwt: %s' % ex)
        return None, None

    if decoded.get('iss') != DISC.get('issuer'):
        logging.warning('invalid iss: ' + decoded.get('iss'))
        return None, None
    if decoded.get('aud') != CLIENT_ID:
        logging.warning('invalid aud: ' + decoded.get('aud'))
        return None, None

    exp_seconds = int(decoded.get('exp'))
    if datetime.utcnow() > datetime.utcfromtimestamp(exp_seconds):
        logging.warning('token expired: %s' % datetime.utcfromtimestamp(exp_seconds))
        return None, None

    return id_token, decoded


def fetch_token(code):
    resp = requests.post(TOKEN_ENDPOINT, create_token_params(code))
    id_token = resp.json()['id_token']
    return verify_token(id_token)


def from_header(headers):
    for name in AUTH_HEADERS:
        if name in headers:
            # Expects an id token rather than an access token
            id_token = headers[name][len('Bearer '):]
            return verify_token(id_token)
    return None, None


def from_cookie(cookies):
    token = cookies.get(ID_COOKIE)
    if not token:
        return None, None
    return verify_token(token)


def get_trusted_header(headers):
    for h in [APPID_HEADER, TASK_HEADER]:
        if h in headers:
            return headers[h]


def from_request(req):
    if fake_auth():
        return {}, {'email': FAKE_USER}

    # Trust AppEngine headers
    app_or_task_header = get_trusted_header(req.headers)
    if app_or_task_header:
        return {}, {'sub': app_or_task_header}

    token, decoded = from_cookie(req.cookies)
    if not token:
        token, decoded = from_header(req.headers)
    return token, decoded


def fake_auth():
    if not utils.is_production_server() and not os.getenv('DEBUG_AUTH'):
        global WARNED_FOR_FAKE_AUTH
        if not WARNED_FOR_FAKE_AUTH:
            logging.warning('!!! Running in local mode with AUTHENTICATION DISABLED !!!!')
            WARNED_FOR_FAKE_AUTH = True
        return True
    else:
        return False


def handle_login(req, sess):
    if fake_auth():
        return req.args.get('next', '/')
    next_path = req.args.get('next', '/')
    state = '%s$%s' % (hashlib.sha256(os.urandom(1024)).hexdigest(), next_path)
    sess['state'] = state
    url = create_auth_url(state)
    return url


def handle_redirect(req, sess):
    code = req.args.get('code')
    state = req.args.get('state')

    if state != sess.get('state'):
        raise ValueError('bad state')

    id_token, _ = fetch_token(code)

    next_path = state.split('$')[1]
    return next_path, id_token
