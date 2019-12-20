import os
import json
import requests
import logging
from .common import urls
from ptitnetwork.base_client import BaseHTTPClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class PinpointApiClient:
    BASE_URL = os.getenv('BASE_PINPOINT_URL')

    def __init__(self, client=None):
        self._client = client or BaseHTTPClient(self.BASE_URL)

    def get_ready_minion(self):
        return self._client.get(url=urls.MINION+'?status=READY')

    def update_bot(self, bot_id, data):
        resp = self._client.patch(url=urls.BOT + '/' + bot_id, data=data)
        print(resp.json())
        return resp

    def get_bots(self, minion_id):
        return self._client.get(url=urls.BOT + '?minion={0}'.format(minion_id))
