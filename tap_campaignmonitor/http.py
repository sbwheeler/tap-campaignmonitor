import base64
from datetime import datetime

import requests

import singer
import time

from .timeout import timeout

logger = singer.get_logger()


FULL_URI = 'https://api.createsend.com/api/v3.1/clients/{client_id}/{stream}.json'  # NOQA
ACTIVITY_URI = 'https://api.createsend.com/api/v3.1/campaigns/{campaign_id}/{stream}.json?{date}page={page}'  # NOQA


def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")


class Client(object):
    def __init__(self, config):
        self.user_agent = config.get("user_agent")
        self.api_key = config.get('api_key')
        self.client_id = config.get('client_id')
        self.session = requests.Session()

    def full_sync_url(self, stream, page=None):
        uri = FULL_URI.format(client_id=self.client_id,
                              stream=stream)
        if page:
            return uri + '?page={}'.format(page)
        else:
            return uri

    def activity_sync_url(self, campaign_id, stream, page, date):
        return ACTIVITY_URI.format(campaign_id=campaign_id,
                                   stream=stream,
                                   page=page,
                                   date='date={}&'.format(
                                       date) if date else '')

    @timeout(seconds=60)
    def create_get_request(self,
                           stream=None,
                           campaign_id=None,
                           page=None,
                           date=None):
        if stream == 'campaigns' or stream == 'suppressionlist':
            return requests.Request(method='GET', url=self.full_sync_url(stream, page=page))
        else:
            return requests.Request(method='GET',
                                    url=self.activity_sync_url(campaign_id,
                                                               stream,
                                                               page,
                                                               date
                                                               ))

    def prepare_and_send(self, request):
        if self.user_agent:
            request.headers['User-Agent'] = self.user_agent

        request.headers['Content-Type'] = 'application/json; charset=utf-8'
        request.headers['Accept-Encoding'] = 'gzip, deflate'
        request.headers['Authorization'] = 'Basic %s' % base64.b64encode(
            ('%s:x' % self.api_key).encode()).decode()

        return self.session.send(request.prepare())

    def GET(self, stream=None, campaign_id=None, page=1, date=None):
        req = self.create_get_request(stream=stream,
                                      campaign_id=campaign_id,
                                      page=page,
                                      date=date)
        return self.prepare_and_send(req)

    def retry_get(self, stream=None, campaign_id=None, page=1, date=None):
        """Wrap certain streams in a retry wrapper for frequent 500s"""
        retries = 5
        delay = 10
        backoff = 1.5
        attempt = 1
        while retries >= attempt:
            response = self.GET(stream=stream, campaign_id=campaign_id, page=page,
                                date=date)
            if response.status_code >= 500:
                logger.info(f'Got a status code of {response.status_code}, attempt '
                            f'{attempt} of {retries}. Backing off for {delay} '
                            f'seconds')
                time.sleep(delay)
                delay *= backoff
                attempt += 1
            else:
                return response
        url = self.activity_sync_url(stream=stream, campaign_id=campaign_id,
                                     page=page, date=date)
        logger.error(f'Status code of latest attempt: {response.status_code}')
        logger.error(f'Latest attempt response {response.content}')
        raise ValueError(f'Failed {retries} times trying to hit endpoint {url}')