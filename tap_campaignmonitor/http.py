import backoff
import requests

import singer
from createsend import Client as CampaignMonitorClient
from createsend import Campaign

logger = singer.get_logger()


class Client(object):
    def __init__(self, config):
        self.api_key = config.get('api_key')
        self.client_id = config.get('client_id')
        self.session = requests.Session()
        self.campaign_client = self._get_campaign_client()

    def create_get_request(self, path, activity, date):
        return requests.Request(method='GET', url=self.url(path, activity,
                                                           date))

    def GET(self, activity=None, campaign_id=None, date=None, page=1):
        if not activity:
            return self.campaign_client.campaigns()
        else:
            return self._get_campaign_activity(activity,
                                               campaign_id,
                                               date,
                                               page)

    def _get_campaign_client(self):
        campaign_client = CampaignMonitorClient({'api_key': self.api_key},
                                                self.client_id)

        return campaign_client

    def _get_campaign_activity(self, activity, campaign_id, date, page):
        campaign_activity_client = Campaign(
            {'api_key': self.api_key}, campaign_id=campaign_id)
        activity_clients = {
            'recipients': campaign_activity_client.recipients,
            'clicks': campaign_activity_client.clicks,
            'bounces': campaign_activity_client.bounces,
            'opens': campaign_activity_client.opens,
            'marked_as_spam': campaign_activity_client.spam,
            'unsubscribes': campaign_activity_client.unsubscribes,
        }
        if activity == 'recipients':
            return activity_clients[activity](page=page)
        else:
            # get latest records first
            return activity_clients[activity](date=date,
                                              page=page,
                                              order_direction='asc')
