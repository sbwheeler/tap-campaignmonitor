#!/usr/bin/env python3
import os
import singer
from singer import utils


class IDS(object):
    CAMPAIGNS = 'campaigns'
    RECIPIENTS = 'recipients'
    BOUNCES = 'bounces'
    OPENS = 'opens'
    CLICKS = 'clicks'
    UNSUBSCRIBES = 'unsubscribes'
    MARKED_AS_SPAM = 'marked_as_spam'


stream_ids = [getattr(IDS, x) for x in dir(IDS)
              if not x.startswith('__')]

PK_FIELDS = {
    IDS.CAMPAIGNS: ['CampaignID'],
    IDS.RECIPIENTS: ['EmailAddress'],
    IDS.BOUNCES: ['EmailAddress'],
    IDS.OPENS: ['EmailAddress'],
    IDS.CLICKS: ['EmailAddress'],
    IDS.UNSUBSCRIBES: ['EmailAddress'],
    IDS.MARKED_AS_SPAM: ['EmailAddress'],
}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    return utils.load_json(get_abs_path(path))


def load_and_write_schema(tap_stream_id):
    schema = load_schema(tap_stream_id)
    singer.write_schema(tap_stream_id, schema, PK_FIELDS[tap_stream_id])
