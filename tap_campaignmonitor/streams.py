import json
from collections import defaultdict
from datetime import datetime

import singer

from .schemas import IDS

logger = singer.get_logger()


def metrics(tap_stream_id, records):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))


def write_records(tap_stream_id, records):
    singer.write_records(tap_stream_id, records)
    metrics(tap_stream_id, records)


class BOOK(object):
    CAMPAIGNS = [IDS.CAMPAIGNS]
    RECIPIENTS = [IDS.RECIPIENTS]
    SUPPRESSIONLIST = [IDS.SUPPRESSIONLIST]
    BOUNCES = [IDS.BOUNCES, 'Date']
    OPENS = [IDS.OPENS, 'Date']
    CLICKS = [IDS.CLICKS, 'Date']
    UNSUBSCRIBES = [IDS.UNSUBSCRIBES, 'Date']
    SPAM = [IDS.SPAM, 'Date']

    @classmethod
    def return_bookmark_path(cls, stream):
        return getattr(cls, stream.upper())

    @classmethod
    def get_incremental_syncs(cls):
        syncs = []
        for k, v in cls.__dict__.items():
            if not k.startswith("__") and not isinstance(v, classmethod):
                if len(v) > 1:
                    syncs.append(k)

        return syncs

    @classmethod
    def get_full_syncs(cls):
        syncs = []
        for k, v in cls.__dict__.items():
            if not k.startswith("__") and not isinstance(v, classmethod):
                if len(v) == 1:
                    syncs.append(k)

        return syncs


def sync(context):
    # do full syncs first as they are used later
    for stream in context.selected_stream_ids:
        if stream.upper() in BOOK.get_full_syncs():
            call_stream_full(context, stream)

    for stream in context.selected_stream_ids:
        if stream.upper() in BOOK.get_incremental_syncs():
            bk = call_stream_incremental(context, stream)
            save_state(context, stream, bk)


def call_stream_full(context, stream):
    if stream == 'campaigns':
        response = context.client.GET(stream='campaigns')
        records_to_write = json.loads(response.content)
        write_records(stream, records_to_write)

        context.save_campaigns_meta(records_to_write)
    elif stream == 'suppressionlist':
        run_suppression_request(context)
    else:
        call_recipients_stream(context, stream)


def call_recipients_stream(context, stream):
    # just campaigns for now
    stream_resource = 'campaigns'

    for campaign in context.campaigns:
        logger.info('{ts} - querying {stream} id: {id}'.format(
            ts=datetime.now(),
            stream=stream_resource,
            id=campaign['id'],
        ))

        run_campaign_activity_request(context, campaign['id'], stream)


def call_stream_incremental(context, stream):
    last_updated = context.get_bookmark(BOOK.return_bookmark_path(stream)) or \
        defaultdict(str)

    # just campaigns for now
    stream_resource = 'campaigns'

    for campaign in context.campaigns:
        context.update_latest(campaign['id'], last_updated)

        logger.info('{ts} - querying {stream} id: {id}, since: {since}'.format(
            ts=datetime.now(),
            stream=stream_resource,
            id=campaign['id'],
            since=last_updated[campaign['id']],
        ))

        run_campaign_activity_request(
            context, campaign['id'], stream, last_updated)

        context.set_bookmark_and_write_state(
            BOOK.return_bookmark_path(stream),
            last_updated)

    return last_updated


def run_suppression_request(context):
    current_page = 1
    total_pages = 2

    while current_page <= total_pages:

        response = context.client.GET(stream='suppressionlist',
                                      page=current_page)
        data = json.loads(response.content)
        if current_page == 1:
            logger.info(
                '{ts} querying suppresion list now - will retrieve '
                '{total} total records'.format(
                    ts=datetime.now(),
                    total=data['TotalNumberOfRecords']))

        total_pages = data['NumberOfPages']
        current_page = data['PageNumber'] + 1
        write_records('suppressionlist', data['Results'])


def run_campaign_activity_request(context,
                                  campaign_id,
                                  stream=None,
                                  last_updated=None):
    """
    Method for properly requesting, paginating, and saving campaign activity
    data from Campaign Monitor.
    args:
        context: the Context object from context.py
        campaign_id: the campaign ID string
        last_updated(dict): dict of {campaign_id: last_updated_datestring}
            with datestring of format 2017-01-01T00:00:00+00:00
    """
    current_page = 1
    total_pages = 2
    request_date = None
    if last_updated:
        request_date = get_date_string_from_last_updated(
            last_updated[campaign_id])

    while current_page <= total_pages:
        response = context.client.GET(stream=stream,
                                      campaign_id=campaign_id,
                                      page=current_page,
                                      date=request_date)
        logger.info('Response: {r}'.format(r=response))
        data = json.loads(response.content)

        if current_page == 1:
            logger.info(
                '{ts} querying campaign {campaign_id} now - will retrieve '
                '{total} total records'.format(
                    ts=datetime.now(),
                    campaign_id=campaign_id,
                    total=data['TotalNumberOfRecords']))

        total_pages = data['NumberOfPages']
        current_page = data['PageNumber'] + 1

        records = join_campaign_id(data['Results'], campaign_id)

        if last_updated:
            (stop_paginating, records_to_save) = filter_new_records(
                records, last_updated[campaign_id])
            write_records_and_update_state(
                campaign_id, stream, records_to_save, last_updated)
            if stop_paginating:
                break
        else:
            write_records(stream, records)


def filter_new_records(records, last_updated_datestring):
    """
    We are receiving events from the API by latest date descending, so we can
    loop through and then chop off & ignore  any events that are before our
    last updated date. We first check if the min (last) date is still greater
    than last_updated to be somewhat smart about things.
    Returns:
        stop_paginating (bool): a boolean to indicate to the calling
            function whether there are more records we need
        records_to_save (list): the list of records after last_updated that we
            can save.
    """
    if not len(records):
        return True, records

    last_updated_date = get_date_from_last_updated(last_updated_datestring)
    if get_date_from_record_string(records[-1]['Date']) > last_updated_date:
        return False, records

    for idx, r in enumerate(records):
        if get_date_from_record_string(r['Date']) <= last_updated_date:
            return True, records[:idx]


def write_records_and_update_state(campaign_id, stream,
                                   batched_records, last_updated):
    write_records(stream, batched_records)
    last_updated[campaign_id] = get_latest_record_timestamp(
        batched_records,
        last_updated[campaign_id],
        BOOK.return_bookmark_path(stream)[1]
    )


def get_latest_record_timestamp(records, last_updated_date, time_key):
    """
    Must return properly formatted date to write to state/
    """
    if records:
        return_date = max(max([r[time_key] for r in records]),
                          last_updated_date)
    else:
        return_date = last_updated_date

    date_to_return = return_date.replace(' ', 'T')
    if '+' in date_to_return:
        return date_to_return
    else:
        return date_to_return + '+00:00'


def join_campaign_id(data, campaign_id):
    """
    Join the campaign ID to each record so these can be joined to campaign
    information in SQL.
    """
    for record in data:
        record['campaign_id'] = campaign_id

    return data


def save_state(context, stream, bk):
    context.set_bookmark(BOOK.return_bookmark_path(stream), bk)
    context.write_state()

def get_date_string_from_last_updated(datestring):
    """
    Takes in datestring of format 2017-01-01T00:00:00+00:00 (from singer),
    returns datestring of format 2017-01-01 00:00 (for campaign monitor API)
    """
    return datestring[:-9].replace('T', ' ')


def get_date_from_last_updated(datestring):
    return datetime.strptime(datestring[:-6], '%Y-%m-%dT%H:%M:%S')


def get_date_from_record_string(datestring):
    return datetime.strptime(datestring, '%Y-%m-%d %H:%M:%S')
