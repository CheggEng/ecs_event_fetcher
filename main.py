import boto3
import time
import logging
import datetime
import pytz
import sdb
import os


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
CLOUDWATCH_LOG_GROUP = os.getenv("CLOUDWATCH_LOG_GROUP", "ecs_service_events")
POLL_INTERVAL = os.getenv('POLL_INTERVAL', 60)
API_REQUEST_SPACING = os.getenv('API_REQUEST_SPACING', 0.2)
SDB_DOMAIN = os.getenv('SDB_DOMAIN', 'ecs_service_events')
ACCESS_KEY = os.getenv('ACCESS_KEY', None)
SECRET_KEY = os.getenv('SECRET_KEY', None)
REGION = os.getenv('REGION', None)

ecs = boto3.client('ecs', region_name=REGION, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

logs = boto3.client('logs', region_name=REGION, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

logger = logging.getLogger('ecs_event_fetcher')
logger.setLevel(LOG_LEVEL)
logger.propagate = False
stderr_logs = logging.StreamHandler()
stderr_logs.setLevel(getattr(logging, LOG_LEVEL))
stderr_logs.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(stderr_logs)


def create_event_watchers():
    event_watchers = {}
    for cluster in ecs_cluster_enumerator():
        for service in ecs_service_enumerator(cluster):
            event_watchers[cluster+"_"+service] = EcsEventWatcher(cluster, service)
    return event_watchers


def ecs_cluster_enumerator():
    response = ecs.list_clusters()
    clusterArns = response['clusterArns']
    for cluster in clusterArns:
        yield cluster.split('/')[1]


def ecs_service_enumerator(cluster):
    paginate = True
    next_token = ''
    while paginate:
        response = ecs.list_services(
            cluster=cluster,
            nextToken=next_token
        )
        serviceArns = response['serviceArns']
        try:
            next_token = response['nextToken']
        except KeyError:
            next_token = ''
            paginate = False
        for service in serviceArns:
            yield service.split('/')[1]


class EcsEventWatcher(object):
    def __init__(self, cluster_name, service_name):
        self.log_group = CLOUDWATCH_LOG_GROUP
        self.service_name = service_name
        self.cluster = cluster_name
        self.log_stream = self.cluster + "_" + service_name
        self.sequence_token = None
        self.first_flushed_event = None
        self.last_sent_event_id = str
        self.sdb_key_name = cluster_name + "_" + service_name

        # Check if stream exists already
        stream = logs.describe_log_streams(
            logGroupName=CLOUDWATCH_LOG_GROUP,
            logStreamNamePrefix=self.log_stream,
        )

        # If not create the stream
        if len(stream['logStreams']) == 0:
            logs.create_log_stream(
                logGroupName=CLOUDWATCH_LOG_GROUP,
                logStreamName=self.log_stream
            )
            stream = logs.describe_log_streams(
            logGroupName=CLOUDWATCH_LOG_GROUP,
            logStreamNamePrefix=self.log_stream,
        )

        # Try to capture sequence token from describe_log_streams response
        # TODO figure out if we need to get sequence token when we first create the stream
        try:
            if len(stream['logStreams']) > 1:  # We only expect to have one stream by this name
                logger.warn("Found more than one log stream in DescribeLogStreams call. {}".format(stream))
                logger.debug(stream)
            self.sequence_token = stream['logStreams'][0]['uploadSequenceToken']
        except KeyError:
            self.sequence_token = None

        # Check SDB for flushed event pointer
        sdb_results = sdb.get(self.sdb_key_name, SDB_DOMAIN)
        if sdb_results:
            logger.debug("Found state in SDB for stream {}".format(self.log_stream))
            self.first_flushed_event = sdb_results['first_flushed_event']

    def write_to_cloudwatch(self, event):
        # TODO batch these requests in chunks of 1,048,576 bytes or less
        logger.info('Writing to cloudwatch for log stream {}. Event ID {}'.format(self.log_stream, event['id']))
        event = {
            'timestamp': int((
                                 event['createdAt'] - datetime.datetime(
                                     1970, 1, 1, tzinfo=pytz.timezone('US/Pacific'))
                             ).total_seconds()) * 1000,  # must be milliseconds since epoch
            'message': event['message']
        }
        if self.sequence_token:
            response = logs.put_log_events(logGroupName=self.log_group,
                                           logStreamName=self.log_stream,
                                           logEvents=[event],
                                           sequenceToken=self.sequence_token
                                           )
        else:
            response = logs.put_log_events(logGroupName=self.log_group,
                                           logStreamName=self.log_stream,
                                           logEvents=[event]
                                           )
        logger.debug(response)
        self.sequence_token = response['nextSequenceToken']

    def persist_events(self, events):
        for event in events:
            time.sleep(API_REQUEST_SPACING)
            logger.debug(event)
            self.write_to_cloudwatch(event)
        self.first_flushed_event = [x['id'] for x in events][0]

    def process(self):
        new_events = ecs.describe_services(services=[self.service_name], cluster=self.cluster)['services'][0]['events']
        if not self.first_flushed_event:
            self.persist_events(new_events[:])
        # find where the segment of the stream we've already sent begins in the new events stream
        try:
            top_of_stack = [x['id'] for x in new_events].index(self.first_flushed_event)
            if top_of_stack > 0:
                # slice off the events before the ones we already sent
                logger.info('Found new events in stream')
                self.persist_events(new_events[:top_of_stack])
            else:
                # the index of the first_flushed event is 0 therefore there are no new items before it to flush
                logger.debug('No new events found')
        except ValueError:
            # we didn't find the beginning of the stream meaning it has moved faster than our polling interval
            logger.warn('Was not able to find last event in our stream, possible events are being missed!')
            self.persist_events(new_events[:])

        # write the flush pointer to SDB so we have it for next time we run
        sdb.put(self.sdb_key_name, {"first_flushed_event": self.first_flushed_event}, SDB_DOMAIN, replace=True)


if __name__ == '__main__':
    logger.info("Creating event watchers...")
    event_watchers = create_event_watchers()
    while True:
        for k, v in event_watchers.iteritems():
            time.sleep(API_REQUEST_SPACING)
            logger.info("Processing service {}".format(k))
            try:
                v.process()
            except Exception as e:
                logger.error(e)
        time.sleep(POLL_INTERVAL)