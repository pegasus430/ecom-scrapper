import os
import six
import json
import boto3
import logging

from uuid import uuid4
from cStringIO import StringIO

from twisted.internet import threads, task, reactor

from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import NotConfigured
from scrapy.utils.project import get_project_settings

from content_analytics import signals
from content_analytics.utils import aws_from_settings
from content_analytics.messages import MessageResolverMixin, BaseInputMessage


class Runner(CrawlerProcess, MessageResolverMixin):
    DEFAULT_MAX_TASKS = 10
    DEFAULT_VISIBILITY_TIMEOUT = 300
    DEFAULT_VISIBILITY_TIMEOUT_OFFSET = 10
    SPIDER_NAME_FORMAT = '{}_products'

    logger = logging.getLogger(__name__)

    input_queue = None
    input_queue_name = None
    input_queue_timeout = None
    input_queue_timeout_offset = None
    input_queue_resource = None

    output_queue = None
    output_queue_name = None
    output_queue_resource = None

    max_tasks = None
    grace_period = None
    in_progress_messages = []
    finished_messages = []
    visibility_update_pool = {}

    def __init__(self, settings=None):
        super(Runner, self).__init__(settings or get_project_settings())
        self.git_branch = os.environ.get('SCRAPERS_GIT_BRANCH')
        self.max_tasks = self.settings.getint('RUNNER_MAX_TASKS', self.DEFAULT_MAX_TASKS)
        self.logger.debug('Runner will process {} maximum tasks'.format(self.max_tasks))

        if self.settings.get('RUNNER_SETTINGS_BUCKET_ENABLED', None):
            self.set_proxy_settings_from_bucket()
        if self.settings.get('RUNNER_CACHE_SETTINGS_BUCKET_ENABLED', None):
            self.set_cache_settings_from_bucket()
        self.setup_input_queue()
        self.setup_output_queue()
        self.check_output_bucket()

        self.process_input_queue()
        self.start(stop_after_crawl=False)

    def setup_input_queue(self):
        self.logger.info('Setting up input SQS queue')

        # Setting up input SQS queue resource
        input_queue_settings = aws_from_settings(self.settings, prefix='INPUT_QUEUE_')
        if input_queue_settings is None or not all(input_queue_settings.values()):
            raise NotConfigured('AWS region, key and secret are required for input SQS queue!')
        self.input_queue_resource = boto3.resource('sqs', **input_queue_settings)

        # Setting up input SQS queue
        self.input_queue_name = self.settings.get('INPUT_QUEUE_NAME', None)
        if not self.input_queue_name:
            raise NotConfigured('SQS input queue name must be set!')
        self.logger.debug('Input SQS queue name is {}'.format(self.input_queue_name))
        self.input_queue = self.input_queue_resource.get_queue_by_name(QueueName=self.input_queue_name)

        # Setting up input SQS queue visibility timeout
        self.input_queue_timeout = int(self.input_queue.attributes.get(
            'VisibilityTimeout',
            self.DEFAULT_VISIBILITY_TIMEOUT
        ))
        self.logger.debug('Input SQS queue visibility timeout is {} seconds'.format(self.input_queue_timeout))

        # Setting up input SQS queue visibility timeout offset
        self.input_queue_timeout_offset = self.settings.getint(
            'INPUT_QUEUE_VISIBILITY_TIMEOUT_OFFSET',
            self.DEFAULT_VISIBILITY_TIMEOUT_OFFSET
        )
        self.logger.debug(
            'Input SQS queue visibility timeout offset is {} seconds'.format(self.input_queue_timeout_offset)
        )

    def setup_output_queue(self):
        self.logger.info('Setting up output SQS queue')

        # Setting up output SQS queue resource
        output_queue_settings = aws_from_settings(self.settings, prefix='OUTPUT_QUEUE_')
        if output_queue_settings is None or not all(output_queue_settings.values()):
            output_queue_settings = aws_from_settings(self.settings, prefix='INPUT_QUEUE_')
            self.logger.info('AWS region, key and secret for output SQS queue will be used same as input SQS queue')
        self.output_queue_resource = boto3.resource('sqs', **output_queue_settings)

        # Setting up output SQS queue
        self.output_queue_name = self.settings.get('OUTPUT_QUEUE_NAME', None)
        if self.output_queue_name:
            self.output_queue = self.output_queue_resource.get_queue_by_name(QueueName=self.output_queue_name)
            self.logger.debug('Output SQS queue name is {}'.format(self.output_queue_name))
        else:
            self.logger.debug(
                'Output SQS queue name is not provided. '
                'Will be used output SQS queue name from input message'
            )

    def check_output_bucket(self):
        self.logger.info('Checking output S3 bucket')

        # Checking output S3 bucket resource
        output_bucket_settings = aws_from_settings(self.settings, prefix='OUTPUT_BUCKET_')
        if output_bucket_settings is None or not all(output_bucket_settings.values()):
            self.logger.info('AWS region, key and secret for output S3 bucket will be used same as input queue')

        # Checking output S3 bucket
        output_bucket_name = self.settings.get('OUTPUT_BUCKET_NAME', None)
        if not output_bucket_name:
            raise NotConfigured('Output S3 bucket name and key must be set!')
        self.logger.debug('Output S3 bucket name is {}'.format(output_bucket_name))

    def get_bucket_resource(self):
        # Setting up S3 bucket resource
        bucket_settings = aws_from_settings(self.settings, prefix='SETTINGS_BUCKET_')
        if bucket_settings is None or not all(bucket_settings.values()):
            bucket_settings = aws_from_settings(self.settings, prefix='INPUT_QUEUE_')
            self.logger.info('AWS region, key and secret for settings S3 bucket will be used same as input queue')
        return boto3.resource('s3', **bucket_settings)

    def set_proxy_settings_from_bucket(self):
        self.logger.info('Getting proxy settings from S3 bucket')
        bucket_resource = self.get_bucket_resource()

        # Setting up S3 bucket
        bucket_name = self.settings.get('SETTINGS_BUCKET_NAME', None)
        if not bucket_name:
            raise NotConfigured('Settings S3 bucket name must be set!')

        # Different bucket keys which purpose is to not break production while development
        if self.git_branch == 'production':
            proxy_bucket_key = self.settings.get('PROXY_SETTINGS_PRODUCTION_BUCKET_KEY')
        else:
            proxy_bucket_key = self.settings.get('PROXY_SETTINGS_DEVELOPMENT_BUCKET_KEY')
        if not proxy_bucket_key:
            raise NotConfigured('Settings S3 bucket key for proxy must be set!')
        self.logger.debug('Settings S3 bucket key for proxy is {}'.format(proxy_bucket_key))

        try:
            proxies = self.get_settings(bucket_resource, bucket_name, proxy_bucket_key)
        except:
            raise NotConfigured("Couldn't fetch proxy settings from S3 bucket {}".format(bucket_name))

        self.settings.set('proxies', proxies)
        self.logger.debug('Proxy settings are updated from S3 bucket')

    def set_cache_settings_from_bucket(self):
        self.logger.info('Getting cache settings from S3 bucket')
        bucket_resource = self.get_bucket_resource()

        cache_bucket_name = self.settings.get('CACHE_BUCKET_NAME', None)
        if not cache_bucket_name:
            raise NotConfigured('Cache settings S3 bucket name must be set!')

        cache_bucket_key = self.settings.get('CACHE_SETTINGS_BUCKET_KEY')
        if not cache_bucket_key:
            raise NotConfigured('Settings S3 bucket key for cache must be set')

        try:
            cache = self.get_settings(bucket_resource, cache_bucket_name, cache_bucket_key)
        except:
            raise NotConfigured("Couldn't fetch cache settings from S3 bucket {}".format('cache_bucket_name'))

        if self.git_branch == 'production':
            config = cache.get('production')
        else:
            config = cache.get('dev')
        cache_spiders = config.get('include')
        self.settings.set('CACHE_SPIDERS', cache_spiders)
        self.settings.set('CACHE_ENABLED', True)
        self.logger.debug('Cache settings are updated from S3 bucket')

    def get_settings(self, bucket_resource, bucket_name, bucket_key):
        settings_raw_file = StringIO()
        bucket_resource.Bucket(bucket_name).download_fileobj(
            bucket_key,
            settings_raw_file
        )
        settings_raw_file.seek(0)
        config = json.load(settings_raw_file)
        settings_raw_file.close()
        return config

    def add_visibility_update_pool(self, message):
        looping_call = task.LoopingCall(self.update_visibility, message.raw_message)
        looping_call.start(
            interval=self.input_queue_timeout - self.input_queue_timeout_offset,
            now=False
        )
        self.visibility_update_pool.update({
            message.raw_message: looping_call
        })

    def remove_visibility_update_pool(self, message):
        looping_call = self.visibility_update_pool.pop(message.raw_message, None)
        if looping_call:
            looping_call.stop()

    def update_visibility(self, message):
        self.input_queue.change_message_visibility_batch(
            Entries=[{
                'Id': str(uuid4()),
                'ReceiptHandle': message.receipt_handle,
                'VisibilityTimeout': self.input_queue_timeout
            }]
        )

    def process_input_queue(self):
        def get_messages(number_of_messages=1, timeout=None, long_polling=False):
            assert isinstance(number_of_messages, int) and 0 < number_of_messages
            assert (isinstance(timeout, int) and 0 <= timeout < 43200) or timeout is None
            assert isinstance(long_polling, bool)

            if number_of_messages > 10:
                number_of_messages = 10

            if not timeout:
                timeout = self.input_queue_timeout

            return self.input_queue.receive_messages(
                MaxNumberOfMessages=number_of_messages,
                VisibilityTimeout=timeout,
                WaitTimeSeconds=20 if long_polling else 0
            )

        def process_messages(messages):
            for raw_message in messages:
                message = self.resolve(raw_message=raw_message)
                if message:
                    self.in_progress_messages.append(raw_message)
                    self.add_visibility_update_pool(message)
                    self.process_input_message(message)
                else:
                    self.logger.warning('Can not handle such message format: {}'.format(raw_message.body))
                    raw_message.delete()
                    self.process_input_queue()

        def process_grace_period():
            if not self.settings.get('RUNNER_GRACE_PERIOD_ENABLED', True):
                self.logger.debug('Grace period is disabled')
                return

            grace_period = self.settings.getint(
                'RUNNER_GRACE_PERIOD',
                default=self.input_queue_timeout + self.input_queue_timeout_offset
            )
            polling_attempts = int(grace_period / 20) + (grace_period % 20 > 0)
            self.logger.info('Start long-polling for SQS messages during {} seconds (20 seconds x {} attempts)'.format(
                grace_period,
                polling_attempts
            ))

            while polling_attempts > 0:
                self.logger.debug('Left {} long-polling attempts'.format(polling_attempts))
                polling_attempts -= 1
                messages = get_messages(
                    number_of_messages=10,
                    long_polling=True
                )
                if messages:
                    self.logger.info('Got {} messages while long-polling. Continue crawling'.format(len(messages)))
                    return messages

        def process_grace_period_callback(messages):
            if not messages:
                self.logger.info('There are no messages in SQS queue and in-progress tasks. Shutting down. Bye!')
                reactor.callFromThread(self._graceful_stop_reactor)
                return
            process_messages(messages)

        self.logger.debug('Processing input queue')
        while len(self.in_progress_messages) < self.max_tasks:
            number_of_messages = self.max_tasks - len(self.in_progress_messages)
            self.logger.debug('Try to get {} messages'.format(number_of_messages))

            messages = get_messages(number_of_messages)
            self.logger.debug('Got {} messages exactly'.format(len(messages)))
            if not messages:
                if self.in_progress_messages:
                    return

                dt = threads.deferToThread(process_grace_period)
                dt.addCallback(process_grace_period_callback)
                return

            process_messages(messages)

    def process_output_queue(self, input_message_body, filename=None):
        output_message = self.resolve(
            message=input_message_body,
            bucket_key=filename,
            bucket_name=self.settings.get('OUTPUT_BUCKET_NAME')
        )
        if not self.output_queue_name:
            queue_name = output_message.get_queue_name()
            if not queue_name:
                self.logger.warning('There is no output queue name in SQS task message!')
                return
            self.output_queue = self.output_queue_resource.create_queue(QueueName=queue_name)
            self.logger.debug('Got or created SQS output queue {}'.format(queue_name))

        self.logger.debug('Output message type {} with data {}'.format(type(output_message), output_message))
        self.output_queue.send_message(MessageBody=str(output_message))

    def bucket_uploaded_callback(self, filename, spider):
        self.logger.debug('Output result file {} was successfully uploaded'.format(filename))
        if spider.message.raw_message in self.finished_messages:
            self.finished_messages.remove(spider.message.raw_message)
            self.finish(spider.message, filename)

    def bucket_failed_callback(self, failure, spider):
        self.logger.error('Error while uploading file output result file {}'.format(failure))
        if spider.message.raw_message in self.finished_messages:
            self.finished_messages.remove(spider.message.raw_message)
            self.finish(spider.message)

    def item_scraped_callback(self, response, spider):
        self.logger.debug('Item scraped with response {}'.format(response))
        if spider.message.raw_message in self.in_progress_messages:
            self.in_progress_messages.remove(spider.message.raw_message)
            self.finished_messages.append(spider.message.raw_message)

    def spider_closed_callback(self, reason, spider):
        if spider.message.raw_message in self.in_progress_messages:
            self.logger.warning(
                'For some reason raw_message still in in_progress list, it will be removed from queue'
            )
            self.in_progress_messages.remove(spider.message.raw_message)
            self.finish(spider.message)
        self.process_input_queue()

    def finish(self, message, filename=None):
        self.process_output_queue(message, filename)
        self.remove_visibility_update_pool(message)
        message.raw_message.delete()

    def start_crawler(self, spider_name, message, options):
        assert isinstance(spider_name, six.string_types)
        assert isinstance(message, BaseInputMessage)
        assert isinstance(options, dict)

        crawler = self.create_crawler(spider_name)

        crawler.signals.connect(self.bucket_uploaded_callback, signals.bucket_uploaded)
        crawler.signals.connect(self.bucket_failed_callback, signals.bucket_failed)

        crawler.signals.connect(self.item_scraped_callback, signals.item_scraped)
        crawler.signals.connect(self.spider_closed_callback, signals.spider_closed)

        self.crawl(
            crawler_or_spidercls=crawler,
            message=message,
            **options
        )

    def process_input_message(self, message):
        spider_name = message.get_spider_name()
        if spider_name not in self.spiders.list():
            self.logger.warning('Unsupported spider name {}'.format(spider_name))
            if message.raw_message in self.in_progress_messages:
                self.in_progress_messages.remove(message.raw_message)
            self.finish(message)
            self.process_input_queue()
            return
        self.start_crawler(
            spider_name=message.get_spider_name(),
            message=message,
            options=message.get_options()
        )
