import json
import logging
import os
from datetime import timedelta
from uuid import uuid4

from raven import Client as SentryClient
from raven.conf import setup_logging
from raven.handlers.logging import SentryHandler
from scrapy.exceptions import NotConfigured
from scrapy.utils.serialize import ScrapyJSONEncoder

from content_analytics import signals
from content_analytics.pipelines.simple_validator import SimpleValidator
from content_analytics.utils import cond_set_value


class FilebeatJSONEncoder(ScrapyJSONEncoder):
    # pylint: disable=method-hidden
    def default(self, o):
        if isinstance(o, timedelta):
            return o.total_seconds()
        return super(FilebeatJSONEncoder, self).default(o)


class FilebeatEntry(dict):
    def __init__(self):
        super(FilebeatEntry, self).__init__({
            'build': None,

            'duration': None,
            'failure_cause': None,
            'git_branch': None,
            'page_size': None,
            'pl_name': None,
            'proxy_config_file': None,
            'proxy_service': None,
            'response_time': None,
            'response_format': None,
            's3_filepath': None,
            'spider_name': None,
            'server_hostname': None,
            'status': None,
            'status_code': None,
            'url': None,
            'shelf_url': None,
            'search_term': None,
            'scrapy_stats': None,
            'input_queue_name': None,
            'output_queue_name': None,
            'job_id': None,
            'slack_username': None,
            'errors_traceback': None
        })


class FilebeatExtension(object):
    STATUS_SUCCEED = 'ok'
    STATUS_FAILED = 'failed'
    FAILURE_CAUSE_REQUEST_DROPPED = 'Request dropped'
    FAILURE_CAUSE_ITEM_NOT_SCRAPED = 'Item not scraped'

    def __init__(self, stats, settings):
        self.scraped = False
        self.stats = stats
        self.entry = FilebeatEntry()
        self.path = settings.get('FILEBEAT_PATH', None)
        self.git_branch = os.environ.get('SCRAPERS_GIT_BRANCH')
        self.input_queue_name = settings.get('INPUT_QUEUE_NAME')
        if not self.path:
            raise NotConfigured('Filebeat path must be set')
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        self.sentry_enabled = all([
            settings.getbool('SENTRY_ENABLED'),
            settings.get('SENTRY_DSN'),
            self.git_branch in ['production', 'master']
        ])
        if self.sentry_enabled:
            self.client = SentryClient(
                settings.get('SENTRY_DSN')
            )
            handler = SentryHandler(self.client)
            handler.setLevel(logging.ERROR)
            setup_logging(handler)


    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('FILEBEAT_ENABLED', False):
            return None
        extension = cls(crawler.stats, crawler.settings)
        crawler.signals.connect(extension.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(extension.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(extension.spider_error, signal=signals.spider_error)

        crawler.signals.connect(extension.request_scheduled, signal=signals.request_scheduled)
        crawler.signals.connect(extension.request_dropped, signal=signals.request_dropped)

        crawler.signals.connect(extension.response_received, signal=signals.response_received)

        crawler.signals.connect(extension.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(extension.item_dropped, signal=signals.item_dropped)

        crawler.signals.connect(extension.bucket_uploaded, signal=signals.bucket_uploaded)
        crawler.signals.connect(extension.bucket_failed, signal=signals.bucket_failed)

        return extension

    def spider_opened(self, spider):
        if hasattr(spider, 'product_url'):
            cond_set_value(self.entry, 'url', spider.product_url)
        if hasattr(spider, 'shelf_url'):
            cond_set_value(self.entry, 'shelf_url', spider.shelf_url)
        if hasattr(spider, 'search_term'):
            cond_set_value(self.entry, 'search_term', spider.search_term)
        if hasattr(spider, 'quantity'):
            cond_set_value(self.entry, 'quantity', spider.quantity)
        if hasattr(spider, 'pages_count'):
            cond_set_value(self.entry, 'pages_count', spider.pages_count)
        if hasattr(spider, 'message'):
            cond_set_value(self.entry, 'task_message_str', str(spider.message))

            cmd_args = spider.message.get('cmd_args', {})
            for key, value in cmd_args.iteritems():
                if value in ['True', 'true']:
                    cmd_args[key] = True
                elif value in ['False', 'false']:
                    cmd_args[key] = False
            cond_set_value(self.entry, 'cmd_args', cmd_args)

            cond_set_value(self.entry, 'pl_name',
                           spider.message.get('pl_name') or
                           spider.message.get('product_list_name')
                           )
            cond_set_value(self.entry, 'server_hostname',
                           spider.message.get('server_hostname', spider.message.get('server_name'))
                           )
            cond_set_value(self.entry, 'job_id',
                           spider.message.get('task_id')
                           )
            cond_set_value(self.entry, 'response_format', spider.message.get('response_format'))
            cond_set_value(self.entry, 'input_queue_name', self.input_queue_name)
            cond_set_value(self.entry, 'output_queue_name', spider.message.get('result_queue'))

        # For slack notification bot
        if hasattr(spider, 'slack_username'):
            cond_set_value(self.entry, 'slack_username', spider.slack_username)

        cond_set_value(self.entry, 'git_branch', self.git_branch)
        cond_set_value(self.entry, 'spider_name', spider.name)
        cond_set_value(self.entry, 'proxy_config_file', spider.settings.get('SETTINGS_BUCKET_KEY'))

        # set required sentry tags
        self.setup_sentry_tags()

    def request_scheduled(self, request, spider):
        cond_set_value(self.entry, 'proxy_service', request.meta.get('proxy', 'none'))

    def request_dropped(self, request, spider):
        cond_set_value(self.entry, 'failure_cause', self.FAILURE_CAUSE_REQUEST_DROPPED)

    def response_received(self, response, request, spider):
        cond_set_value(self.entry, 'page_size', self.stats.get_value('downloader/request_bytes'))
        cond_set_value(self.entry, 'response_time', response.meta.get('download_latency'))
        cond_set_value(self.entry, 'status_code', response.status)

    def item_scraped(self, item):
        self.scraped = True

    def item_dropped(self, item, response, exception, spider):
        cond_set_value(self.entry, 'failure_cause', exception.message)

    def bucket_uploaded(self, filename, spider):
        cond_set_value(self.entry, 's3_filepath', filename)

    def bucket_failed(self, failure, spider):
        cond_set_value(self.entry, 'failure_cause', str(failure))

    def spider_error(self, failure, response, spider):
        cond_set_value(self.entry, 'errors_traceback', str(failure.getTraceback(detail="default")))
        cond_set_value(self.entry, 'failure_cause', str(failure.value))

    def spider_closed(self, spider):
        stats = self.stats.get_stats()
        if SimpleValidator.VALIDATION_FAILURE_FIELD in stats:
            self.entry[SimpleValidator.VALIDATION_FAILURE_FIELD] = stats.pop(SimpleValidator.VALIDATION_FAILURE_FIELD) or 0

        cond_set_value(self.entry, 'scrapy_stats', stats)
        cond_set_value(self.entry, 'duration', stats.get('finish_time') - stats.get('start_time'))
        cond_set_value(self.entry, 's3_filepath', getattr(spider, 's3_filepath', None))

        if self.entry.get('failure_cause', None):
            cond_set_value(self.entry, 'status', self.STATUS_FAILED)
            self.entry['s3_filepath'] = None
        if not self.scraped and not self.entry.get('failure_cause', None):
            cond_set_value(self.entry, 'status', self.STATUS_FAILED)
            cond_set_value(self.entry, 'failure_cause', self.FAILURE_CAUSE_ITEM_NOT_SCRAPED)
            self.entry['s3_filepath'] = None
        cond_set_value(self.entry, 'status', self.STATUS_SUCCEED)

        if self.path:
            filename = '{}_{}.json'.format(spider.name, str(uuid4()))
            filename = os.path.join(self.path, filename)
            with open(filename, 'w+') as f:
                json.dump(self.entry, f, cls=FilebeatJSONEncoder)
                f.write('\n')  # add end of file for filebeat parser

    def setup_sentry_tags(self):
        if self.sentry_enabled:
            self.client.tags_context(
                {
                    'git_branch': self.entry.get('git_branch'),
                    'server_hostname': self.entry.get('server_hostname'),
                }
            )
