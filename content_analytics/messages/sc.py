import six
import json
import logging

from datetime import datetime

from content_analytics.messages import BaseInputMessage, BaseOutputMessage, BaseMessageResolver

logger = logging.getLogger(__name__)


class InputMessage(BaseInputMessage):
    SPIDER_NAME_FORMAT = '{}_products'
    SPIDER_NAME_SHELF_FORMAT = '{}_products'

    options = {}
    spider_name = None

    def __init__(self, raw_message):
        try:
            message = json.loads(raw_message.body)
            assert isinstance(message.get('url'), six.string_types) or \
                   isinstance(message.get('searchterms_str'), six.string_types)

            assert isinstance(message.get('quantity'), six.string_types) or message.get('quantity') is None
            assert isinstance(message.get('pages_count'), (six.string_types, int)) or message.get('pages_count') is None
            assert isinstance(message.get('num_pages'), (six.string_types, int)) or message.get('num_pages') is None
            assert isinstance(message.get('crawl_date'), six.string_types) or message.get('crawl_date') is None
            assert isinstance(message.get('summary'), six.string_types) or message.get('summary') is None

            assert isinstance(message.get('site'), six.string_types)
            assert isinstance(message.get('result_queue'), six.string_types)
            assert isinstance(message.get('response_format'), six.string_types)

            assert isinstance(message.get('cmd_args'), dict) or message.get('cmd_args') is None

            if message.get('searchterms_str'):
                message['search_term'] = message.pop('searchterms_str')

            if message.get('cmd_args', {}).get('quantity'):
                message['quantity'] = message.get('cmd_args', {}).get('quantity')

            if '_shelf_urls' in message.get('site'):
                self.spider_name = self.SPIDER_NAME_SHELF_FORMAT.format(
                    message.get('site').replace("_shelf_urls", ""))

                message['shelf_url'] = message.pop('url')
            else:
                self.spider_name = self.SPIDER_NAME_FORMAT.format(message.get('site'))

            self.options = message.setdefault('cmd_args', {})

            if message.get('crawl_date'):
                self.options['crawl_date'] = message.get('crawl_date')

            super(InputMessage, self).__init__(raw_message, message)
        except Exception as e:
            logger.error('Error while parsing raw message {}'.format(e))

    def get_spider_name(self):
        return self.spider_name

    def get_format(self):
        return self.get('response_format')

    def get_options(self):
        return self.options


class SuccessMessage(BaseOutputMessage):
    def __init__(self, input_message, bucket_name, bucket_key):
        super(SuccessMessage, self).__init__({
            # General parameters
            'msg_id': input_message.get('task_id'),
            'server_ip': input_message.get('server_ip'),
            'utc_datetime': datetime.utcnow().isoformat(),
            'status': 'success',
            'site': input_message.get('site'),

            # Spider parameters
            'url': input_message.get('url') or input_message.get('shelf_url'),
            'searchterms_str': input_message.get('search_term'),

            # Results
            'bucket_name': bucket_name,
            's3_key_data': bucket_key,
        })
        self.queue_name = input_message.get('result_queue')

    def get_queue_name(self):
        return self.queue_name


class FailureMessage(BaseOutputMessage):
    def __init__(self, input_message):
        super(FailureMessage, self).__init__({
            # General parameters
            'msg_id': input_message.get('task_id'),
            'server_ip': input_message.get('server_ip'),
            'utc_datetime': datetime.utcnow().isoformat(),
            'status': 'failure',
            'site': input_message.get('site'),

            'url': input_message.get('url') or input_message.get('shelf_url'),
            'searchterms_str': input_message.get('search_term'),
        })
        self.queue_name = input_message.get('result_queue')

    def get_queue_name(self):
        return self.queue_name


class MessageResolver(BaseMessageResolver):
    @classmethod
    def resolve(cls, raw_message=None, message=None, **kwargs):
        if raw_message:
            return InputMessage(raw_message)
        if message:
            bucket_name = kwargs.pop('bucket_name', None)
            bucket_key = kwargs.pop('bucket_key', None)
            if bucket_name and bucket_key:
                return SuccessMessage(message, bucket_name, bucket_key)
            return FailureMessage(message)
