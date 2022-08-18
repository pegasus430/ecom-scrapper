import six
import json
import logging
import traceback

from zlib import compress
from base64 import b64encode
from datetime import datetime

from content_analytics.messages import BaseInputMessage, BaseOutputMessage as _BaseOutputMessage, BaseMessageResolver

logger = logging.getLogger(__name__)


class BaseOutputMessage(_BaseOutputMessage):
    def __repr__(self):
        message = super(BaseOutputMessage, self).__repr__()
        message = compress(message)
        message = b64encode(message)
        return message


class InputMessage(BaseInputMessage):
    SPIDER_NAME_FORMAT = '{}_products'
    spider_name = None

    def __init__(self, raw_message):
        try:
            message = json.loads(raw_message.body)
            assert isinstance(message.get('url'), six.string_types)
            assert isinstance(message.get('site'), six.string_types)
            assert isinstance(message.get('result_queue'), six.string_types)
            assert isinstance(message.get('response_format'), six.string_types)
            assert isinstance(message.get('cmd_args'), dict) or message.get('cmd_args') is None

            self.spider_name = self.SPIDER_NAME_FORMAT.format(message.get('site'))
            self.options = message.get('cmd_args', {})
            super(InputMessage, self).__init__(raw_message, message)
        except Exception:
            logger.error('Error while parsing raw message {}'.format(traceback.format_exc()))

    def get_spider_name(self):
        return self.spider_name

    def get_format(self):
        return self.get('response_format')
    
    def get_options(self):
        return self.options


class SuccessMessage(BaseOutputMessage):
    def __init__(self, input_message, filename, bucket_name):
        super(SuccessMessage, self).__init__({
            'url': input_message.get('url'),
            'event': input_message.get('event'),
            'site_id': input_message.get('site_id'),
            'product_id': input_message.get('product_id'),
            'date': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            's3_filepath': filename,
            'bucket_name': bucket_name,
            'status': 'failure',
            'failure_type': 'Response too large'
        })
        self.queue_name = input_message.get('result_queue')

    def get_queue_name(self):
        return self.queue_name


class FailureMessage(BaseOutputMessage):
    def __init__(self, input_message):
        super(FailureMessage, self).__init__({
            'url': input_message.get('url'),
            'event': input_message.get('event'),
            'site_id': input_message.get('site_id'),
            'product_id': input_message.get('product_id'),
            'date': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'failure'
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
            filename = kwargs.pop('bucket_key', None)
            bucket_name = kwargs.pop('bucket_name', None)
            if filename and bucket_name:
                return SuccessMessage(message, filename, bucket_name)
            return FailureMessage(message)
