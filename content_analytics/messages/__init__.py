# pylint: disable=R0401
import six
import json
import logging


class BaseInputMessage(dict):
    raw_message = None

    def __init__(self, raw_message, message):
        self.raw_message = raw_message
        super(BaseInputMessage, self).__init__(message)

    def get_spider_name(self):
        raise NotImplementedError

    def get_format(self):
        raise NotImplementedError

    def get_options(self):
        return {}


class BaseOutputMessage(dict):
    queue_name = None

    def __repr__(self):
        return json.dumps(self)

    def get_queue_name(self):
        raise NotImplementedError


class BaseMessageResolver(object):
    @classmethod
    def resolve(cls, raw_message=None, message=None, **kwargs):
        raise NotImplementedError


class MessageResolverMixin(object):
    from content_analytics.messages.ch import MessageResolver as CHMessageResolver
    from content_analytics.messages.sc import MessageResolver as SCMessageResolver

    logger = logging.getLogger(__name__)
    resolvers = {
        'ch': CHMessageResolver,
        'sc': SCMessageResolver,
    }

    def resolve(self, raw_message=None, message=None, **kwargs):
        if isinstance(message, BaseInputMessage):
            resolver = self.resolvers.get(message.get_format())
            if not resolver:
                self.logger.warning('Unknown resolver for message {}'.format(message))
                return
            return resolver.resolve(message=message, **kwargs)
        if raw_message:
            try:
                message = json.loads(raw_message.body)
            except Exception as e:
                self.logger.error('Error while parsing message json {}'.format(e))
            else:
                resolver = self.resolvers.get(message.get('response_format'))
                if resolver:
                    return resolver.resolve(raw_message=raw_message)
                elif not resolver and not message.get("url"):
                    # Shelf and search term tasks should have sc format
                    self.logger.warning('Assuming SC format for message {}'.format(message))
                    resolver = self.resolvers.get('sc')
                    return resolver.resolve(raw_message=raw_message)
