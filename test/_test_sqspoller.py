# pylint: skip-file
import mock
import pytest
from scrapy.exceptions import NotConfigured

from content_analytics.middlewares.sqspoller import SQSPollerMiddleware


class TestSQSPollerMiddleware(object):
    __module = 'content_analytics.sqspoller'

    @mock.patch.object(
        SQSPollerMiddleware, '_SQSPollerMiddleware__setup_keep_messages'
    )
    @mock.patch.object(
        SQSPollerMiddleware, '_SQSPollerMiddleware__setup_queue'
    )
    def __get_instance(
            self, mocked_setup_keep_messages, mocked_setup_queue,
            aws_options=None, queue_name=None
    ):
        if aws_options is None:
            aws_options = {}
        return SQSPollerMiddleware(aws_options, queue_name)

    def test_from_crawler__SQSPOLLER_ENABLED_false(self):
        crawler = mock.MagicMock()
        crawler.settings.getbool.return_value = False
        with pytest.raises(NotConfigured):
            SQSPollerMiddleware.from_crawler(crawler)
        crawler.settings.getbool.assert_called_once_with(
            'SQSPOLLER_ENABLED', False
        )

    @mock.patch('{}.aws_from_settings'.format(__module))
    def test_from_crawler__aws_options_None(self, mocked_aws_from_settings):
        crawler = mock.MagicMock()
        crawler.settings.getbool.return_value = True
        mocked_aws_from_settings.return_value = None
        with pytest.raises(NotConfigured):
            SQSPollerMiddleware.from_crawler(crawler)
        crawler.settings.getbool.assert_called_once_with(
            'SQSPOLLER_ENABLED', False
        )
        mocked_aws_from_settings.assert_called_once_with(
            crawler.settings, prefix='SQSPOLLER_'
        )

    @mock.patch('{}.aws_from_settings'.format(__module))
    def test_from_crawler__SQSPOLLER_QUEUE_None(
            self, mocked_aws_from_settings
    ):
        crawler = mock.MagicMock()
        crawler.settings.getbool.return_value = True
        crawler.settings.get.return_value = None
        with pytest.raises(NotConfigured):
            SQSPollerMiddleware.from_crawler(crawler)
        crawler.settings.getbool.assert_called_once_with(
            'SQSPOLLER_ENABLED', False
        )
        mocked_aws_from_settings.assert_called_once_with(
            crawler.settings, prefix='SQSPOLLER_'
        )
        crawler.settings.get.assert_called_once_with('SQSPOLLER_QUEUE')

    @mock.patch('{}.aws_from_settings'.format(__module))
    @mock.patch.object(
        SQSPollerMiddleware, '__init__'
    )
    def test_from_crawler(self, mocked_init, mocked_aws_from_settings):
        mocked_init.return_value = None
        crawler = mock.MagicMock()
        crawler.settings.getbool.return_value = True
        crawler.settings.get.return_value = 'Queue'
        SQSPollerMiddleware.from_crawler(crawler)
        crawler.settings.getbool.assert_called_once_with(
            'SQSPOLLER_ENABLED', False
        )
        crawler.settings.get.assert_called_once_with('SQSPOLLER_QUEUE')
        mocked_init.assert_called_once_with(
            mocked_aws_from_settings.return_value,
            crawler.settings.get.return_value
        )

    @mock.patch.object(
        SQSPollerMiddleware, '_SQSPollerMiddleware__setup_keep_messages'
    )
    @mock.patch.object(
        SQSPollerMiddleware, '_SQSPollerMiddleware__setup_queue'
    )
    def test___init__(self, mocked_setup_queue, mocked_setup_keep_messages):
        aws_options, queue_name = None, None
        SQSPollerMiddleware(aws_options, queue_name)
        mocked_setup_queue.assert_called_once_with(aws_options, queue_name)
        mocked_setup_keep_messages.assert_called_once_with()

    @mock.patch('{}.logging'.format(__module))
    @mock.patch('{}.boto'.format(__module))
    def test___setup_queue(self, mocked_boto, mocked_logging):
        aws_options, queue_name = {}, None
        mware = SQSPollerMiddleware(aws_options, queue_name)
        mocked_logging.getLogger.assert_called_once_with('boto')
        mocked_logging.getLogger.return_value.setLevel\
            .assert_called_once_with(mocked_logging.WARNING)
        mocked_boto.connect_sqs.assert_called_once_with(**aws_options)
        mocked_boto.connect_sqs.return_value.get_queue\
            .assert_called_once_with(queue_name)
        mocked_boto.connect_sqs.return_value.get_queue.return_value\
            .set_message_class.assert_called_once_with(
                mocked_boto.sqs.message.RawMessage
            )
        assert mware.input_queue is mocked_boto.connect_sqs.return_value\
            .get_queue.return_value

    def test___keep_messages__empty(self):
        mware = self.__get_instance()
        mware._SQSPollerMiddleware__keep_messages()
        mware.input_queue.change_message_visibility_batch.assert_not_called()

    def test___keep_messages__one(self):
        mware = self.__get_instance()
        mware.in_progress = [None]
        mware._SQSPollerMiddleware__keep_messages()
        mware.input_queue.change_message_visibility_batch\
            .assert_called_once_with([(None, 60)])

    def test___keep_messages__ten(self):
        mware = self.__get_instance()
        mware.in_progress = range(10)
        mware._SQSPollerMiddleware__keep_messages()
        mware.input_queue.change_message_visibility_batch\
            .assert_called_once_with(map(lambda x: (x, 60), range(10)))

    def test_spider_closed__stop_loopingcall(self):
        mware = self.__get_instance()
        mware._SQSPollerMiddleware__keep_messages_loop = mock.MagicMock()
        mware._SQSPollerMiddleware__keep_messages_loop.running = True
        mware.spider_closed()
        mware._SQSPollerMiddleware__keep_messages_loop.stop\
            .assert_called_once_with()

    def test_spider_closed__empty(self):
        mware = self.__get_instance()
        mware._SQSPollerMiddleware__keep_messages_loop = mock.MagicMock()
        mware._SQSPollerMiddleware__keep_messages_loop.running = False
        mware.spider_closed()
        mware._SQSPollerMiddleware__keep_messages_loop.stop.assert_not_called()
        mware.input_queue.delete_message_batch.assert_not_called()
        mware.input_queue.change_message_visibility_batch.assert_not_called()

    def test_spider_closed__one(self):
        mware = self.__get_instance()
        mware._SQSPollerMiddleware__keep_messages_loop = mock.MagicMock()
        mware._SQSPollerMiddleware__keep_messages_loop.running = False
        mware.in_progress = [None]
        mware.done = [None]
        mware.spider_closed()
        mware._SQSPollerMiddleware__keep_messages_loop.stop.assert_not_called()
        mware.input_queue.delete_message_batch.assert_called_once_with([None])
        assert not mware.done
        mware.input_queue.change_message_visibility_batch\
            .assert_called_once_with([(None, 1)])
        assert not mware.in_progress

    def test_process_start_requests__empty(self):
        mware = self.__get_instance()
        mware.input_queue.get_messages.return_value = []
        with pytest.raises(StopIteration):
            next(mware.process_start_requests(None, None))
        mware.input_queue.get_messages.assert_called_once_with(
            10, visibility_timeout=60, wait_time_seconds=1
        )

    def test_process_start_requests__many(self):
        mware = self.__get_instance()
        message = mock.MagicMock()
        message.get_body.return_value = '{"url": "http://example.com"}'
        messages = [message for _ in range(100)]
        __messages = iter([messages, []])  # avoids an infinite loop

        def get_messages(*args, **kwargs):
            return next(__messages)

        mware.input_queue.get_messages = get_messages
        spider = mock.MagicMock()
        requests = [x for x in mware.process_start_requests(None, spider)]
        assert len(requests) == 100

    def test_process_start_requests__make_request_from_message(self):
        mware = self.__get_instance()
        message = mock.MagicMock()
        message.get_body.return_value = '{"url": "http://example.com"}'
        mware.input_queue.get_messages.return_value = [message]
        spider = mock.MagicMock()
        request = next(mware.process_start_requests(None, spider))
        spider.make_request_from_message.assert_called_once_with(
            {'url': 'http://example.com'}
        )
        assert spider.make_request_from_message.return_value is request
        request.meta.__setitem__.assert_called_once_with('_message', message)

    def test_process_start_requests__make_request_from_url(self):
        mware = self.__get_instance()
        message = mock.MagicMock()
        message.get_body.return_value = '{"url": "http://example.com"}'
        mware.input_queue.get_messages.return_value = [message]
        spider = mock.MagicMock()
        del spider.make_request_from_message
        request = next(mware.process_start_requests(None, spider))
        spider.make_request_from_url.assert_called_once_with(
            'http://example.com'
        )
        assert spider.make_request_from_url.return_value is request
        request.meta.__setitem__.assert_called_once_with('_message', message)

    def test_process_spider_input__no_key(self):
        mware = self.__get_instance()
        mware.done = mock.MagicMock()
        response = mock.MagicMock()
        response.meta = {}
        result = mware.process_spider_input(response, None)
        assert result is None
        mware.done.append.assert_not_called()

    def test_process_spider_input__one(self):
        mware = self.__get_instance()
        mware.in_progress = mock.MagicMock()
        response = mock.MagicMock()
        message = mock.MagicMock()
        response.meta = {'_message': message}
        assert len(mware.done) == 0
        result = mware.process_spider_input(response, None)
        assert result is None
        assert len(mware.done) == 1
        mware.in_progress.remove.assert_called_once_with(message)
        mware.input_queue.delete_message_batch.assert_not_called()

    def test_process_spider_input__ten(self):
        mware = self.__get_instance()
        message = mock.MagicMock()
        mware.done.extend([message for _ in range(9)])
        __done = [message for _ in range(10)]
        mware.in_progress = mock.MagicMock()
        response = mock.MagicMock()
        response.meta = {'_message': message}
        assert len(mware.done) == 9
        result = mware.process_spider_input(response, None)
        assert result is None
        mware.in_progress.remove.assert_called_once_with(message)
        mware.input_queue.delete_message_batch.assert_called_once_with(__done)
        assert len(mware.done) == 0

    def test_process_spider_input__eleven(self):
        mware = self.__get_instance()
        message = mock.MagicMock()
        mware.done.extend([message for _ in range(10)])
        __done = [message for _ in range(10)]
        mware.in_progress = mock.MagicMock()
        response = mock.MagicMock()
        response.meta = {'_message': message}
        assert len(mware.done) == 10
        result = mware.process_spider_input(response, None)
        assert result is None
        mware.in_progress.remove.assert_called_once_with(message)
        mware.input_queue.delete_message_batch.assert_called_once_with(__done)
        assert len(mware.done) == 1
