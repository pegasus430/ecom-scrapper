import boto3
import logging

from uuid import uuid4
from datetime import datetime
from cStringIO import StringIO
from os.path import join

from boto3.s3.transfer import TransferConfig
from scrapy.exceptions import NotConfigured
from twisted.internet import threads

from content_analytics import signals
from content_analytics.utils import aws_from_settings
from content_analytics.exporters import CompatibleJsonLinesItemExporter

logger = logging.getLogger(__name__)


class S3ExportPipeline(object):
    BUCKET_KEY_FORMAT = 'output/{}/{}.jl'

    s3 = None
    file = None
    bucket = None
    settings = None

    def __init__(self, stats, settings):
        self.file = None
        self.filename = None
        self.bucket_name = None
        self.exporter = None
        self.stats = stats
        self.settings = settings

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls(crawler.stats, crawler.settings)
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def setup_bucket(self):
        aws_settings = aws_from_settings(self.settings, prefix='OUTPUT_BUCKET_')
        if aws_settings is None or not all(aws_settings.values()):
            aws_settings = aws_from_settings(self.settings, prefix='INPUT_QUEUE_')

        self.bucket_name = self.settings.get('OUTPUT_BUCKET_NAME')
        if not self.bucket_name:
            raise NotConfigured('S3 bucket export name must be set.')

        self.s3 = boto3.resource('s3', **aws_settings)
        self.bucket = self.s3.Bucket(self.bucket_name)

    def spider_opened(self, spider):
        def generate_key():
            return self.BUCKET_KEY_FORMAT.format(datetime.utcnow().strftime('%Y/%m/%d'), uuid4())

        self.setup_bucket()
        self.file = StringIO()
        self.filename = generate_key()

        # TODO: temporary fix related to CON-37613
        setattr(spider, 's3_filepath', join(self.bucket_name, self.filename))

        self.exporter = CompatibleJsonLinesItemExporter(self.file)
        self.exporter.start_exporting()

    def spider_closed(self, spider, sender, *args, **kwargs):
        def store():
            if not self.file.tell():
                raise Exception('Item was scraped, but it is empty')
            logger.debug('Storing results to {}'.format(self.filename))
            self.file.seek(0)
            self.bucket.upload_fileobj(
                Fileobj=self.file,
                Key=self.filename,
                Config=TransferConfig(
                    use_threads=False
                )
            )
            return self.filename

        def callback(filename, sender, **kwargs):
            logger.debug('Results were stored to {}'.format(filename))
            self.file.close()
            self.exporter.finish_exporting()
            return sender.signals.send_catch_log(
                signal=signals.bucket_uploaded,
                filename=filename,
                **kwargs
            )

        def errback(failure, sender, **kwargs):
            logger.error('Error while storing results {}'.format(failure))
            self.file.close()
            self.exporter.finish_exporting()
            return sender.signals.send_catch_log(
                signal=signals.bucket_failed,
                failure=failure,
                **kwargs
            )

        if spider._message and self.stats.get_value('item_scraped_count'):
            dt = threads.deferToThread(store)
            dt.addCallback(
                callback,
                sender=sender,
                spider=spider
            )
            dt.addErrback(
                errback,
                sender=sender,
                spider=spider
            )
            return dt

        logger.debug('Spider did not return items')
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        itemdict = self.exporter.export_item(item)
        return itemdict
