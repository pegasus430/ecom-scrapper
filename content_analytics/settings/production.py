BOT_NAME = 'content_analytics'

SPIDER_MODULES = ['content_analytics.spiders']
NEWSPIDER_MODULE = 'content_analytics.spiders'

RUNNER_MAX_TASKS = 20
RUNNER_GRACE_PERIOD = 310
RUNNER_GRACE_PERIOD_ENABLED = True
RUNNER_SETTINGS_BUCKET_ENABLED = True
RUNNER_CACHE_SETTINGS_BUCKET_ENABLED = True
REACTOR_THREADPOOL_MAXSIZE = 50

RETRY_TIMES = 20

LOG_LEVEL = 'WARNING'

FILEBEAT_ENABLED = True
FILEBEAT_PATH = '/tmp/filebeat'  # nosec

INPUT_QUEUE_VISIBILITY_TIMEOUT_OFFSET = 10
INPUT_QUEUE_NAME = ''
INPUT_QUEUE_AWS_REGION_NAME = 'us-east-1'
INPUT_QUEUE_AWS_ACCESS_KEY_ID = ''
INPUT_QUEUE_AWS_SECRET_ACCESS_KEY = ''

OUTPUT_QUEUE_NAME = None
OUTPUT_QUEUE_AWS_REGION_NAME = 'us-east-1'
OUTPUT_QUEUE_AWS_ACCESS_KEY_ID = ''
OUTPUT_QUEUE_AWS_SECRET_ACCESS_KEY = ''

OUTPUT_BUCKET_NAME = 'ch-responses'
OUTPUT_BUCKET_AWS_REGION_NAME = 'us-east-1'
OUTPUT_BUCKET_AWS_ACCESS_KEY_ID = ''
OUTPUT_BUCKET_AWS_SECRET_ACCESS_KEY = ''

# Same key names, as in old architecture
CACHE_BUCKET_NAME = 'settings.contentanalyticsinc.com'
CACHE_SETTINGS_BUCKET_KEY = 'cache.json'
PROXY_SETTINGS_PRODUCTION_BUCKET_KEY = 'global_proxy_config.cfg'
PROXY_SETTINGS_DEVELOPMENT_BUCKET_KEY = 'master_proxy_config.cfg'
SETTINGS_BUCKET_NAME = 'scraper-settings'
SETTINGS_BUCKET_AWS_REGION_NAME = 'us-east-1'
SETTINGS_BUCKET_AWS_ACCESS_KEY_ID = ''
SETTINGS_BUCKET_AWS_SECRET_ACCESS_KEY = ''

CACHE_ENABLED = False  # set to True in runner if cache config is present
CACHE_MODULE = 'content_analytics.middlewares.cache.aero.AerospikeCache'
CACHE_HOSTS = 'aerospike.aerospike:3000'
CACHE_USERNAME = None
CACHE_PASSWORD = None
CACHE_NAMESPACE = 'cache'
CACHE_SET = None  # set in aerospike cache init method based on scraper name
CACHE_DEFAULT_TTL = 5 * 24 * 60 * 60
CACHE_DEFAULT_POLICIES = {}

ROBOTSTXT_OBEY = False
TELNETCONSOLE_ENABLED = False

SPLASH_URL = 'http://splash:8050'

DUPEFILTER_CLASS = 'scrapy.dupefilters.BaseDupeFilter'

SPIDER_MIDDLEWARES = {
    'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': None,
    'content_analytics.middlewares.httperror.HttpErrorMiddleware': 50,
    'content_analytics.middlewares.mergeitem.MergeItemMiddleware': 51,
    'content_analytics.middlewares.technical.TechnicalMiddleware': 52,
    'content_analytics.middlewares.content.ContentMiddleware': 53,
}

DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
    'content_analytics.middlewares.cache.CacheMiddleware': 1,
    'content_analytics.middlewares.proxy.ProxyRetryDownloaderMiddleware': 550,
    'content_analytics.middlewares.splash.SplashRetryMiddleware': 555,
    'scrapy_splash.SplashCookiesMiddleware': 723,
    'content_analytics.middlewares.splash.CustomSplashMiddleware': 725,
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
    'content_analytics.middlewares.mergeitem.MergeItemDownloaderMiddleware': 999,
}

EXTENSIONS = {
    'scrapy.extensions.telnet.TelnetConsole': None,
    'scrapy.extensions.statsmailer.StatsMailer': None,
    'content_analytics.extensions.filebeat.FilebeatExtension': 10,
}

ITEM_PIPELINES = {
    'content_analytics.pipelines.simple_validator.SimpleValidator': 998,
    'content_analytics.pipelines.s3export.S3ExportPipeline': 999,
}

DOWNLOADER_CLIENTCONTEXTFACTORY = 'content_analytics.utils.CustomClientContextFactory'

FEED_FORMAT = 'jsonlines'
FEED_EXPORT_ENCODING = 'utf8'
FEED_EXPORTERS = {
    'jl': 'content_analytics.exporters.CompatibleJsonLinesItemExporter',
    'jsonline': 'content_analytics.exporters.CompatibleJsonLinesItemExporter',
}

SENTRY_ENABLED = True
SENTRY_DSN = 'https://b261517d2e8a4801bc5019f75563ca46:03da766c0b6546049676c9f83a2e7be0@sentry.io/222620'
