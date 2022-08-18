BOT_NAME = 'content_analytics'

SPIDER_MODULES = ['content_analytics.spiders']
NEWSPIDER_MODULE = 'content_analytics.spiders'

LOG_LEVEL = 'DEBUG'
# LOG_FILE = '/tmp/scrapy.log'

# If you want to use or test cache locally, just switch CACHE_ENABLED to True,
# run 'pip install aerospike', because it's not set in requirements because of problems with installing it on aerospike,
# download docker and run in terminal "docker run -p3000:3000 -e 'NAMESPACE=cache' aerospike/aerospike-server".
# It'll run a local instance of aerospike. Now you're good to run crawlers with cache locally.
CACHE_ENABLED = False
CACHE_SPIDERS = ['walmart_products']
CACHE_MODULE = 'content_analytics.middlewares.cache.aero.AerospikeCache'
CACHE_HOSTS = 'localhost:3000'
CACHE_USERNAME = None
CACHE_PASSWORD = None
CACHE_NAMESPACE = 'cache'
CACHE_SET = None  # set in aerospike cache init method based on scraper name
CACHE_DEFAULT_TTL = 5 * 24 * 60 * 60
CACHE_DEFAULT_POLICIES = {}

ROBOTSTXT_OBEY = False

DUPEFILTER_CLASS = 'scrapy.dupefilters.BaseDupeFilter'

# You can run splash locally for testing with a simple docker command
# docker run -p 8050:8050 --net=host scrapinghub/splash --max-timeout 240 --disable-private-mode -v2 --slots 3
# then splash middleware will magically work
SPLASH_URL = 'http://localhost:8050'

SPIDER_MIDDLEWARES = {
    'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': None,
    'content_analytics.middlewares.httperror.HttpErrorMiddleware': 50,
    'content_analytics.middlewares.mergeitem.MergeItemMiddleware': 51,
    'content_analytics.middlewares.technical.TechnicalMiddleware': 52,
    'content_analytics.middlewares.content.ContentMiddleware': 53,
}

DOWNLOADER_MIDDLEWARES = {
    'content_analytics.middlewares.splash.SplashRetryMiddleware': 555,
    'scrapy_splash.SplashCookiesMiddleware': 723,
    'content_analytics.middlewares.splash.CustomSplashMiddleware': 725,
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
    'content_analytics.middlewares.cache.CacheMiddleware': 1,
    'content_analytics.middlewares.mergeitem.MergeItemDownloaderMiddleware': 999,
}

EXTENSIONS = {}

ITEM_PIPELINES = {'content_analytics.pipelines.simple_validator.SimpleValidator': 998}

DOWNLOADER_CLIENTCONTEXTFACTORY = 'content_analytics.utils.CustomClientContextFactory'

FEED_FORMAT = 'jsonlines'
FEED_EXPORT_ENCODING = 'utf8'
FEED_EXPORTERS = {
    'jl': 'content_analytics.exporters.CompatibleJsonLinesItemExporter',
    'jsonline': 'content_analytics.exporters.CompatibleJsonLinesItemExporter',
}
