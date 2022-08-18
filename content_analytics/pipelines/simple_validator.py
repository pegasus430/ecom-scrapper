from scrapy.exceptions import DropItem


class SimpleValidationError(DropItem):
    message = 'Title and image are not presented!'


class SimpleValidator(object):
    VALIDATION_FAILURE_FIELD = 'validation_failures_count'

    def __init__(self, stats):
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

    def process_item(self, item, spider):
        # Do not validate for search terms and shelf urls
        if spider.search_term or spider.shelf_url:
            return item
        # Do not validate INLA, invalid url, redirect, 404 and 520 responses
        if item.get("temporary_unavailable") or item.get("not_found") or item.get('no_longer_available') \
                or item.get('redirect') or item.get('invalid_url'):
            return item
        if not item.get('title') and not item.get('image_url'):
            self.stats.inc_value(self.VALIDATION_FAILURE_FIELD)
            raise SimpleValidationError()
        return item
