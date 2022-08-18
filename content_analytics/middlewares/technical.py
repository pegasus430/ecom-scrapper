from datetime import datetime

from content_analytics.utils import cond_set_value


class TechnicalMiddleware(object):
    @staticmethod
    def process_spider_input(response, spider):
        item = response.meta.get('item')
        if item is not None:
            # set date
            cond_set_value(item, '_date', datetime.utcnow())

            # set redirect status
            cond_set_value(item, '_redirect', response.status == 302)

            # set response code
            cond_set_value(item, '_response_code', response.status)

            # set time spent for all requests
            item['_loaded_in_seconds'] = item.get('_loaded_in_seconds', 0) + response.meta.get('download_latency', 0)

            # general fields
            cond_set_value(item, 'url', response.url)
            cond_set_value(item, 'site', spider.message.get('site') or
                                         spider.allowed_domains[0] if spider.allowed_domains else None)
            cond_set_value(item, 'product_id', spider.message.get('product_id'))
            response.meta['item'] = item
