import re
import sys
import unicodedata
from datetime import datetime

from parsel import Selector
from scrapy.exporters import BaseItemExporter
from scrapy.utils.python import to_bytes
from scrapy.utils.serialize import ScrapyJSONEncoder

from content_analytics.items import Price


class CompatibleJsonLinesItemExporter(BaseItemExporter):
    def __init__(self, _file, **kwargs):
        # TODO: implement correct overriding of base class init method
        # super(CompatibleJsonLinesItemExporter, self).__init__(**kwargs)
        # pylint: disable=super-init-not-called
        self._configure(kwargs, dont_fail=True)
        self.file = _file
        kwargs.setdefault('ensure_ascii', not self.encoding)
        self.encoder = ScrapyJSONEncoder(**kwargs)

    def _bullet_feature_X(self, item, i):
        bullets = item.get('bullets')
        if bullets:
            bullets = bullets.split('\n')
            if len(bullets) > i:
                return bullets[i]

    # this is the default price implementation if price_amount is defined but not price
    def _price(self, item):
        try:
            return '${:2,.2f}'.format(item.get('price_amount'))
        except Exception:
            return None

    # this is the default price_amount implementation if price is defined but not price_amount
    def _price_amount(self, item):
        try:
            price_amount = float(re.search(r'[\d\.,]+', item.get('price')).group().replace(',', ''))
            return price_amount
        except Exception:
            return None

    def _in_stores_in_stock(self, item):
        if item.get('in_stores_out_of_stock') is None:
            return None
        return 0 if item.get('in_stores_out_of_stock') else 1

    def _in_stores_only(self, item):
        # if any of the seller types is None, return None (cannot be determined)
        if any(s is None for s in [item.get('site_online'), item.get('in_stores'), item.get('_ch_marketplace')]):
            return None

        if not item.get('site_online') and item.get('_ch_marketplace') and \
                item.get('in_stores'):
            return 1
        return 0

    def _marketplace_in_stock(self, item):
        if item.get('_ch_marketplace'):
            return 1 if any(m['in_stock'] for m in item.get('_ch_marketplace')) else 0

    def _marketplace_lowest_price(self, item):
        if item.get('_ch_marketplace'):
            return min(m['price'] for m in item.get('_ch_marketplace'))

    def _marketplace_out_of_stock(self, item):
        if item.get('_ch_marketplace'):
            return 0 if any(m['in_stock'] for m in item.get('_ch_marketplace')) else 1

    def _online_only(self, item):
        # if any of the seller types is None, return None (cannot be determined)
        if any(s is None for s in [item.get('site_online'), item.get('in_stores'), item.get('_ch_marketplace')]):
            return None

        if (item.get('site_online') or item.get('_ch_marketplace')) and not item.get('in_stores'):
            return 1
        return 0

    def _owned(self, item):
        if item.get('owned') is not None:
            return 1 if item.get('owned') else 0
        return 1 if item.get('site_online') or item.get('in_stores') else 0

    def _site_online_in_stock(self, item):
        if item.get('site_online_out_of_stock') is None:
            return None
        return 0 if item.get('site_online_out_of_stock') else 1

    def _webcollage(self, item):
        if any((item.get('wc_360'),
               item.get('wc_emc'),
               item.get('wc_pdf'),
               item.get('wc_prodtour'),
               item.get('wc_video'),
               item.get('webcollage_images_count'),
               item.get('webcollage_pdfs_count'),
               item.get('webcollage_videos_count'))):
            return 1
        return 0

    @staticmethod
    def search_term_in_title(search_term, title):
        def normalize(s):
            def remove_punctuation(text):
                # here we remove punctuation symbols from text
                tbl = dict.fromkeys(
                    i for i in xrange(sys.maxunicode) if unicodedata.category(unichr(i)).startswith('P'))
                return text.translate(tbl)

            # and here we use unicode normalization for text, ref: http://unicode.org/reports/tr15/
            filter_decodable_re = r'[^\x00-\x7F]+'
            normalized_filtered_string = unicodedata.normalize('NFKD', unicode(re.sub(filter_decodable_re, u'', s)))
            return remove_punctuation(normalized_filtered_string).lower().strip()

        title = normalize(title)
        title_words = set(title.split())
        search_term = normalize(search_term)
        search_term_words = set(search_term.split())

        exactly = search_term in title
        partial = search_term_words.issubset(title_words)
        # FIXME: need to explain what is this field exactly
        interleaved = False

        return exactly, partial, interleaved

    @staticmethod
    def get_image_url(item):
        image_urls = item.get('image_urls')
        image_url = item.get('image_url')
        if image_url:
            return image_url
        elif image_urls and isinstance(image_urls, list):
            return image_urls[0]

    def make_compatible(self, item):
        itemdict = dict(self._get_serialized_fields(item))
        item = {key: value for key, value in item.iteritems() if value is not None}
        if item.get('no_longer_available'):
            item['is_out_of_stock'] = True
        compatibility_item_dict = {
            'department': item.get('department'),
            'image_url': self.get_image_url(item),
            'buyer_reviews': item.get('buyer_reviews')
            if any(item.get('buyer_reviews', {}).get('stars', {}).values()) else None,
            'crawled_at': item.get('_date').strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(item.get('_date'), datetime) else None,
            'classification': {
                'brand': item.get('brand'),
                'categories': item.get('departments', []),
                'category_name': item.get('department'),
                'date': item.get('_date').strftime('%Y-%m-%d %H:%M:%S')
                if isinstance(item.get('_date'), datetime) else None
            },
            'page_attributes': {
                'bundle': int(item['bundle']) if item.get('bundle') else None,
                'canonical_link': item.get('meta', {}).get('canonical_url'),
                'collection_availability': item.get('collection_availability', 0),
                'collection_count': item.get('collection_count', 0),
                'how_to_measure': item.get('how_to_measure'),
                'htags': item.get('htags', {}),
                'image_alt_text': item.get('image_alts', []),
                'image_alt_text_len': [len(alt_text or '') for alt_text in item.get('image_alts', [])],
                'image_colors': item.get('image_colors', []),
                'image_count': len(item.get('image_urls', [])),
                'image_dimensions': item.get('image_dimensions', []),
                'image_res': item.get('image_res', []),
                'image_urls': item.get('image_urls', []),
                'keywords': item.get('meta', {}).get('keywords'),
                'loaded_in_seconds': item.get('_loaded_in_seconds'),
                'lowest_item_price': item.get('price_lowest'),
                'meta_description': item.get('meta', {}).get('description'),
                'meta_description_count': len(item.get('meta', {}).get('description', '') or ''),
                'meta_tags_count': len(item.get('meta', {}).get('meta_tags', [])),
                'meta_tags': [
                    Selector(meta_tag).xpath('//meta/@*').extract()
                    for meta_tag in item.get('meta', {}).get('meta_tags', [])
                ],
                'pdf_count': len(item.get('pdf_urls', [])),
                'pdf_urls': item.get('pdf_urls', []),
                'questions_total': item.get('questions_total', 0),
                'questions_unanswered': item.get('questions_unanswered', 0),
                'redirect': 1 if item.get('_redirect') else 0,
                'selected_variant': next((
                    ' '.join(v['properties'].values())
                    for v in item.get('variants', []) if v.get('selected')
                    and v.get('properties', {}).values()), None),
                'sellpoints': 1 if item.get('sellpoints') else 0,
                'swatches': item.get('swatches', []),
                'variants': item.get('variants', []),
                'video_urls': item.get('video_urls', []),
                'video_count': len(item.get('video_urls', [])),
                'wc_360': 1 if item.get('wc_360') else 0,
                'wc_emc': 1 if item.get('wc_emc') else 0,
                'wc_pdf': 1 if item.get('wc_pdf') else 0,
                'wc_prodtour': 1 if item.get('wc_prodtour') else 0,
                'wc_video': 1 if item.get('wc_video') else 0,
                'webcollage': self._webcollage(item),
                'webcollage_image_urls': item.get('webcollage_image_urls', []),
                'webcollage_images_count': item.get('webcollage_images_count', 0),
                'webcollage_pdfs_count': item.get('webcollage_pdfs_count', 0),
                'webcollage_videos_count': item.get('webcollage_videos_count', 0),
                'zoom_image_dimensions': item.get('zoom_image_dimensions', []),

            },
            'product_id': item.get('product_id'),
            'site_product_id': item.get('site_product_id') or item.get('reseller_id'),
            'reseller_id': item.get('site_product_id') or item.get('reseller_id'),
            'product_info': {
                'bullet_feature_1': self._bullet_feature_X(item, 0),
                'bullet_feature_2': self._bullet_feature_X(item, 1),
                'bullet_feature_3': self._bullet_feature_X(item, 2),
                'bullet_feature_4': self._bullet_feature_X(item, 3),
                'bullet_feature_5': self._bullet_feature_X(item, 4),
                'bullet_feature_6': self._bullet_feature_X(item, 5),
                'bullet_feature_7': self._bullet_feature_X(item, 6),
                'bullet_feature_8': self._bullet_feature_X(item, 7),
                'bullet_feature_9': self._bullet_feature_X(item, 8),
                'bullet_feature_10': self._bullet_feature_X(item, 9),
                'bullet_feature_11': self._bullet_feature_X(item, 10),
                'bullet_feature_12': self._bullet_feature_X(item, 11),
                'bullet_feature_13': self._bullet_feature_X(item, 12),
                'bullet_feature_14': self._bullet_feature_X(item, 13),
                'bullet_feature_15': self._bullet_feature_X(item, 14),
                'bullet_feature_16': self._bullet_feature_X(item, 15),
                'bullet_feature_17': self._bullet_feature_X(item, 16),
                'bullet_feature_18': self._bullet_feature_X(item, 17),
                'bullet_feature_19': self._bullet_feature_X(item, 18),
                'bullet_feature_20': self._bullet_feature_X(item, 19),
                'bullet_feature_count': len(item.get('bullets').split('\n')) if item.get('bullets') else 0,
                'bullets': item.get('bullets'),
                # description and ch_description deprecated, renamed to match UI field naming
                'description': item.get('short_description'),
                'description_len': len(item.get('short_description', '')),
                'details': item.get('details'),
                'directions': item.get('directions'),
                'features': item.get('features', []),
                'feature_count': len(item.get('features', [])),
                'gtin': item.get('gtin'),
                'ingredients': item.get('ingredients', []),
                'ingredient_count': len(item.get('ingredients', [])),
                'long_description': item.get('long_description'),
                'long_description_len': len(item.get('long_description', '')),
                'model': item.get('model'),
                'model_meta': next((
                    Selector(meta_tag).xpath('//meta[@itemprop="model"]/@content').extract_first()
                    for meta_tag in item.get('meta', {}).get('meta_tags', [])
                    if Selector(meta_tag).xpath('//meta[@itemprop="model"]/@content').extract_first()
                ), None),
                'mta': item.get('mta'),
                'no_longer_available': 1 if item.get('no_longer_available') else 0,
                'nutrition_fact_count': item.get('nutrition_fact_count') or 0,
                'nutrition_fact_text_health': item.get('nutrition_fact_text_health') or None,
                'product_name': item.get('title'),
                'product_title': item.get('title'),
                'rich_content': 1 if item.get('rich_content') else 0,
                'shelf_description': item.get('shelf_description'),
                'shelf_description_len': len(item.get('shelf_description', '')),
                'shipping': int(item['shipping']) if item.get('shipping') is not None else None,
                'shipping_speed': item.get('shipping_speed'),
                'specs': item.get('specs'),
                'temporary_unavailable': 1 if item.get('temporary_unavailable') else 0,
                'title_seo': item.get('meta', {}).get('browser_title'),
                'title_len': len(item.get('title', '')),
                'ugc': item.get('ugc'),
                'upc': item.get('upc'),
                'warnings': item.get('warnings'),
                'wupc': item.get('wupc'),
            },
            'proxy_service': None,  # TODO: Add this field
            # Now those two are must-have fields for SC, instead of old "Price" object
            'price_amount': item.get('price_amount') or self._price_amount(item),
            'price_currency': item.get('price_currency', 'USD'),
            # this field includes current price and original price of item
            'was_now': "{now}, {was}".format(now=item.get('now_price'), was=item.get('was_price'))
            if item.get('now_price') and item.get('was_price') else None,
            'reviews': {
                'average_review': item.get('buyer_reviews', {}).get('average') or None,
                'max_review': max([
                    s for s, v in item.get('buyer_reviews', {}).get('stars', {}).items() if v
                ] or [None]),
                'min_review': min([
                    s for s, v in item.get('buyer_reviews', {}).get('stars', {}).items() if v
                ] or [None]),
                'review_count': item.get('buyer_reviews', {}).get('count', 0),
                'reviews': [
                    [ star, value ]
                    for star, value in sorted(item.get('buyer_reviews', {}).get('stars', {}).items(), reverse=True)
                ] if any(item.get('buyer_reviews', {}).get('stars', {}).values()) else None,
            },
            'scraper': 'Walmart v2',
            'sellers': {
                # If item isn't shippable, we label it as OOS = True for SC, but CH should return opposite (CON-40734)
                'in_stock': int(item.get('in_stock')) if item.get('in_stock') is not None else (0 if item.get('is_out_of_stock') else 1),
                'in_stores': int(item['in_stores']) if item.get('in_stores') is not None else None,
                'in_stores_in_stock': self._in_stores_in_stock(item),
                'in_stores_only': self._in_stores_only(item),
                'in_stores_out_of_stock': item.get('in_stores_out_of_stock'),
                'marketplace': 1 if item.get('marketplace_bool') or item.get('_ch_marketplace') else 0,
                'marketplace_in_stock': self._marketplace_in_stock(item),
                'marketplace_lowest_price': self._marketplace_lowest_price(item),
                'marketplace_out_of_stock': self._marketplace_out_of_stock(item),
                'marketplace_prices': [
                    marketplace.get('price')
                    for marketplace in item.get('_ch_marketplace') or []
                    if marketplace.get('price')
                ],
                'marketplace_sellers': [
                    marketplace.get('name')
                    for marketplace in item.get('_ch_marketplace') or []
                    if marketplace.get('name')
                ],
                'online_only': self._online_only(item),
                'owned': self._owned(item),
                'price': item.get('price') or self._price(item),
                # Use price amount derived from price by default for CH, since price_amount may differ for SC (CON-41778)
                'price_amount': self._price_amount(item) or item.get('price_amount'),
                'price_currency': item.get('price_currency', 'USD'),
                'primary_seller': item.get('primary_seller'),
                'seller_id': item.get('seller_id'),
                'site_online': int(item['site_online']) if item.get('site_online') is not None else None,
                'site_online_in_stock': self._site_online_in_stock(item),
                'site_online_out_of_stock': item.get('site_online_out_of_stock'),
                'subscribe_discount': item.get('subscribe_discount'),
                'subscribe_price': item.get('subscribe_price'),
                'temp_price_cut': item.get('temp_price_cut'),
                'us_seller_id': item.get('us_seller_id'),
            },
            # Warning, order is important, this should only affect SC field, not CH in_stock field
            # We set all products that aren't shippable to OOS, so for those products
            # CH field should return OOS = False, but SC should return OOS = True
            'is_out_of_stock': True if item.get('shipping') is False else item.get("is_out_of_stock"),
            'site_version': 2,
            'status': 'success' if item.get('_response_code') else None,
            'status_code': item.get('_response_code'),
            'walmart_no': item.get('walmart_no'),
        }

        if item.get('search_term') and item.get('title'):
            (
                itemdict['search_term_in_title_exactly'],
                itemdict['search_term_in_title_partial'],
                itemdict['search_term_in_title_interleaved']
            ) = self.search_term_in_title(
                item.get('search_term'),
                item.get('title')
            )

        if item.get('invalid_url'):
            status = 'failure'
            failure_type = 'Invalid url'
        elif item.get('not_found'):
            status = 'failure'
            failure_type = '404'
        elif item.get('redirect'):
            status = 'failure'
            failure_type = 'Redirect'
        elif item.get('temporary_unavailable'):
            status = 'success'
            failure_type = 'Temporary unavailable'
        else:
            status = 'success'
            failure_type = None

        compatibility_item_dict.update({
            'failure_type': failure_type,
            'status': status
        })
        itemdict.update(compatibility_item_dict)
        return itemdict

    def export_item(self, item):
        itemdict = self.make_compatible(item)
        data = self.encoder.encode(itemdict) + '\n'
        self.file.write(to_bytes(data, self.encoding))
        return itemdict
