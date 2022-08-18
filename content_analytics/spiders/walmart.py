import re
import json
import traceback
from dateutil.parser import parse
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from urllib import quote, unquote
from urlparse import urljoin, urlsplit

import lxml
from lxml import html
from PIL import ImageFile
from scrapy.http import Response
from scrapy.item import Field
from six import string_types
from HTMLParser import HTMLParser

from collections import Iterable
from content_analytics.middlewares.cache import CacheContext, TTL_NEVER_EXPIRE
from content_analytics.items import BuyerReviews, HTags, Meta, SiteProductItem
from content_analytics.exporters import Price
from content_analytics.spiders import BaseProductsSpider, MergeRequest
from content_analytics.utils import catch_json_exceptions, cond_set_value, deep_search, replace_http_with_https, \
    parse_all_webcollage, catch_dictionary_exception


class WalmartProductItem(SiteProductItem):
    wupc = Field()
    bestseller_rank = Field()               # (Int) Bestseller rank in top category
    bestseller_ranks = Field()              # Bestseller ranks in categories
    pickup_today = Field()
    price_with_discount = Field()           # (float) price with pickup discount
    recent_questions = Field()
    in_store_pickup = Field()
    save_amount = Field()                   # (float) amount saved - difference between highest and lowest prices
    is_sponsored_product = Field()          # (bool) product labeled as "Sponsored" on search page
    is_best_seller_product = Field()        # (bool) product labeled as "Best Seller" on search page
    is_new_product = Field()                # (bool) product labeled as "New Product" on search page
    is_catapult_product = Field()           # (bool) product labeled as "Catapult" on search page
    price_details_in_cart = Field()         # (bool) price is available only after you put the product in the cart
    is_pickup_only = Field()                # (bool) product labeled as "This item is only sold at a Walmart store"
    is_add_to_cart = Field()                # (bool) product with "add to cart" button
    questions_total = Field()


class WalmartProductsSpider(BaseProductsSpider):
    # pylint: disable=W0221
    # TODO rework start_requests method and remove this escape
    name = 'walmart_products'
    allowed_domains = ['walmart.com']

    custom_settings = {
        'RETRY_HTTP_CODES': [500, 504, 521, 400, 403, 408, 429],
        'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
                      ' (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'
    }

    # Constants
    WALMART_SELLER_ID = 'F55CDC31AB754BB68FE0B39041159D63'
    DEFAULT_STORE = '5260'
    # Not used, just for reference
    DEFAULT_ZIP_CODE = '72758'

    # Urls
    SEARCH_URL = 'https://www.walmart.com/search/api/preso?prg=desktop&cat_id=0&po=1&query={search_term}&page={page}'
    TERRA_API_URL = 'https://www.walmart.com/terra-firma/item/{product_id}'
    QUESTIONS_ANSWERS_URL = 'https://www.walmart.com/terra-firma/fetch?rgs=QUESTIONS_MAP'  # POST method
    REVIEWS_URL = 'https://www.walmart.com/terra-firma/fetch?rgs=REVIEWS_MAP'
    SEARCH_TERM_SCREENSHOT_URL = 'https://www.walmart.com/search/?query={search_term}'

    def __init__(self, zip_code=None, store=None, scrape_questions=False, summary=False, username=None, *args, **kwargs):
        super(WalmartProductsSpider, self).__init__(*args, **kwargs)
        # TODO implement this for all optional arguments in corresponding component of base class
        self.scrape_questions = scrape_questions in ('1', 1, True, 'true', 'True')
        self.summary = summary in ('1', 1, True, 'true', 'True')
        self.zip_code = zip_code
        self.store = store
        self.username = username

    # ############################
    # Requests generator methods #
    # ############################

    def start_requests(self):
        for request in super(WalmartProductsSpider, self).start_requests():
            request = request.replace(cookies={'PSID': self.store or self.DEFAULT_STORE})
            with CacheContext(request, date=self.crawl_date) as cached_request:
                yield cached_request

    def make_single_product_requests(self, url, *args, **kwargs):
        request = next(super(WalmartProductsSpider, self).make_single_product_requests(url, *args, **kwargs))
        request = request.replace(url=replace_http_with_https(request.url))
        request.meta['handle_httpstatus_list'] = [404, 520]
        with CacheContext(request, date=self.crawl_date) as cached_request:
            yield cached_request

    def make_search_term_requests(self, search_term, *args, **kwargs):
        request = next(super(WalmartProductsSpider, self).make_search_term_requests(search_term, *args, **kwargs))
        request.meta['handle_httpstatus_list'] = [404, 520]
        with CacheContext(request, date=self.crawl_date) as cached_request:
            yield cached_request

    def make_shelf_page_requests(self, url, *args, **kwargs):
        request = next(super(WalmartProductsSpider, self).make_shelf_page_requests(url, *args, **kwargs))
        request.meta['handle_httpstatus_list'] = [404, 520]
        with CacheContext(request, date=self.crawl_date) as cached_request:
            yield cached_request

    ###############################
    # Response validation methods #
    ###############################

    @staticmethod
    def is_valid_url(url):
        # Return True if url is a valid product url, False otherwise
        match = re.match(r'(?:https?://)?(?:www\.)?walmart\.com/(\w+)/([\w-]*/)?\d+', url.lower())
        if match:
            # valid url formats include ip/co/nco/col
            return True
        return False

    @staticmethod
    def is_redirect(response):
        # Return True if the request has been redirected to a url with a different product id, else False
        redirect_urls = response.meta.get('redirect_urls')
        if redirect_urls:
            prod_id = urlsplit(redirect_urls[0]).path.split('/')[-1]
            redirected_prod_id = urlsplit(redirect_urls[-1]).path.split('/')[-1]
            return prod_id != redirected_prod_id

    def check_for_invalid_response(self, response):
        product = response.meta.get('item')

        # TODO: make URL validation before request
        if not self.is_valid_url(response.url):
            cond_set_value(product, 'invalid_url', True)
            return True
        if self.is_redirect(response):
            cond_set_value(product, 'redirect', True)
            return True
        if response.status == 520:
            cond_set_value(product, 'temporary_unavailable', True)
            return True
        if response.status == 404 or not self._parse_inline_json(response):
            cond_set_value(product, 'not_found', True)
            return True
        return False

    ############################
    # General abstract methods #
    ############################

    def get_default_item(self, *args, **kwargs):
        return WalmartProductItem()

    def parse_product(self, response):
        def _parse_terra(response_or_failure):
            try:
                assert isinstance(response_or_failure, Response)
                terra_data = json.loads(response_or_failure.body).get('payload')
            except AssertionError:
                self.logger.warning('Error while retrieving Terra-Firma API data {}'.format(str(response_or_failure)))
            except ValueError:
                self.logger.warning('Error while parsing Terra-Firma API data {}'.format(traceback.format_exc()))
            except Exception:
                self.logger.error(
                    'Unknown error while parsing and retrieving Terra-Firma API data {}'.format(traceback.format_exc())
                )
            else:
                product_json_obj.setdefault('terra', terra_data)
            return self._parse_product(response, product, product_json_obj)

        product = response.meta.get('item')

        if self.check_for_invalid_response(response):
            yield product
            return

        product_json_obj = self._parse_inline_json(response)
        item_id = self._parse_site_product_id(product['url'])
        product_id = self._parse_sku(product_json_obj, item_id=item_id)
        if product_id:
            request = MergeRequest(
                url=self.TERRA_API_URL.format(product_id=product_id),
                item=product,
                callback=_parse_terra,
                errback=_parse_terra
            )
            with CacheContext(request, date=self.crawl_date) as cached_request:
                yield cached_request
            if self.scrape_questions:
                request = MergeRequest(
                    url=self.QUESTIONS_ANSWERS_URL,
                    method='POST',
                    item=product,
                    callback=self._parse_questions_answers_first_page,
                    body=self._compile_questions_data(product_id, 1),
                    headers={
                        'Content-Type': 'application/json',
                        'Accept': '*/*'
                    }
                )
                request.meta['product_id'] = product_id
                with CacheContext(request, date=self.crawl_date) as cached_request:
                    yield cached_request
        else:
            self.logger.warning('Could not parse product id to retrieve Terra-Firma API data')
            for r in self._parse_product(response, product, product_json_obj):
                yield r

    # ##############################
    # Search term abstract methods #
    # ##############################

    def get_search_term_url(self, search_term, page=1):
        # pylint: disable=W0221
        return self.SEARCH_URL.format(search_term=quote(search_term), page=page)

    def parse_search_term_results_per_page(self, response):
        try:
            data = json.loads(response.body)
        except Exception:
            self.logger.error('Error while parsing JSON data: {}'.format(traceback.format_exc()))
        else:
            return data.get('requestContext', {}).get('itemCount', {}).get('currentSize')

    def parse_search_term_total_matches(self, response):
        try:
            data = json.loads(response.body)
        except Exception:
            self.logger.error('Error while parsing JSON data: {}'.format(traceback.format_exc()))
        else:
            return data.get('requestContext', {}).get('itemCount', {}).get('total')

    def parse_search_term_items(self, response):
        try:
            data = json.loads(response.body)
        except Exception:
            self.logger.error('Error while parsing JSON data: {}'.format(traceback.format_exc()))
            return []
        else:
            featured_product_id = data.get('featuredItem', {}).get('productId')

            results_list = []
            for item in data.get('items', []):
                url = item.get('productPageUrl')
                if url:
                    product = WalmartProductItem(is_sponsored_product=bool(item.get('wpa')))

                    is_product_featured = featured_product_id == item.get('productId')
                    product['is_catapult_product'] = is_product_featured

                    special_offer = item.get('specialOfferBadge')
                    if special_offer == "bestseller":
                        product['is_best_seller_product'] = True
                    elif special_offer == "new":
                        product['is_new_product'] = True
                    results_list.append((urljoin(response.url, url), product))
            return results_list

    def get_search_term_next_page(self, response):
        try:
            data = json.loads(response.body)
        except Exception:
            self.logger.error('Error while parsing JSON data: {}'.format(traceback.format_exc()))
        else:
            if data.get('pagination', {}).get('next'):
                current_page = re.search(r'page=(\d+)', response.url)
                current_page = int(current_page.group(1)) if current_page else 1

                return self.get_search_term_url(response.meta['search_term'], current_page + 1)

    # ##############################
    # Shelf pages abstract methods #
    # ##############################
    def _get_shelf_data(self, response):
        data = self._parse_inline_json(response)
        if data:
            # CON-43722 Walmart has couple types of shelf pages. Some for a category, some for the dedicated brand.
            is_brand_shelf = '/brand/' in response.url
            data_key = 'topicData' if is_brand_shelf else 'preso'
            data = data.get(data_key, {})

            if data.get('pageType') == 'Mashup':
                merged_topics_data = {'items': []}
                for topic in data.get('mashupTopics', []):
                    merged_topics_data['items'].extend(topic.get('items', []))
                return merged_topics_data

            return data
        return {}

    def parse_shelf_page_results_per_page(self, response):
        return self._get_shelf_data(response).get('requestContext', {}).get('itemCount', {}).get('pageSize')

    def parse_shelf_page_total_matches(self, response):
        return self._get_shelf_data(response).get('requestContext', {}).get('itemCount', {}).get('total')

    def parse_shelf_page_items(self, response):
        return [
            urljoin(response.url, item.get('productPageUrl'))
            for item in self._get_shelf_data(response).get('items', [])
            if item.get('productPageUrl')
        ]

    def get_shelf_page_next_page(self, response):
        data = self._parse_inline_json(response)
        if data:
            next_url = data.get('preso', {}).get('pagination', {}).get('next', {}).get('url')
            if next_url:
                return urljoin(response.url, '?' + next_url)

    # #################
    # Parsing methods #
    # #################

    @staticmethod
    def _parse_inline_json(response, ids_keys=('atf-content', 'content')):
        """This method should be able to extract both product links from shelf
         page response json and product json from different page layouts
         Order of atf -> content is important
         reviews only exist in btf-content for some reason, but there is no
         variants data in there
        :param response:
        :return: json data
        """
        _JS_DATA_RE = re.compile(
            r'window\.__WML_REDUX_INITIAL_STATE__\s*=\s*(\{.+?\})(\s*;\s*})?\s*;\s*<\/script>', re.DOTALL)
        raw_data = re.search(_JS_DATA_RE, response.body)
        data = json.loads(raw_data.group(1), encoding='utf-8') if raw_data else {}
        if not data.get('product', {}).get('selected', {}).get('product'):
            for key in reversed(ids_keys):
                raw_data = response.xpath('//script[@id="{}"]/text()'.format(key)).extract_first()
                if raw_data:
                    data = json.loads(raw_data)[key]

        return data

    def _parse_product(self, response, product, data):
        # parse site_product_id
        site_product_id = self._parse_site_product_id(product.get('url'))
        cond_set_value(product, 'site_product_id', site_product_id)

        # parse product id
        sku = self._parse_sku(data, item_id=site_product_id)
        cond_set_value(product, 'sku', sku)

        is_collection = False

        product_url = getattr(self, 'product_url', None)
        if product_url:
            product['url'] = product_url

            if '/col/' in product['url']:
                is_collection = True

        # parse selected product
        selected_product = self._parse_selected_product(data, sku)

        # parse bundle
        bundle = self._parse_bundle(selected_product)
        cond_set_value(product, 'bundle', bundle)

        # parse offers
        offers = self._parse_offers(data, selected_product)

        # get store id
        store = self._parse_store(offers)
        cond_set_value(product, 'store', store)

        # parse variant offers (including out of stock offers, to get prices for out of stock variants)
        variant_offers = self._parse_offers(data, selected_product, include_oos=True)

        # parse title
        title = self._parse_title(selected_product)
        cond_set_value(product, 'title', title)

        # parse shipping
        shipping = self._parse_shipping(offers)
        cond_set_value(product, 'shipping', shipping)

        # parse shipping
        shipping_speed = self._parse_shipping_speed(response)
        cond_set_value(product, 'shipping_speed', shipping_speed)

        # parse wupc
        wupc = self._parse_wupc(selected_product)
        cond_set_value(product, 'wupc', wupc)

        # parse brand
        brand = self._parse_brand(selected_product)
        cond_set_value(product, 'brand', brand)

        # parse price
        (price, temp_price_cut) = self._parse_price(data, offers, selected_product, bundle, True)
        cond_set_value(product, 'price', price)
        cond_set_value(product, 'price_currency', 'USD')
        cond_set_value(product, 'temp_price_cut', temp_price_cut)

        # parse is out of stock
        # Warning: this field may be set as True if item isn't shippable - see exporters.py
        is_out_of_stock = self._parse_is_out_of_stock(offers)
        cond_set_value(product, 'is_out_of_stock', is_out_of_stock)

        # "Summary" mode - only need few fields for realtime / FMR(first mover report) tasks
        if getattr(self, "summary", None):
            yield product
            return

        # parse primary_seller, seller_id, and us_seller_id
        if offers:
            sellers = data.get('terra', {}).get('sellers', {})
            if not sellers:
                self.logger.info('Could not retrieve sellers from Terra-Firma API. Falling back to default.')
                sellers = data.get('product', {}).get('sellers', {})

            seller_id = offers[0].get('sellerId') or self.WALMART_SELLER_ID
            if seller_id:
                primary_seller = sellers.get(seller_id, {})
                cond_set_value(product, 'primary_seller', primary_seller.get('sellerDisplayName'))
                cond_set_value(product, 'seller_id', primary_seller.get('sellerId'))
                cond_set_value(product, 'us_seller_id', primary_seller.get('catalogSellerId'))

        # parse low stock
        low_stock = self._parse_low_stock(data)
        cond_set_value(product, 'low_stock', low_stock)

        # parse shelf_description (shortest of them)
        if is_collection:
            shelf_description = data.get('collectionMap', {}).get('collectionProduct', {}).get('shortDescription')
        else:
            shelf_description = self._parse_shelf_description(selected_product)
        cond_set_value(product, 'shelf_description', shelf_description)

        short_description, long_description = self._parse_short_long_description(data, selected_product, title)
        cond_set_value(product, 'short_description', short_description)
        cond_set_value(product, 'long_description', long_description)

        if is_collection:
            long_description = data.get('collectionMap', {}).get('collectionProduct', {}).get('longDescription')
            cond_set_value(product, 'long_description', self._clean_up_html(long_description))

            # parse collection count
            collection_count = len(data.get('productData', {}).get('products', {}))
            cond_set_value(product, 'collection_count', collection_count)

            # parse collection availability
            collection_availability = self._parse_collection_availability(data)
            cond_set_value(product, 'collection_availability', collection_availability)

        # parse departments
        departments = self._parse_departments(selected_product)
        cond_set_value(product, 'departments', departments)

        # parse bestseller ranks
        bestseller_ranks = self._parse_bestseller_ranks(data, sku)
        cond_set_value(product, 'bestseller_ranks', bestseller_ranks)
        cond_set_value(product, 'bestseller_rank', bestseller_ranks[0][0] if bestseller_ranks else None)

        # parse department
        department = self._parse_department(selected_product, departments)
        cond_set_value(product, 'department', department)

        # parse site online
        site_online = self._parse_site_online(offers)
        cond_set_value(product, 'site_online', site_online)

        # parse is no longer available
        no_longer_available = self._parse_no_longer_available(site_online, price, offers)
        cond_set_value(product, 'no_longer_available', no_longer_available)

        # parse site online out of stock
        site_online_out_of_stock = self._parse_site_online_out_of_stock(no_longer_available, offers)
        cond_set_value(product, 'site_online_out_of_stock', site_online_out_of_stock)

        # parse in stores
        in_stores = self._parse_in_stores(offers)
        cond_set_value(product, 'in_stores', in_stores)

        # parse in stores out of stock
        in_stores_out_of_stock = self._parse_in_stores_out_of_stock(no_longer_available, offers)
        cond_set_value(product, 'in_stores_out_of_stock', in_stores_out_of_stock)

        # Parse in-store pickup
        in_store_pickup = False if is_out_of_stock else self._parse_in_store_pickup(data)
        cond_set_value(product, 'in_store_pickup', in_store_pickup)

        # parse is_pickup_only
        is_pickup_only = self._parse_is_pickup_only(offers)
        cond_set_value(product, 'is_pickup_only', is_pickup_only)

        # parse is in store only, all products that are pickup-only are in-store only as well
        if is_pickup_only:
            is_in_store_only = True
        else:
            is_in_store_only = self._parse_is_in_store_only(data, offers)
        cond_set_value(product, 'is_in_store_only', is_in_store_only)

        # parse images and dimensions from terra-firma API
        image_urls, image_res, image_dimensions, zoom_image_dimensions = \
                self._parse_image_urls(data, selected_product, bundle, is_collection)

        cond_set_value(product, 'image_urls', image_urls)
        cond_set_value(product, 'image_url', image_urls[0] if image_urls else None)
        cond_set_value(product, 'image_res', image_res)
        cond_set_value(product, 'image_dimensions', image_dimensions)
        cond_set_value(product, 'zoom_image_dimensions', zoom_image_dimensions)

        # image alts
        if is_collection:
            image_alts = self._parse_collection_image_alts(data)
        else:
            image_alts = [title] * len(image_urls)
        cond_set_value(product, 'image_alts', image_alts)

        # parse videos
        video_urls = self._parse_video_urls(data, sku)
        cond_set_value(product, 'video_urls', video_urls)
        cond_set_value(product, 'video_count', len(video_urls))

        # parse meta
        meta_field = self._parse_meta(response)
        cond_set_value(product, 'meta', meta_field)

        # parse htags
        htags = self._parse_htags(response)
        cond_set_value(product, 'htags', htags)

        # parse specs
        specs = self._parse_specs(data)
        cond_set_value(product, 'specs', specs)

        # parse model
        model = self._parse_model(specs)
        cond_set_value(product, 'model', model)

        # parse directions
        directions = self._parse_directions(data, sku, selected_product)
        cond_set_value(product, 'directions', directions)

        # parse ingredients
        ingredients = self._parse_ingredients(selected_product)
        cond_set_value(product, 'ingredients', ingredients)

        # parse warnings
        warnings = self._parse_warnings(data, sku, selected_product)
        cond_set_value(product, 'warnings', warnings)

        # parse swatches
        swatches = self._parse_swatches(data)
        cond_set_value(product, 'swatches', swatches)

        # parse variants
        # TODO consider replacing variant parsing method with current SC method
        variants = self._parse_variants(data, sku, variant_offers)
        cond_set_value(product, 'variants', variants)

        # For SC, price amount is determined by selected variant
        for variant in variants or []:
            if variant.get('selected') and variant.get('price'):
                cond_set_value(product, 'price_amount', variant['price'])
                break

        # parse if "add to cart" button present on the page
        cond_set_value(
            product,
            'is_add_to_cart',
            self._parse_is_add_to_cart(response)
        )

        # get upc from variant that corresponds to the product url,
        # as that is the correct value rather than upc from selected_product
        if variants:
            for variant in variants:
                if site_product_id in variant['url']:
                    cond_set_value(product, 'upc', variant['upc'])
                    break

        if not product.get('upc'):
            gtin = self._parse_gtin(selected_product)
            cond_set_value(product, 'gtin', gtin)

            if gtin:
                upc = gtin.lstrip('0')[-12:].zfill(12)
                cond_set_value(product, 'upc', upc)

        # parse marketplace (CH)
        ch_marketplace = self._parse_marketplace(data, offers)
        cond_set_value(product, '_ch_marketplace', ch_marketplace)

        # parse marketplaces (SC)
        marketplace_offers = self._parse_sc_offers(data, sku)
        marketplace_sellers = self._parse_sc_sellers(data)
        sc_marketplace = self._parse_sc_marketplaces(marketplace_offers, marketplace_sellers)
        cond_set_value(product, 'marketplace', sc_marketplace)

        is_rollback = self._parse_rollback(marketplace_offers)
        cond_set_value(product, 'special_pricing', is_rollback)

        # parse price details in cart
        price_details_in_cart = self._parse_price_details_in_cart(sc_marketplace)
        cond_set_value(product, 'price_details_in_cart', price_details_in_cart)

        # parse buyer_reviews
        request_reviews_from_api = False
        buyer_reviews = {}
        try:
            buyer_reviews = self._parse_buyer_reviews(data, sku)
            if not self._are_reviews_parsed(buyer_reviews):
                # sometimes json data only exists in btf-content block
                buyer_reviews = self._parse_buyer_reviews(self._parse_inline_json(response,
                                                                                  ids_keys=['btf-content']), sku)
            cond_set_value(product, 'buyer_reviews', buyer_reviews)
        except KeyError:
            # Request reviews from api if sku is not present in reviews (CON-43957)
            request_reviews_from_api = True

        # Rare (~1 in 100-200) case when no reviews data on page due to a Walmart-side bug
        # Fixed assuming typical error layout:
        # reviews_error_dict = {u'heroMedia': {},
        # u'showMediaModal': False,
        # u'selectedReviewMediaId': u''}
        # ERROR_DICT_SIZE = 3
        # See CON-40892 and CON-39643
        if len(data.get('product', {}).get('reviews', {})) == 3:
            # Request reviews from Walmart API
            request_reviews_from_api = True

        if not self._are_reviews_parsed(buyer_reviews) or request_reviews_from_api:
            are_reviews_present = response.xpath('//*[contains(@class, "ReviewsHeader-seeAll")]//text()').extract()
            if are_reviews_present:
                response.meta['reviews_api_retries_left'] = 5
                request = MergeRequest(**self._prepare_review_request_args(response.meta))
                with CacheContext(request, date=self.crawl_date) as cached_request:
                    yield cached_request

        # parse pickup today status
        pickup_today = self._parse_pickup_today(offers)
        cond_set_value(product, 'pickup_today', pickup_today)

        # parse discount price
        price_with_discount = self._parse_price_with_discount(offers)
        cond_set_value(product, 'price_with_discount', price_with_discount)

        # parse save_amount
        save_amount = self._parse_save_amount(offers)
        cond_set_value(product, 'save_amount', save_amount)

        # parse was_price
        was_price = self._parse_was_price(offers)
        cond_set_value(product, 'was_price', was_price)

        # parse now_price
        now_price = self._parse_now_price(offers)
        cond_set_value(product, 'now_price', now_price)

        # parse list_price
        list_price = self._parse_list_price(offers)
        cond_set_value(product, 'list_price', list_price)

        # parse nutrition facts
        nutrition_facts = self._parse_nutrition_facts(data)
        cond_set_value(product, 'nutrition_fact_count', nutrition_facts, len)

        # parse nutrition fact text health
        if data.get('terra'):
            cond_set_value(product, 'nutrition_fact_text_health', int(bool(nutrition_facts)))
        else:
            cond_set_value(product, 'nutrition_fact_text_health', 2)

        # parse image dimensions if they weren't extracted previously (only do this for single-url crawls)
        if not image_dimensions and image_urls and self._product_url:
            cond_set_value(product, 'image_res', [None] * len(image_urls))
            cond_set_value(product, 'image_dimensions', [None] * len(image_urls))
            for i, image_url in enumerate(image_urls):
                request = MergeRequest(
                    url=image_url,
                    item=product,
                    callback=self._parse_image_dimensions,
                    meta={
                        'index': i,
                        'max_index': len(image_urls)
                    },
                    dont_filter=True
                )
                with CacheContext(request, ttl=TTL_NEVER_EXPIRE) as cached_request:
                    yield cached_request

        # parse walmart_no
        walmart_no = selected_product.get('productAttributes', {}).get('walmartItemNumber')
        cond_set_value(product, 'walmart_no', walmart_no)

        # parse sellpoints
        sellpoints = bool(deep_search('SellPointsMarketingContent', data))
        cond_set_value(product, 'sellpoints', sellpoints)

        # parse webcollage
        wc_page_contents, webcollage_360 = self._parse_webcollage_contents(data)

        if webcollage_360:
            wc_360 = self._parse_webcollage_360(webcollage_360)
            cond_set_value(product, 'wc_360', wc_360)

        pdf_urls = self._parse_webcollage_pdf(data)
        if pdf_urls:
            cond_set_value(product, 'pdf_urls', pdf_urls)
            cond_set_value(product, 'wc_pdf', bool(pdf_urls))
            cond_set_value(product, 'webcollage_pdfs_count', len(pdf_urls))

        parse_all_webcollage(wc_page_contents, product)

        # parse rich content
        if wc_page_contents and ('richcontext' in html.tostring(wc_page_contents) or
                'contentanalytics' in html.tostring(wc_page_contents)):
            cond_set_value(product, 'rich_content', True)

        if self.shelf_url and walmart_no:
            cond_set_value(product, 'secondary_id', walmart_no)

        # parse questions total
        questions_total = self._parse_questions_total(data, sku)
        cond_set_value(product, 'questions_total', questions_total)

        yield product

    @staticmethod
    def _parse_sku(data, item_id=None):
        """Get current product id from data.

        item_id was added for CON-42940 issue
        for cases when `selected` product in general json data
        not the one which is associated with URL
        for example https://walmart.com/ip/27280854 associated with `Size 6, 18 ct` variant
        but `Size 6, 144 ct` will be selected in json data (as best value)

        :param data (dict): general json `data`
        :param item_id (str, optional): `site_product_id` from `_parse_site_product_id` method
        :return (str): product id
        """
        if item_id:
            products = data.get('product', {}).get('products', {})
            for product in products.values():
                if isinstance(product, dict) and product.get('usItemId') == item_id:
                    return product.get('productId')
        selected = data.get('product', {}).get('selected', {})
        selected_product_id = selected.get('lastSuccessfullyFetchedProduct')
        if selected.get('status') == 'FETCHED':
            selected_product_id = selected.get('product') or selected_product_id
        return selected_product_id

    @staticmethod
    def _parse_selected_product(data, sku):
        """Get selected product section from data

        :param data (dict): general json `data`
        :param sku (str): product id from `_parse_sku` method
        :return (dict): selected product section
        """
        selected_product = data.get('product', {}).get('products', {}).get(sku, {})

        # for bundle pages
        if not selected_product:
            selected_product = data.get('product', {}).get('primaryProduct', {})

        return selected_product

    @staticmethod
    def _parse_bundle(selected_product):
        product_attributes = selected_product.get('productAttributes', {})
        if not isinstance(product_attributes, dict):
            return False
        if product_attributes.get('classType') == 'BUNDLE' and \
                not selected_product.get('bundleType') == 'INFLEXIBLE_KIT':
            return True
        return False

    @staticmethod
    def _is_in_stock(offer):
        return offer.get('productAvailability', {}).get('availabilityStatus') not in ['OUT_OF_STOCK', 'RETIRED']

    def _parse_offers(self, data, selected_product, include_oos=False):
        offers = data.get('terra', {}).get('offers', {}).values()

        if not offers:
            self.logger.info('Could not retrieve offers from Terra-Firma API. Falling back to default.')
            offers = data.get('product', {}).get('offers', {}).values()

        # get offers order
        offers_order = data.get('offersOrder', {}).get(selected_product.get('productId'))
        if offers_order:
            offers_order = [o.get('id') for o in offers_order if o]

            # sort offers by order in offers_order
            offers = sorted(offers, key=lambda o: offers_order.index(o.get('id')) if o.get('id') in offers_order else len(offers_order))

        filtered_offers = []

        # filter out empty offers
        offers = filter(None, offers)

        online_offers = [o for o in offers if 'ONLINE' in o.get('offerInfo', {}).get('offerType', '')]
        in_stock_offers = [o for o in offers if self._is_in_stock(o)]
        in_stock_online_offers = [o for o in offers if o in online_offers and o in in_stock_offers]

        invalid_offer_types = ['NON_TRANSACTABLE_STORE_ONLY', 'DISPLAY_ONLY']
        valid_offers = [o for o in offers if o.get('offerInfo', {}).get('offerType') not in invalid_offer_types]
        for offer in offers:
            # if it's a marketplace offer
            if offer.get('sellerId') and offer.get('sellerId') != self.WALMART_SELLER_ID:
                midas_price = data.get('product', {}).get('midasContext', {}).get('price')

                # if there is no midas price, don't include it (because product is actually INLA)
                if not midas_price:
                    continue

                # if it's not in stock
                if not self._is_in_stock(offer):
                    price = offer.get('pricesInfo', {}).get('priceMap', {}).get('CURRENT', {}).get('price')

                    # only include it if there are no in stock offers, it is online,
                    #  and its price matches the midas price and there are no store only offers
                    if not in_stock_offers and offer in online_offers and price == midas_price:
                        filtered_offers.append(offer)
                        # then break
                        break

                    continue

            # if it's not in stock
            if not self._is_in_stock(offer) and not include_oos:
                # if there are other in stock, online offers, don't include it
                if in_stock_online_offers:
                    continue

            offer_type = offer.get('offerInfo', {}).get('offerType')

            # if it is an invalid offer, only inlcude it if there are no valid offers and it is the first offer
            if offer_type in invalid_offer_types:
                if len(valid_offers) == 0 and offer == offers[0]:
                    filtered_offers.append(offer)

            else:
                filtered_offers.append(offer)

        return filtered_offers

    @staticmethod
    def _parse_title(selected_product):
        """Get product title.

        :param selected_product (dict): selected product from method `_parse_selected_product`
        :return (str): product title
        """
        product_name = selected_product.get('productAttributes', {}).get('productName')
        if product_name:
            return re.sub(r'\s+', ' ', product_name)  # remove extra spaces

    @staticmethod
    def _is_valid_html(html_string_or_list):
        if len(html_string_or_list) and isinstance(html_string_or_list, list):
            html_string_or_list = html_string_or_list[0]
        # Checks if html string have useful text inside and isn't empty
        # sometimes it only contains pair of tags
        return bool(re.sub(r'<[^>]*>', '', html_string_or_list))

    @staticmethod
    def _clean_up_html(desc_or_list):
        # This method removes all unneeded tags from html, leaving only ones we need
        # probably those are <ul><li> tags
        if len(desc_or_list) and isinstance(desc_or_list, list):
            desc = desc_or_list[0]
        else:
            desc = desc_or_list
        # remove links
        desc = re.sub(r'<a href.*?>(.*?)</a>', r'\1', desc)
        # remove attributes
        desc = re.sub(r'(<\w+)[^>]*?(/?>)', r'\1\2', desc)
        # remove div and span tags
        desc = re.sub(r'</?div>|</?span>', '', desc)
        # remove replacement characters
        desc = re.sub(u'\ufffd', '', desc)
        # remove &nbsp;
        desc = re.sub('&nbsp;', ' ', desc)
        # unescape entities like &amp;
        desc = HTMLParser().unescape(desc)
        return desc.strip()

    def _parse_shelf_description(self, selected_product):
        # returns description, visible on the page initially
        shelf_description = selected_product.get('productAttributes', {}).get('shortDescription')
        if shelf_description and self._is_valid_html(shelf_description):
            return self._clean_up_html(shelf_description)

    def _parse_short_long_description(self, data, selected_product, title):
        # pylint: disable=I1101
        # returns pair of values for two types of descriptions
        short_description = None
        long_description = None

        # Short description
        product_short_description = deep_search('product_short_description', data)
        if product_short_description:
            s_desc = product_short_description[0].get('values')
            if s_desc and self._is_valid_html(s_desc):
                short_description = self._clean_up_html(s_desc)

        # Another try, from another part of json
        if not short_description:
            s_desc = selected_product.get('productAttributes', {}).get('mediumDescription')
            if s_desc and self._is_valid_html(s_desc):
                short_description = self._clean_up_html(s_desc)

        # Final check for valid html schema:
        if short_description:
            try:
                html.fromstring(short_description)
            except lxml.etree.ParserError:
                # Invalid html inside, do not return it
                short_description = None

            # Do not use short descriptions that end with "..."
            if short_description and short_description.endswith('...'):
                short_description = None

        # Long description
        for product in data.get('product', {}).get('idmlMap', {}).values():
            l_desc = product.get('modules', {}).get('LongDescription', {}).get('product_long_description', {}).get(
                'values')
            if l_desc and self._is_valid_html(l_desc):
                long_description = self._clean_up_html(l_desc)

        # Another try, from another part of json
        if not long_description:
            l_desc = selected_product.get('productAttributes', {}).get('detailedDescription')
            if l_desc and self._is_valid_html(l_desc):
                long_description = self._clean_up_html(l_desc)

        if long_description:
            # Final check for valid html schema:
            try:
                html.fromstring(long_description)
            except lxml.etree.ParserError:
                # Invalid html inside, do not return it
                long_description = None

            if long_description and long_description.lower() == 'long description is not available':
                long_description = None

        if long_description and long_description == title:
            if short_description:
                short_description += ' ' + long_description
            else:
                short_description = long_description
            long_description = None

        if not short_description:
            short_description = long_description
            long_description = None

        if short_description:
            separator_index = self._get_description_separator_index(short_description, title)

            if separator_index and not long_description:
                long_description = short_description[separator_index:]
                short_description = short_description[:separator_index]

        return short_description, long_description

    @staticmethod
    def _get_description_separator_index(description, title):
        """Get description separator index, to split combined description into short
            and long description according CON-24403

        :param description (str): combined short and long description
        :param title (str): from method `_parse_title`
        :return (int): index at which description should be separated, or None
        """
        product_name = title.split(',')[0]
        product_name_bold = '<b>' + product_name
        product_name_strong = '<strong>' + product_name

        has_product_name = False

        product_name_regex = r'(<b>|<strong>)[^<]*(</b>|</strong>)[(<br>)\s":]*(</p>)?(<br>)*(<ul>|<li>)'

        if product_name_bold in description or product_name_strong in description\
                or re.search(product_name_regex, description, re.DOTALL):

            has_product_name = True

        possible_end_indexes = []

        for item in [product_name_bold, product_name_strong, '<h3>', '<section class="product-about']:
            if item in description:
                possible_end_indexes.append(description.find(item))

        for item in ['<dl>', '<ul>', '<li>']:
            if not has_product_name and item in description:
                possible_end_indexes.append(description.find(item))

        if not (product_name_bold in description or product_name_strong in description):
            match = re.search(product_name_regex, description, re.DOTALL)
            if match:
                possible_end_indexes.append(match.start())

        if possible_end_indexes:
            end_index = min(possible_end_indexes)
        else:
            end_index = None

        short_description = description[:end_index]

        while len(short_description) > 1000:
            if '<p>' in short_description:
                end_index = short_description.rfind('<p>')
                short_description = description[:end_index]
            else:
                break

        return end_index

    @staticmethod
    def _parse_shipping(offers):
        """Check product shipping availability

        :param offers (list): list of product offers from method `_parse_offers`
        :return (bool): Product can be shipped or not
        """
        return any([
            offer.get('fulfillment', {}).get('shippable')
            for offer in offers
            if offer.get('fulfillment', {}).get('shippable', None) is not None
        ])

    def _parse_shipping_speed(self, response):
        """Extract product shipping speed

        :param response (HtmlResponse): general url response
        :return (str): time to arrive, e.g. '4 days'
        """
        arrival = re.search(r'Arrives by \w+?, (\w+? \d+)', response.body) or \
                re.search(r'Available \w+?, (\w+? \d+)', response.body) or \
                re.search(r'Free pickup \w+?, (\w+? \d+)', response.body)
        if arrival:
            arrival = arrival.group(1)
            try:
                return '{} days'.format(relativedelta(parse(arrival), date.today()).days)
            except:
                self.logger.warning('Error parsing shipping speed: {}'.format(traceback.format_exc()))

    @staticmethod
    def _parse_gtin(selected_product):
        """Get gtin from selected product.

        :param selected_product (dict): selected product from method `_parse_selected_product`
        :return (str): product `gtin` code
        """
        gtin = selected_product.get('upc')
        if not gtin:
            gtin = selected_product.get('productAttributes', {}).get('sku')
        # only return gtin if it only contains digits
        if isinstance(gtin, (str, unicode)):
            if re.match(r'\d+$', gtin):
                return gtin.zfill(14)

    @staticmethod
    def _parse_wupc(selected_product):
        """Get product walmart upc from selected product.

        :param selected_product (dict): selected product from method `_parse_selected_product`
        :return (str): product `wupc` code
        """
        return selected_product.get('wupc', None)

    @staticmethod
    def _parse_site_product_id(url):
        """Same as reseller id, used to identify product on front end side
        :param url: product url (str)
        :return: (str): resulting id
        """
        g = re.findall(r'/([0-9]{3,20})', url)
        if not g:
            g = url.split('?')[0].split('/')
        return g[-1] if g else None

    @staticmethod
    def _parse_brand(selected_product):
        """Get product brand from selected product.

        :param selected_product (dict): selected product from method `_parse_selected_product`
        :return (str): product `brand`
        """
        return selected_product.get('productAttributes', {}).get('brand', None)

    @staticmethod
    def _parse_departments(selected_product):
        """Get product departments from selected product.

        :param selected_product (dict): selected product from method `_parse_selected_product`
        :return (list): ordered list of product `departments`
        """
        return [
            path.get('name')
            for path
            in selected_product.get('productAttributes', {}).get('productCategory', {}).get('path', [])
            if path.get('name', None)
        ]

    @staticmethod
    def _parse_department(selected_product, departments):
        """Try to get product department from selected list or from selected product.

        :param selected_product (dict): selected product from method `_parse_selected_product`
        :param departments (list): ordered list from method `_parse_departments`
        :return (str): product `department`
        """
        if isinstance(departments, list) and departments:
            return departments[-1]
        return selected_product.get('productAttributes', {}).get('primaryShelf', None)

    @staticmethod
    def _parse_price(data, offers, product, bundle, is_main=False):
        """Try to get all possible product prices from offers or from general data.
        If not possible to get price from general data, fallback to minimal price.

        :param data (dict): general json `data`
        :param offers (list): list of product offers from method `_parse_offers`
        :param product (dict): the product you want the price of (main or variant)
        :param bundle (bool): whether this page is a bundle product
        :param is_main (bool): whether it is the main product (as opposed to variant)
        :return (tuple(str, bool)): formatted price, temp price cut
        """
        temp_price_cut = False

        if is_main:
            if bundle:
                meta_data = data.get('product', {}).get('choiceBundleMetaData', {})
                min_price = meta_data.get('minPrice', {}).get('price')
                max_price = meta_data.get('maxPrice', {}).get('price')
                if min_price and max_price:
                    return "${0} - ${1}".format(min_price, max_price), temp_price_cut

            price_ranges = data.get('product', {}).get('priceRanges')

            if price_ranges:
                price_range = price_ranges.values()[0]
                min_price = price_range['minPrices'].get('CURRENT', {}).get('price')
                max_price = price_range['maxPrices'].get('CURRENT', {}).get('price')
                if min_price and max_price:
                    return "${0} - ${1}".format(min_price, max_price), temp_price_cut

        price_maps = [
            o.get('pricesInfo', {}).get('priceMap')
            for o in offers if o.get('id') in product.get('offers', []) and o.get('pricesInfo', {}).get('priceMap')
                     ]

        if not price_maps and offers:
            offers = filter(None, data.get('product', {}).get('offers', {}).values())
            price_maps = [
                o.get('pricesInfo', {}).get('priceMap', {}) for o in offers
            ]

        if price_maps:
            price = price_maps[0].get('CURRENT', {}).get('price')
            if price_maps[0].get('WAS'):
                temp_price_cut = True
        else:
            price = data.get('product', {}).get('midasContext', {}).get('price')

        if price:
            return '${:2,.2f}'.format(price), temp_price_cut

        return None, None

    @staticmethod
    def _parse_low_stock(data):
        """Check is product low stock.

        :param data (dict): general json `data`
        :return (bool): is product low stock
        """
        condition = data.get('buyingOptions', {}).get('urgencyMessage', '')
        return bool(re.search(r'Only\s*\d+\s*left!', condition))

    @staticmethod
    def _parse_in_stores(offers):
        if offers and 'STORE' in offers[0].get('offerInfo', {}).get('offerType', ''):
            return 1
        return 0

    def _parse_in_stores_out_of_stock(self, no_longer_available, offers):
        if self._parse_in_stores(offers):
            if no_longer_available:
                return 1
            for offer in offers:
                if 'STORE' in offer.get('offerInfo', {}).get('offerType') and \
                        self._is_in_stock(offer):
                    return 0
            return 1

    def _parse_site_online(self, offers):
        for offer in offers:
            if (not offer.get('sellerId') or offer.get('sellerId') == self.WALMART_SELLER_ID) and \
                    offer.get('offerInfo', {}).get('offerType') != 'STORE_ONLY':
                return 1
        return 0

    def _parse_site_online_out_of_stock(self, no_longer_available, offers):
        if self._parse_site_online(offers):
            if no_longer_available:
                return 1
            for offer in offers:
                if 'ONLINE' in offer.get('offerInfo', {}).get('offerType') and \
                        self._is_in_stock(offer):
                    return 0
            return 1

    @staticmethod
    def _parse_is_out_of_stock(offers):
        """Check is product out of stock.

        :param data (dict): general json `data`
        :param offers (list): list of product offers from method `_parse_offers`
        :return (bool): is product out of stock
        """
        return not any([
            offer.get('productAvailability').get('availabilityStatus') == 'IN_STOCK'
            for offer in offers
            if offer.get('productAvailability', {}).get('availabilityStatus', None)
        ])

    @staticmethod
    def _parse_is_pickup_only(offers):
        return any([
            'NON_TRANSACTABLE_STORE_ONLY' in offer.get('offerInfo', {}).get('offerType', '')
            for offer in offers
            if offer.get('offerInfo', {}).get('offerType', None)
        ])

    @staticmethod
    def _parse_is_in_store_only(data, offers):
        """Check is product is available in store only. If no info in midasContext, try to get from offers.

        :param data (dict): general json `data`
        :param offers (list): list of product offers from method `_parse_offers`
        :return (bool): is product available in store only
        """
        is_in_store_only = data.get('product', {}).get('midasContext', {}).get('inStore', None)
        if is_in_store_only is not None:
            return is_in_store_only
        return not any([
            offer.get('offerInfo').get('offerType').startswith('ONLINE')
            for offer in offers
            if offer.get('offerInfo', {}).get('offerType', None)
        ])

    @staticmethod
    def _parse_no_longer_available(site_online, price, offers):
        """Check is product no longer available. If there is no offers, then no longer available.

        :param offers (list): product offers from method `_parse_offers`
        :return (bool): is product no longer available
        """
        if site_online and not price:
            return True
        return not bool(offers)

    @staticmethod
    def _parse_image_urls(data, selected_product, bundle, is_collection):
        """Get all images info.

        :param data (dict): general json `data`
        :param selected_product (dict): selected product from method `_parse_selected_product`
        :return (tuple(image_urls, image_res, image_dimensions, zoom_image_dimensions)): image urls and image info
        """
        if is_collection:
            products = data.get('productData', {}).get('products', {}).values()
            image_urls = []
            for product in products or []:
                image_url = product.get('imageUrl')
                if image_url:
                    image_urls.append(image_url)
            return image_urls, [], []

        image_keys = selected_product.get('images', [])
        images = data.get('product', {}).get('images', {})

        variants = deep_search('variants', data.get('product', {}).get('variantCategoriesMap', {}))

        if variants and len(variants) > 1:
            for variant in variants[0].values():
                if variant.get('selected') and variant.get('images'):
                    image_keys = variant['images']
                    break

        if not images:
            images = data.get('terra', {}).get('images', {})
            image_keys = images.keys()

        if bundle:
            image_urls = []
            for section in data.get('product', {}).get('sections', []):
                image_url = section['components'][0]['productImageUrl'].split('?')[0]
                if image_url not in image_urls:
                    image_urls.append(image_url)
            if image_urls:
                return [i for i in image_urls if not 'no-image' in i], [], [], []
            if data.get('product', {}).get('bundlePrimaryImage'):
                return [data['product']['bundlePrimaryImage']], [], [], []

        image_urls = []

        image_res = []
        image_dimensions = []
        zoom_image_dimensions = []

        for image_key in image_keys:
            image = images.get(image_key, {})

            if bundle and len(images) > 1 and not \
                    (image.get('rank') == 1 and image.get('type') == 'PRIMARY'):
                continue

            image_sizes = image.get('assetSizeUrls', {})

            if len(image_sizes) == 1:
                main_image_url = image_sizes.get(image_sizes.keys()[0])
            else:
                main_image_url = image_sizes.get('main') or image_sizes.get('DEFAULT')

            if main_image_url:
                image_url = main_image_url.split('?')[0]
                if image_url not in image_urls:
                    image_urls.append(image_url)

            max_width, max_height = 0, 0

            terra_image = data.get('terra', {}).get('images', {}).get(image_key, {})
            for image_size in terra_image.get('assetSizeUrls', {}).keys():
                image_size = re.match(r'IMAGE_SIZE_(\d+)_(\d+)', image_size)
                if image_size:
                    if int(image_size.group(1)) > max_width:
                        max_width, max_height = int(image_size.group(1)), int(image_size.group(2))

            image_res.append([max_width, max_height])

            if max_width >= 500 or max_height >= 500:
                image_dimensions.append(1)
            else:
                image_dimensions.append(0)

            if image_sizes.get('zoom'):
                zoom_image_dimensions.append(1)
            else:
                zoom_image_dimensions.append(0)

        return image_urls, image_res, image_dimensions, zoom_image_dimensions

    def _parse_image_dimensions(self, response):
        meta = response.meta
        idx = meta.get('index')
        max_index = meta.get('max_index')
        product = meta.get('item')

        image_res = None
        dimensions = None

        try:
            image_parser = ImageFile.Parser()
            image_parser.feed(response.body)
            if image_parser.image:
                image_size = image_parser.image.size
                image_res = image_size
                if image_size[0] >= 500 and image_size[1] >= 500:
                    dimensions = 1
                else:
                    dimensions = 0

        except Exception:
            self.logger.warning('Error while retrieving image dimension {}'.format(traceback.format_exc()))

        # If image_dimensions field is empty - fill it with 0 so we don't get race condition
        # because of [idx] assignment - this callback is processed in parallel

        if not product.get('image_res'):
            product['image_res'] = [0] * max_index
        if not product.get('image_dimensions'):
            product['image_dimensions'] = [0] * max_index

        product['image_res'][idx] = image_res
        product['image_dimensions'][idx] = dimensions

        return product

    @staticmethod
    def _parse_video_urls(data, product_id):
        """Get product videos

        :param data (dict): general json `data`
        :param product_id (str): product id from `_parse_sku` method
        :return (list): list of video urls
        """
        return [
            video.get('versions').get('LARGE')
            for video in data.get('product', {}).get('idmlMap', {}).get(product_id, {}).get('videos', [])
            if video.get('versions', {}).get('LARGE', None)
        ]

    @staticmethod
    def _parse_meta(response):
        """Get meta info from product page

        :param response (HtmlResponse): general url response
        :return (Meta): object represents meta tags from product page
        """
        charset = response.xpath(
            '//meta[@charset]'
            '/@charset'
        ).extract_first()

        canonical_url = response.xpath(
            '//link[@rel="canonical"]/@href'
        ).extract_first()

        if canonical_url:
            canonical_url = urljoin(response.url, canonical_url)

        browser_title = response.xpath(
            '//title/'
            'text()'
        ).extract_first()

        keywords = response.xpath(
            '//meta[@name="keywords"]'
            '/@content'
        ).extract_first()

        description = response.xpath(
            '//meta[@name="description"]'
            '/@content'
        ).extract_first()

        meta_tags = response.xpath(
            '//meta'
        ).extract()

        def remove_extra_spaces(s):
            if isinstance(s, string_types):
                return re.sub(r'\s+', ' ', s)
            return s

        return Meta(
            charset=charset,
            canonical_url=canonical_url,
            browser_title=remove_extra_spaces(browser_title),
            keywords=remove_extra_spaces(keywords),
            description=remove_extra_spaces(description),
            meta_tags=[remove_extra_spaces(m) for m in meta_tags],
        )

    @staticmethod
    def _parse_htags(response):
        """Get heading tags from product page (h1 ,h2)

        :param response (HtmlResponse): general url response
        :return (HTags): object represents headings from product page
        """
        h1 = response.xpath(
            '//h1//'
            'text()'
        ).extract()
        h2 = response.xpath(
            '//h2'
            '//text()'
        ).extract()
        return HTags(h1, h2)

    @staticmethod
    def _parse_specs(data):
        """Get product specs.

        :param data (dict): general json `data`
        :return (dict): dictionary of product specs
        """
        specs_dict = {}

        for v in data.get('product', {}).get('idmlMap', {}).values():
            specs = v.get('modules', {}).get('Specifications', {}).get('specifications', {}).get('values')
            if specs:
                for spec in specs[0]:
                    for s in spec.values():
                        specs_dict[s['displayName']] = s['values'][0]

        if specs_dict:
            return specs_dict

    @staticmethod
    def _parse_model(specs):
        """Get product model.

        :param specs (dict): specs from method `_parse_specs`
        :return (str): model
        """

        for spec_name, spec_value in (specs or {}).iteritems():
            if spec_name.lower() == 'model':
                return spec_value

    @staticmethod
    def _parse_directions(data, product_id, selected_product):
        """Get product directions.

        :param product_id (str): product id from method `_parse_sku`
        :param selected_product (dict): selected product from `_parse_selected_product`
        :return (str): directions
        """
        directions = data.get('product', {}).get('idmlMap', {}).get(product_id, {}).get('modules', {})\
            .get('Directions', {}).get('instructions', {}).get('values')
        if directions:
            return directions[0]

        return selected_product.get('productAttributes', {}).get('instructions')

    @staticmethod
    def _parse_ingredients(selected_product):
        """Get product ingredients.

        :param selected_product (dict): selected product from `_parse_selected_product`
        :return (list): ingredients
        """
        ingredients = selected_product.get('productAttributes', {}).get('ingredients')
        if ingredients:
            if ingredients.lower() != 'no':
                return [i.strip() for i in ingredients.split(',')]

    @staticmethod
    def _parse_warnings(data, product_id, selected_product):
        """Get product warnings.

        :param product_id (str): product id from method `_parse_sku`
        :param selected_product (dict): selected product from `_parse_selected_product`
        :return (str): warnings
        """
        modules = data.get('product', {}).get('idmlMap', {}).get(product_id, {}).get('modules', {})
        warnings = modules.get('Warnings', {}).get('warnings', {}).get('values')
        if not warnings:
            warnings = modules.get('Warnings', {}).get('prop_65_warning_text', {}).get('values')
        if warnings:
            return warnings[0]

        return selected_product.get('productAttributes', {}).get('warnings')

    @staticmethod
    def _parse_pickup_today(offers):
        return any(
            [
                pickup_option.get('pickupMethod') == 'PICK_UP_TODAY' for offer in offers
                for pickup_option in offer.get('fulfillment', {}).get('pickupOptions', [])
                ]
        )

    @staticmethod
    def _parse_price_with_discount(offers):
        if isinstance(offers, Iterable):
            for offer in offers:
                price = offer.get('pickupDiscountOfferPrice', {}).get('price') or \
                        offer.get('pickupDiscount', {}).get('price')
                if price:
                    return price

    @staticmethod
    def _parse_swatches(data):
        swatches = []

        for product in data.get('product', {}).get('variantCategoriesMap', {}).values():
            for attribute in product.values():
                if attribute.get('type') == 'SWATCH':
                    swatch_name = attribute['id']

                    if swatch_name == 'actual_color':
                        swatch_name = 'color'

                    for variant in attribute['variants'].values():
                        # Some variants are empty
                        if variant:
                            hero_image = variant['swatchImageUrl']
                            hero_image = [hero_image] if hero_image and hero_image != '/static/img/no-image-sm.jpg' else []

                            swatch = {
                                swatch_name: variant['name'],
                                'hero_image': hero_image,
                                'hero': len(hero_image)
                            }

                            swatches.append(swatch)

        if swatches:
            return swatches

    def _parse_variants(self, data, selected_product_id, offers):
        """Get product variants

        :param data (dict): general json `data`
        :param selected_product_id (str): product id from method `_parse_sku`
        :param offers (list): product offers from method `_parse_offers`
        :return (dict): dictionary represents variants
        """
        variants = []
        primary_product_id = data.get('product', {}).get('primaryProduct')
        try:
            properties = data.get('product', {}).get('variantCategoriesMap', {}).get(primary_product_id, {})
        except Exception:
            properties = {}

        first_property = True
        # pylint: disable=R1702
        # TODO rework to contain less nested blocks if possible
        for property_name, property_value in properties.items():
            if property_name == 'actual_color':
                property_name = 'color'
            elif property_name == 'clothing_size':
                property_name = 'size'
            for variant in property_value.get('variants', {}).values():
                for product_id in variant.get('products', []):
                    product = data.get('product', {}).get('products', {}).get(product_id, {})

                    url = 'https://walmart.com/ip/{}'.format(product.get('usItemId'))

                    if first_property:
                        # remove dollar sign from price and convert to float
                        price = self._parse_price(data, offers, product, False)[0]
                        # If variant is no longer available, price will be None
                        price = re.search(Price.REGEXP, price) if price else None
                        if price:
                            price = float(re.sub(',', '', price.group(0)))
                        variants.append({
                            'in_stock': any(self._is_in_stock(o) for o in offers
                                            if o.get('id') in product.get('offers', []))
                            if price else False,
                            'price': price,
                            'properties': {property_name: variant.get('name')},
                            'selected': product_id == selected_product_id,
                            'sku': product_id,
                            'upc': product.get('upc'),
                            'url': 'https://walmart.com/ip/{}'.format(product.get('usItemId')),
                        })
                    else:
                        for v in variants:
                            if v['url'] == url:
                                v['properties'].update({property_name: variant.get('name')})
            first_property = False
        return variants

    def _parse_marketplace(self, data, offers):
        """Get product marketplace for all offers in stock.

        :param data (dict): general json `data`
        :param offers (list): list of product offers from method `_parse_offers`
        :return (list): list of dicts containing name, currency, price, and in_stock
        """
        sellers = data.get('terra', {}).get('sellers', {})
        if not sellers:
            self.logger.info('Could not retrieve sellers from Terra-Firma API. Falling back to default.')
            sellers = data.get('product', {}).get('sellers', {})

        marketplace = []

        for offer in offers:
            seller_name = sellers.get(offer.get('sellerId'), {}).get('sellerDisplayName')
            if seller_name and 'Walmart' not in seller_name:
                currency = offer.get('pricesInfo', {}).get('priceMap', {}).get('CURRENT', {}).get('currencyUnit')
                price = offer.get('pricesInfo', {}).get('priceMap', {}).get('CURRENT', {}).get('price')

                marketplace.append({
                    'name': seller_name,
                    'currency': currency,
                    'price': price,
                    'in_stock': self._is_in_stock(offer),
                })

        # return an empty list if there are no marketplace offers
        return marketplace

    @staticmethod
    def _parse_sc_offers(data, selected_product_id):
        offers = data.get('terra', {}).get('offers', {}) or data.get('product', {}).get('offers', {})
        offer_ids = data.get('terra', {}).get('products', {}).get(
            selected_product_id, {}).get('offers', []) or data.get('product', {}).get(
            'products', {}).get(selected_product_id, {}).get('offers', [])

        # if there is one offers, structure of json is different
        if offers.get('availabilityStatus'):
            return [offers]

        return [
            offers.get(offer_id, {})
            for offer_id in offer_ids
            if offers.get(offer_id, {})
        ]

    @staticmethod
    def _parse_sc_sellers(data):
        sellers = data.get('terra', {}).get('sellers', {}) or data.get('product', {}).get('sellers', {})
        # if there is one seller, structure of json is different
        if sellers.get('sellerId'):
            return {sellers.get('sellerId'): sellers}
        return sellers

    @staticmethod
    def _parse_sc_marketplaces(marketplace_offers, marketplace_sellers):
        def is_marketplace_shippable(marketplace):
            return marketplace.get('fulfillment', {}).get('shippable')

        def is_marketplace_pickupable(marketplace):
            return marketplace.get('fulfillment', {}).get('pickupable')

        def is_marketplace_out_of_stock(marketplace):
            return marketplace.get('productAvailability', {}).get('availabilityStatus') == "OUT_OF_STOCK"

        def is_marketplace_available(marketplace):
            availability_conditions = [
                is_marketplace_pickupable(marketplace) or is_marketplace_shippable(marketplace),
                not is_marketplace_out_of_stock(marketplace)
            ]
            return all(availability_conditions)

        all_offers_unavailable = not any(
            is_marketplace_available(marketplace) for marketplace in marketplace_offers
        )

        marketplaces = []
        for marketplace in marketplace_offers:
            price_map = marketplace.get('pricesInfo', {}).get('priceMap', {}).get('CURRENT', {})

            price = price_map.get('price', 0)
            currency = price_map.get('currencyUnit')
            price_details_in_cart = price_map.get('submapType') in ("CHECKOUT", "CART")

            if marketplace.get('status') == 'NOT_FETCHED':
                name = seller_type = None
            else:
                seller_id = marketplace.get('sellerId')
                name = marketplace_sellers.get(seller_id, {}).get('sellerDisplayName', '')
                seller_type = 'site' if 'Walmart' in name else 'marketplace'

            marketplace_conditions = [
                is_marketplace_available(marketplace),
                all_offers_unavailable and not marketplaces
            ]

            if any(marketplace_conditions):
                marketplaces.append({
                    'name': name,
                    'price': price,
                    'currency': currency,
                    'seller_type': seller_type,
                    'price_details_in_cart': price_details_in_cart,
                    'shippable': is_marketplace_shippable(marketplace),
                    'pickupable': is_marketplace_pickupable(marketplace)
                })

        return marketplaces

    @staticmethod
    def _parse_price_details_in_cart(marketplaces):
        """
        Return True if price is available only after you put the product in the cart

        :param marketplaces (list): marketplaces from '_parse_sc_marketplaces' method
        :return (bool): value of price_details_in_cart for the main offer
        """

        if marketplaces:
            return marketplaces[0].get('price_details_in_cart')

    @staticmethod
    def _create_reviews_obj(reviews):
        review_dates = [
            datetime.strptime(customer_review.get('reviewSubmissionTime'), '%m/%d/%Y')
            for customer_review in reviews.get('customerReviews', [])
            if customer_review.get('reviewSubmissionTime', None)
        ]
        return BuyerReviews(
            stars={
                1: reviews.get('ratingValueOneCount', 0),
                2: reviews.get('ratingValueTwoCount', 0),
                3: reviews.get('ratingValueThreeCount', 0),
                4: reviews.get('ratingValueFourCount', 0),
                5: reviews.get('ratingValueFiveCount', 0),
            },
            count=reviews.get('totalReviewCount', 0),
            average=reviews.get('roundedAverageOverallRating', 0),
            last_review_date=max(review_dates).date() if review_dates else None
        )

    @staticmethod
    def _parse_buyer_reviews(data, product_id):
        """Get product buyer review.

        :param data (dict): general json `data`
        :param product_id (str): product id from `_parse_sku` method
        :return (BuyerReviews): object represents product rating bu stars, average rating and count of reviews
        """
        reviews = data.get('product', {}).get('reviews', {})
        # Raise error if product_id is not present in reviews (CON-43957)
        if reviews and product_id not in reviews:
            raise KeyError(product_id)
        return WalmartProductsSpider._create_reviews_obj(reviews.get(product_id, {}))

    @staticmethod
    def _are_reviews_parsed(buyer_reviews):
        return bool(buyer_reviews.get('last_review_date'))

    def _prepare_review_request_args(self, meta):
        return {
            'url': self.REVIEWS_URL,
            'method': 'POST',
            'meta': meta,
            'item': meta.get('item'),
            'callback': self._parse_reviews_from_api,
            'body': json.dumps({
                'productId': meta.get('item', {}).get('sku'),
                'paginationContext': {
                    'page': 1,
                    'sort': 'submission-desc',
                    'filters': [],
                    'limit': 100
                }
            }),
            'headers': {'Content-Type': 'application/json'}
        }

    @catch_json_exceptions
    def _parse_reviews_from_api(self, response):
        buyer_reviews = {}
        response_obj = json.loads(response.body)
        if response_obj and response_obj.get('status') == 'OK':
            product_id = response.meta.get('item', {}).get('sku')
            reviews_obj = response_obj.get('payload', {}).get('reviews', {}).get(product_id, {})
            buyer_reviews = self._create_reviews_obj(reviews_obj)

        response.meta['reviews_api_retries_left'] -= 1
        if not self._are_reviews_parsed(buyer_reviews) and 0 < response.meta['reviews_api_retries_left']:
            request = MergeRequest(**self._prepare_review_request_args(response.meta))
            with CacheContext(request, date=self.crawl_date) as cached_request:
                return cached_request

        product = response.meta.get('item')
        cond_set_value(product, 'buyer_reviews', buyer_reviews)
        return product

    @staticmethod
    def _parse_rollback(offers):
        return any([
            offer.get('pricesInfo', {}).get('priceDisplayCodes', {}).get('rollback')
            for offer in offers
        ])

    # Q&A parsing
    @staticmethod
    def _compile_questions_data(product_id, page, limit=''):
        return json.dumps({
            "productId": product_id,
            "paginationContext": {
                "sort": "totalAnswerCount",
                "page": int(page),
                "limit": int(limit) if limit else limit,
                "filters": []
            }
        })

    def _parse_questions_answers_first_page(self, response):
        product = response.meta.get('item')
        product_id = response.meta['product_id']

        product['recent_questions'] = []

        recent_questions = product['recent_questions']

        try:
            product_data = self._parse_questions_answers_product_data(response, product_id)
            recent_questions.extend(
                product_data['questionDetails']
            )
        except Exception:
            self.logger.info('Can not load questions data from json - no questions?')
            return

        self._add_answered_by_username(recent_questions)

        try:
            max_page = product_data['pagination']['pages'][-1]['num']
        except Exception:
            self.logger.info('Can not extract next pages data: {}'.format(traceback.format_exc()))
        else:
            for page in range(2, max_page + 1):
                request = response.request.replace(
                    body=self._compile_questions_data(
                        product_id,
                        page
                    ),
                    callback=self._parse_questions_answers
                )
                with CacheContext(request, date=self.crawl_date) as cached_request:
                    yield cached_request

    @staticmethod
    def _parse_questions_answers_product_data(response, product_id):
        data = json.loads(response.body_as_unicode())
        product_data = data['payload']['questionAnswers'][product_id]
        return product_data

    def _parse_questions_answers(self, response):
        product = response.meta.get('item')
        product_id = response.meta['product_id']

        recent_questions = product['recent_questions']

        try:
            product_data = self._parse_questions_answers_product_data(response, product_id)
            recent_questions.extend(
                product_data['questionDetails']
            )
        except Exception:
            self.logger.info('Can not load json from response: {}'.format(traceback.format_exc()))
            return

        self._add_answered_by_username(recent_questions)

    def _add_answered_by_username(self, recent_questions):
        if self.username:
            for q in recent_questions:
                if 'answeredByUsername' not in q:
                    q['answeredByUsername'] = False
                    for answer in q.get('answers', []):
                        if self.username.strip().lower() == answer.get('userNickname', '').strip().lower():
                            q['answeredByUsername'] = True

        return recent_questions

    @staticmethod
    def _parse_in_store_pickup(data):
        offers = data.get('product', {}).get('offers')
        # 'offers' layout is different when bonus gif card present. See: CON-41559
        is_gift_bonus_layout = offers.get('offerInfo', None)
        if is_gift_bonus_layout:
            offer_id = offers.get('offerInfo').get('offerId', '')
            offers_usual_layout = {offer_id: offers}
            offers = offers_usual_layout

        if offers:
            return any(
                option.get('availability') == "AVAILABLE" for store in offers
                for option in offers.get(store).get('fulfillment', {}).get('pickupOptions', [])
            )

    @staticmethod
    def _parse_bestseller_ranks(data, sku):
        ranks = data.get('terra', {}).get('products', {}).get(sku, {}).get("itemSalesRanks", [])
        if ranks:
            return [[int(rank.get('rank', 0)), [path.get('name') for path in rank.get('path', [])]] for rank in ranks]

    @staticmethod
    def _parse_save_amount(offers):
        top_offer = offers[0] if offers else {}
        s_amount = top_offer.get('pricesInfo', {}).get('savings', {}).get('savingsAmount', {}).get('price')
        return s_amount

    @staticmethod
    def _parse_was_price(offers):
        top_offer = offers[0] if offers else {}
        old_p = top_offer.get('pricesInfo', {}).get('priceMap', {}).get('WAS', {}).get('price')
        if not old_p:
            old_p = top_offer.get('pricesInfo', {}).get('priceMap', {}).get('LIST', {}).get('price')
        return old_p

    @staticmethod
    def _parse_now_price(offers):
        top_offer = offers[0] if offers else {}
        new_p = top_offer.get('pricesInfo', {}).get('priceMap', {}).get('CURRENT', {}).get('price')
        return new_p

    @staticmethod
    def _parse_list_price(offers):
        top_offer = offers[0] if offers else {}
        list_price = top_offer.get('pricesInfo', {}).get('priceMap', {}).get('LIST', {}).get('price')
        return list_price

    @staticmethod
    def _parse_webcollage_contents(data):
        wc_page_contents = None

        def fix_html_content(html_content):
            html_content = unquote(html_content)

            html_content = re.sub(r'\\\\', '', html_content)
            html_content = re.sub(r"\\'", '', html_content)
            html_content = re.sub(r'\\"', '"', html_content)

            return html.fromstring(html_content)

        marketing_content = deep_search(
            'MarketingDescription', data) or deep_search('SellPointsMarketingContent', data)

        if marketing_content:
            wc_page_contents = marketing_content[0].get('htmlContent')

            if wc_page_contents:
                wc_page_contents = fix_html_content(wc_page_contents)

        webcollage_360 = deep_search('Webcollage360View', data)

        if webcollage_360:
            webcollage_360 = webcollage_360[0].get('htmlContent')

            if webcollage_360:
                webcollage_360 = fix_html_content(webcollage_360)

        return wc_page_contents, webcollage_360

    @staticmethod
    def _parse_webcollage_360(webcollage_360):
        return bool(webcollage_360.xpath('//div[@data-section-tag="360-view"]'))

    @staticmethod
    def _parse_webcollage_pdf(data):
        webcollage_pdf = deep_search('WebcollageDocuments', data)

        if webcollage_pdf:
            urls = deep_search('url', webcollage_pdf[0])

            if urls:
                return [u['values'][0] for u in urls]

    @staticmethod
    def _parse_nutrition_facts(data):
        nutrition_facts_data = deep_search('NutritionFacts', data)

        if nutrition_facts_data:
            nutrition_facts_data = nutrition_facts_data[0].values()

            nutrition_facts = []

            while nutrition_facts_data:
                nutrition_fact = nutrition_facts_data.pop(0)

                if nutrition_fact.get('children'):
                    nutrition_facts_data.extend(nutrition_fact['children'])
                else:
                    nutrition_facts.append(nutrition_fact)

            return nutrition_facts

    @staticmethod
    def _parse_store(offers):
        for offer in offers:
            if not offer.get('fulfillment', {}).get('pickupable'):
                continue
            options = offer.get('fulfillment', {}).get('pickupOptions')
            if options and options[0].get('preferredStore'):
                return str(options[0].get('storeId'))

    @staticmethod
    def _parse_is_add_to_cart(response):
        return bool(
            response.xpath('//button[contains(., "Add to Cart")]')
        )

    @staticmethod
    def _parse_collection_image_alts(data):
        products = data.get('productData', {}).get('products', {}).values()
        image_alts = []
        for product in products or []:
            name = product.get('name')
            if name:
                image_alts.append(name)
        return image_alts

    @staticmethod
    def _parse_collection_availability(data):
        products = data.get('productData', {}).get('products', {}).values()
        availability = []
        for product in products or []:
            avail = product.get('availabilityStatus') == 'IN_STOCK'
            availability.append(avail)
        return availability

    @staticmethod
    def _parse_questions_total(data, sku):
        return data.get('terra', {}).get('questionAnswers', {}).get(sku, {}).get('pagination', {}).get('total')
