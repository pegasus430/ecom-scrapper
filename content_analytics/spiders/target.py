import re
import json
import urllib
import urlparse
import itertools
import traceback
from lxml import html
from PIL import Image
from datetime import datetime
from cStringIO import StringIO
from HTMLParser import HTMLParser

from scrapy import Field

from . import BaseProductsSpider, MergeRequest
from ..items import BuyerReviews, SiteProductItem

from content_analytics.utils import catch_dictionary_exception, catch_json_exceptions, deep_search, find_between, get_color, parse_all_webcollage


class TargetProductItem(SiteProductItem):
    country_of_origin = Field()
    details = Field()
    dpci = Field()
    how_to_measure = Field()
    image_colors = Field()
    in_stock = Field()
    mta = Field()
    questions_total = Field()
    questions_unanswered = Field()
    size_chart = Field()
    subscribe_discount = Field()
    subscribe_price = Field()
    tcin = Field()
    ugc = Field()
    image_names = Field()


class TargetProductsSpider(BaseProductsSpider):
    name = "target_products"
    allowed_domains = ['target.com']

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
    }

    # Urls
    PRODUCT_URL = 'http://redsky.target.com/v2/pdp/tcin/{tcin}?excludes=taxonomy&storeId={store_id}'

    DEPARTMENTS_URL = 'https://redoak.target.com/content-publish/pages/v1/' \
                      '?url={url}&children=true&breadcrumbs=true&channel=web'

    MODULE_URL = 'https://static.targetimg1.com/itemcontent/sizecharts/html/{module_name}.html'

    SEARCH_URL = 'https://redsky.target.com/v1/plp/search?keyword={search_term}&count=24&offset=0'

    SHELF_URL = 'https://redsky.target.com/v1/plp/search?count=24&offset=0&category={category}'

    QUESTIONS_URL = 'https://redsky.target.com/drax-domain-api/v1/questions?product_id={product_id}'

    VIDEO_BASE_URL = 'http://cdnbakmi.kaltura.com/p/1634272/playManifest/entryId/{entry_id}/format/url/protocol/http/a.mp4'

    WEBCOLLAGE_POWER_PAGE = 'http://content.webcollage.net/target/power-page?ird=true&channel-product-id={product_id}'

    def __init__(self, *args, **kwargs):
        super(TargetProductsSpider, self).__init__(*args, **kwargs)
        self.store_id = kwargs.get('store', '1139') # Store ID for CH is 2088

        self._original_product_url = self._product_url

        if getattr(self, '_product_url', None):
            self._product_url = self.get_product_url(self._product_url)
        elif getattr(self, '_shelf_url', None):
            self._shelf_url = self.get_shelf_url(self._shelf_url)

    # Single component
    def get_product_url(self, product_url):
        tcin = self._get_product_id_from_input_product_url(product_url)
        return self.PRODUCT_URL.format(
            tcin=tcin,
            store_id=self.store_id
        )

    def get_default_item(self, *args, **kwargs):
        return TargetProductItem()

    def _check_how_to_measure(self, response):
        product = response.meta.get('item')
        product['how_to_measure'] = bool(response.xpath(
                    '//div[contains(@class,"size-chart-content")]'
                    '//div[contains(@class,"size-chart-section measure")]'
                ))

    @catch_json_exceptions
    def _parse_questions_unanswered(self, response):
        product = response.meta.get('item')
        questions_unanswered = len([q for q in json.loads(response.body) if not q.get('AnswerIds')])
        product['questions_unanswered'] = questions_unanswered

    def _parse_ugc(self, response):
        product = response.meta.get('item')
        image_urls = response.xpath('//img[@class="imagePost--image"]/@src').extract()
        if not image_urls:
            images_data = response.xpath('//section[@id]//div[@role="img"]/@style').extract()
            image_urls = re.findall(r'url\((.*?)/\w+/\d+x\d+', ' '.join(images_data))
        if image_urls:
            product['ugc'] = ['https:' + i for i in image_urls]

    def _check_video(self, response):
        product = response.meta.get('item')
        # append video url if response is 200
        product['video_urls'].append(response.url)

    def _parse_webcollage(self, response):
        product = response.meta.get('item')
        if '_wccontent' in response.body:
            wc_page_contents = find_between(response.body, 'html: "', '"\n').decode('string_escape')
            wc_page_contents = html.fromstring(wc_page_contents.replace('\\', ''))
            parse_all_webcollage(wc_page_contents, product)

    def _parse_image_colors_and_res(self, response):
        product = response.meta.get('item')
        index = response.meta.get('index')

        try:
            image = Image.open(StringIO(response.body))

            try:
                color = get_color(image.load()[0, 0])
                product['image_colors'][index] = color
            except:
                self.logger.warning('Error while finding image color: {}'.format(traceback.format_exc()))

            try:
                width, height = image.size
                product['image_res'][index] = [width, height]
            except:
                self.logger.warning('Error while finding image res: {}'.format(traceback.format_exc()))

        except:
            self.logger.warning('Error while finding image color and res: {}'.format(traceback.format_exc()))

    @catch_json_exceptions
    def parse_product(self, response):
        data = json.loads(response.body_as_unicode())['product']
        product = response.meta.get('item')

        yield response.request.replace(
            url=self.DEPARTMENTS_URL.format(url='/p/a/-/A-{}'.format(
                self._get_product_id_from_api_url(response.url))),
            callback=self._parse_departments
        )

        yield self._parse_initial_product(response, product, data)

        # How to measure
        size_chart_guide_url = data['item'].get('enrichment', {}).get('size_chart')
        if size_chart_guide_url:
            chart_name = re.search(r'size-charts/(.*?)\?', size_chart_guide_url)
            if chart_name:
                yield MergeRequest(
                    url=self.MODULE_URL.format(module_name=chart_name.group(1)),
                    item=product,
                    callback=self._check_how_to_measure,
                    dont_filter=True
                )

        # Image colors and image res
        image_urls = product['image_urls']
        if image_urls:
            max_index = len(image_urls)
            product['image_colors'] = [None] * max_index
            product['image_res'] = [None] * max_index

            for i, image_url in enumerate(image_urls):
                yield MergeRequest(
                    url=image_url,
                    item=product,
                    callback=self._parse_image_colors_and_res,
                    meta={
                        'index': i,
                    },
                    dont_filter=True
                )

        # Questions unanswered
        yield MergeRequest(
            url=self.QUESTIONS_URL.format(product_id=product['site_product_id']),
            item=product,
            callback=self._parse_questions_unanswered,
            dont_filter=True
        )

        # Ugc
        awesome_shop = data.get('awesome_shop', {})
        awesome_shop_url = awesome_shop.get('awesomeshopUrl', '')
        awesome_shop_ugc = awesome_shop.get('ugc', [])
        if product['site_product_id'] and product['site_product_id'] in awesome_shop_url:
            yield MergeRequest(
                url=awesome_shop_url,
                item=product,
                callback=self._parse_ugc,
                dont_filter=True
            )
        elif awesome_shop_ugc:
            product['ugc'] = ['http:' + ugc['imageUrl'] for ugc in awesome_shop_ugc if ugc.get('imageUrl')]
        else:
            image_url = awesome_shop.get('ugcHero', {}).get('imageUrl')
            if image_url:
                product['ugc'] = ['http:' + image_url]

        # Video urls
        videos = data['item']['enrichment'].get('video_content_list') or []
        for video in videos:
            entry_id = video.get('entry_id')
            if entry_id:
                yield MergeRequest(
                    url=self.VIDEO_BASE_URL.format(entry_id=entry_id),
                    item=product,
                    callback=self._check_video,
                    dont_filter=True
                )

        # Webcollage
        yield MergeRequest(
            url=self.WEBCOLLAGE_POWER_PAGE.format(product_id=product['site_product_id']),
            item=product,
            callback=self._parse_webcollage,
            dont_filter=True
        )

        # Add video from webcollage if not already present
        if not product['video_urls'] and product.get('webcollage_video_urls'):
            product['video_urls'] = product['webcollage_video_urls']

    def _parse_initial_product(self, response, product, data):
        # write for department and image_url pipeline to autofill these fields
        product_main_data = data['item']
        product_id = self._get_product_id_from_input_product_url(self._original_product_url or response.url)
        upc = self._parse_upc(product_main_data)
        image_urls = self._parse_image_urls(product_main_data)

        product.update(
            {
                # Main data
                'title': self._parse_title(product_main_data),
                'brand': self._parse_brand(product_main_data),
                'url': self._parse_url(product_main_data),

                # Shared data
                'buyer_reviews': self._parse_buyer_reviews(data),
                'variants': self._parse_variants(product_main_data, upc),
                'swatches': self._parse_swatches(product_main_data),

                # CH data
                'bullets': self._parse_bullets(product_main_data),
                'details': self._parse_description(product_main_data),
                'features': self._parse_features(product_main_data),
                'how_to_measure': False, # use requests later
                'image_urls': image_urls,
                'image_names': self._parse_image_names(product_main_data),
                'ingredients': self._parse_ingredients(product_main_data),
                'mta': self._parse_mta(product_main_data),
                'short_description': self._parse_description(product_main_data),
                'size_chart': self._parse_size_chart(product_main_data),
                'subscribe_price': self._parse_subscribe_price(data),
                'subscribe_discount': self._parse_subscribe_discount(data),
                'video_urls': self._parse_video_urls(product_main_data),
                'questions_total': self._parse_questions_total(data),

                # SC data
                'country_of_origin': self._parse_country_of_origin(product_main_data),
                'image_url': image_urls[0] if image_urls else None,

                # Availability
                'in_stores': self._parse_in_stores(data),
                'site_online': self._parse_site_online(data),
                'in_stores_out_of_stock': self._parse_in_stores_out_of_stock(data),
                'site_online_out_of_stock': self._parse_site_online_out_of_stock(data),
                'in_stock': self._parse_in_stock(data), # special in-stock logic for Target scraper

                # Price
                'price': self._parse_price(data),
                'price_currency': self._parse_price_currency(),
                'temp_price_cut': self._parse_temp_price_cut(data),

                # Codes parsing
                'upc': upc,
                'tcin': product_id,
                'reseller_id': product_id,
                'store': self.store_id,
                'dpci': self._parse_dpci(product_main_data),
                'site_product_id': product_id
            }
        )

        if self.shelf_url:
            product['secondary_id'] = product.get('dpci')

    # Json product parsing
    @catch_dictionary_exception
    def _parse_title(self, data):
        return HTMLParser().unescape(data['product_description']['title'])

    @catch_dictionary_exception
    def _parse_brand(self, data):
        return data['product_brand']['brand']

    @catch_dictionary_exception
    def _parse_url(self, data):
        return data['buy_url']

    @catch_json_exceptions
    def _parse_upc(self, data):
        return deep_search('upc', data)[0]

    @catch_json_exceptions
    def _parse_dpci(self, data):
        return deep_search('dpci', data)[0]

    @catch_dictionary_exception
    def _parse_description(self, data):
        if data.get('child_items'):
            return data['child_items'][0]['product_description'].get('downstream_description')
        return data['product_description'].get('downstream_description')

    @catch_dictionary_exception
    def _parse_site_online(self, data, first_variant=False):
        child_items = data['item'].get('child_items')
        if child_items:
            if child_items[0]['available_to_promise_network']['availability'] != 'UNAVAILABLE':
                return True
        elif data['available_to_promise_network']['availability'] != 'UNAVAILABLE':
            return True
        return False

    @catch_dictionary_exception
    def _parse_site_online_out_of_stock(self, data):
        if self._parse_site_online(data):
            child_items = data['item'].get('child_items')
            if child_items:
                if child_items[0]['available_to_promise_network']['availability_status'] != 'OUT_OF_STOCK':
                    return False
            elif data['available_to_promise_network']['availability_status'] != 'OUT_OF_STOCK':
                return False
            return True

    @catch_dictionary_exception
    def _parse_in_stores(self, data):
        child_items = data['item'].get('child_items')
        if child_items:
            if child_items[0]['available_to_promise_store']['products'][0]['availability_status'] != 'OUT_OF_STOCK':
                return True
        elif data['available_to_promise_store']['products'][0]['availability_status'] != 'OUT_OF_STOCK':
            return True
        return False

    @catch_dictionary_exception
    def _parse_in_stores_out_of_stock(self, data):
        if self._parse_in_stores(data):
            child_items = data['item'].get('child_items')
            if child_items:
                for loc in child_items[0]['available_to_promise_store']['products'][0]['locations']:
                    if loc['availability_status'] not in ['NOT_SOLD_IN_STORE', 'OUT_OF_STOCK']:
                        return False
            else:
                for loc in data['available_to_promise_store']['products'][0]['locations']:
                    if loc['availability_status'] not in ['NOT_SOLD_IN_STORE', 'OUT_OF_STOCK']:
                        return False
            return True

    @catch_dictionary_exception
    def _parse_in_stock(self, data):
        if self._parse_site_online(data):
            return not(self._parse_site_online_out_of_stock(data))
        return False

    @catch_dictionary_exception
    def _parse_price(self, data):
        price = data['price']['offerPrice']['formattedPrice']
        return price if re.search(r'\d', price) else None

    @catch_dictionary_exception
    def _parse_temp_price_cut(self, data):
        eyebrow = data['price']['offerPrice'].get('eyebrow')
        return eyebrow in ['OnSale', 'Clearance'] if eyebrow else False

    def _parse_price_currency(self):
        return 'USD'

    @catch_dictionary_exception
    def _parse_country_of_origin(self, data):
        return data['country_of_origin']

    @catch_dictionary_exception
    def _parse_features(self, data):
        bullet_description = data['product_description'].get('bullet_description')
        if bullet_description:
            return [re.sub('<.*?>', '', b).strip() for b in bullet_description]

    @catch_dictionary_exception
    def _parse_bullets(self, data):
        soft_bullets = data['product_description'].get('soft_bullets')
        if soft_bullets:
            return '\n'.join(soft_bullets['bullets'])

    @catch_dictionary_exception
    def _parse_mta(self, data):
        return ''.join(data['product_description']['bullet_description'])

    @catch_dictionary_exception
    def _parse_size_chart(self, data):
        return bool(data['display_option']['is_size_chart'])

    @catch_dictionary_exception
    def _parse_subscribe_price(self, data):
        price = data['price']['offerPrice']['price']
        percent = self._parse_subscribe_discount(data)
        if price and percent:
            return round((price * (100 - percent)) / 100, 2)

    @catch_dictionary_exception
    def _parse_subscribe_discount(self, data):
        promotion = data['promotion']['promotionList']
        if promotion and promotion[0]['subscriptionType'] == 'SUBSCRIPTION':
            return promotion[0]['rewardValue']

    def _parse_ingredients(self, data):
        ingredients = deep_search('ingredients', data)

        if ingredients:
            r = re.compile(r'(?:[^,(]|\([^)]*\))+')
            ingredients = r.findall(ingredients[0])

            return [ingredient.strip() for ingredient in ingredients]

    @catch_dictionary_exception
    def _parse_image_urls(self, data):
        images = []
        for image in data['enrichment']['images']:
            base_url = image['base_url']
            all_image_codes = [image['primary']] + image.get('alternate_urls', [])
            images += [urlparse.urljoin(base_url, '{}?scl=1'.format(image_id)) for image_id in all_image_codes]
        return images

    @catch_dictionary_exception
    def _parse_image_names(self, data):
        image_names = []
        for image in data['enrichment']['images']:
            if image['primary']:
                image_names.append('Primary')

            alternate_urls = image.get('alternate_urls', [])
            for alternate_url in alternate_urls:
                image_code = re.search(r'Alt(\d+)', alternate_url)
                if image_code:
                    image_names.append(image_code.group(1))

        return image_names

    @catch_dictionary_exception
    def _parse_video_urls(self, data):
        video_urls = []
        videos = deep_search('videos', data)

        if videos:
            for video in videos[0]:
                for vf in video.get('video_files', []):
                    video_url = vf.get('video_url')
                    if video_url:
                        video_urls.append('http:' + video_url)

        return video_urls

    @catch_dictionary_exception
    def _parse_swatches(self, data):
        swatches = []
        color_list = []

        for child in data.get('child_items') or []:
            images = child['enrichment']['images'][0]

            if images.get('swatch'):
                color = child['variation']['color']
                hero_image = [images['base_url'] + images['swatch']]

                swatch = {
                    'color': color,
                    'hero_image': hero_image,
                    'hero': len(hero_image)
                }

                if not color in color_list:
                    color_list.append(color)
                    swatches.append(swatch)

        return swatches

    @catch_dictionary_exception
    def _parse_variants(self, data, upc):
        def _parse_variant(variant, selected, properties):
            image_urls = self._parse_image_urls(variant)

            variant = {
                'selected': selected,
                'upc': variant.get('upc'),
                'properties': properties,
                'price': variant['price']['offerPrice']['price'],
                'url': self._parse_url(variant['enrichment']),
                'image_url': image_urls[0] if image_urls else None,
                'in_stock': variant['available_to_promise_network']['availability'] != 'UNAVAILABLE' and \
                        variant['available_to_promise_network']['availability_status'] != 'OUT_OF_STOCK'
            }
            return variant

        def _parse_variant_properties(properties):
            properties = {k: v for x in properties for k, v in x.items()}
            for variant_properties in itertools.product(*properties.values()):
                yield {
                    key: variant_property for key, variant_property in zip(properties.keys(), variant_properties)
                }

        variants = []
        child_items = data.get('child_items')
        if child_items:
            properties = data['variation']['flexible_variation']
            for item, properties in zip(child_items, _parse_variant_properties(properties)):
                selected = item.get('upc') == upc
                variant = _parse_variant(item, selected, properties)
                variants.append(variant)
        return variants

    @catch_dictionary_exception
    def _parse_questions_total(self, data):
        return data['question_answer_statistics']['questionCount']

    @catch_dictionary_exception
    def _parse_departments(self, response):
        product = response.meta.get('item')
        data = json.loads(response.body_as_unicode())
        breadcrumbs = data['metadata']['breadcrumbs']
        product['departments'] = [HTMLParser().unescape(breadcrumb['seo_h1']) for breadcrumb in breadcrumbs][1:]
        product['department'] = product['departments'][-1]

    @catch_dictionary_exception
    def _parse_buyer_reviews(self, data):
        tcin = data['item']['tcin']
        rating_review = data['rating_and_review_statistics']['result'][tcin]['coreStats']
        last_review_date = data['rating_and_review_statistics']['result'][tcin]['mostRecentReviews']
        return BuyerReviews(
            stars=dict(reversed(_star.values()) for _star in rating_review['RatingDistribution']),
            average=rating_review.get('AverageOverallRating', 0),
            count=rating_review.get('RatingReviewTotal', 0),
            last_review_date=datetime.strptime(
                last_review_date[0]['SubmissionTime'].split('T')[0], '%Y-%m-%d'
            ).date() if last_review_date else None
        )

    # Search component
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term)

    @catch_dictionary_exception
    def parse_search_term_items(self, response):
        data = response.meta['data']
        items = data['search_response']['items']['Item']

        for item in items:
            yield self.get_product_url(item.get('url'))

    def get_search_term_next_page(self, response):
        metadata = response.meta['metadata']
        if metadata['currentPage'] < metadata['totalPages']:
            url_data = urlparse.urlsplit(response.url)
            qs_data = urlparse.parse_qs(url_data.query)
            qs_data['offset'] = [str(int(qs_data['offset'][0]) + int(metadata['count']))]
            return url_data._replace(query=urllib.urlencode(qs_data, True)).geturl()

    def parse_search_term_total_matches(self, response):
        data = json.loads(response.body_as_unicode())
        metadata = {item['name']: item['value'] for item in data['search_response']['metaData']}
        response.meta['metadata'] = metadata
        response.meta['data'] = data
        return metadata['total_results']

    def parse_search_term_results_per_page(self, response):
        return response.meta['metadata']['count']

    # Shelf component
    def get_shelf_url(self, shelf_url):
        category_id = self._get_category_id_from_shelf_url(shelf_url)
        return self.SHELF_URL.format(category=category_id)

    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def get_shelf_page_next_page(self, response):
        return self.get_search_term_next_page(response)

    def parse_shelf_page_total_matches(self, response):
        return self.parse_search_term_total_matches(response)

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)

    # Url handling
    @staticmethod
    def _get_category_id_from_shelf_url(shelf_url):
        category_id = re.search(r'/-/N-([\w\d]+)', shelf_url)
        if category_id:
            return category_id.group(1)

    @staticmethod
    def _get_product_id_from_input_product_url(product_url):
        product_id = re.search(r'[Aa]-(\d+)', product_url)
        if product_id:
            return product_id.group(1)

    @staticmethod
    def _get_product_id_from_api_url(api_url):
        product_id = re.search(r'tcin/(\d+)', api_url)
        if product_id:
            return product_id.group(1)
