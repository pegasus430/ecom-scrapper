import re
import json

from scrapy.item import Field

from content_analytics.spiders import BaseProductsSpider, MergeRequest
from content_analytics.utils import catch_json_exceptions
from content_analytics.data_parsing.buyer_reviews.bazaarvoice_reviews import BazaarvoiceReviews
from content_analytics.items import SiteProductItem


class GamestopProductItem(SiteProductItem):
    price_preowned = Field()
    price_download = Field()


class GamestopProductsSpider(BaseProductsSpider):
    name = "gamestop_products"
    allowed_domains = ['gamestop.com', 'api.bazaarvoice.com']

    # Urls
    SEARCH_URL = 'https://www.gamestop.com/browse?nav=16k-3-{search_term},28zu0'

    SCREENSHOT_URL = 'https://www.gamestop.com/Catalog/ProductScreenshots.aspx?Product_ID={product_id}&image={idx}'

    RESULTS_PER_PAGE = 12

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/65.0.3325.181 Safari/537.36'
    }

    def get_default_item(self, *args, **kwargs):
        return GamestopProductItem()

    # Single component
    def parse_product(self, response):
        product = response.meta.get('item')

        # Embedded json
        data = self._parse_main_inline_json(response)
        product_id = self._parse_product_id(response)
        price_new, price_preowned, price_download, price_regular = self._parse_multi_price_amount(data, response)

        product.update(
            {
                # General fields
                'title': self._parse_title(response),
                'product_id': product_id,
                'short_description': self._parse_description(response),
                'long_description': self._parse_long_description(response),
                'buyer_reviews': self._parse_buyer_reviews(response),

                # CH fields
                'specs': self._parse_specs(response),

                # Price
                'price_amount': price_new or price_preowned or price_download or price_regular,
                'price_preowned': price_preowned,
                'price_download': price_download,
                'price_currency': 'USD',

                # Identification
                'sku': product_id,
                'reseller_id': product_id,

                # Availability
                'in_stores': True,
                'site_online': True,
                'is_out_of_stock': False,
            }
        )

        if product_id:
            yield MergeRequest(
                url=BazaarvoiceReviews.compile_url(
                    product_id=product_id,
                    passkey='ca0SPanXcxTi6Os49LTaXK2PuXoCok57Y7dzJY0FfuxDs',
                    displaycode='9014-en_us'
                ),
                item=product,
                callback=self._parse_buyer_reviews
            )

            image_count = response.xpath(
                '//a[contains(@class, "viewall") and contains(text(), "screenshots")]/text()').re_first(r'\d+')

            if image_count:
                for i in range(int(image_count)):
                    payload = {
                        'Product_ID': product_id,
                        'image': i + 1,
                    }
                    yield MergeRequest(
                        url=self.SCREENSHOT_URL.format(
                            product_id=product_id,
                            idx=(i + 1),
                        ),
                        method='POST',
                        item=product,
                        callback=self._parse_images,
                        body=json.dumps(payload)
                    )

        yield product

    # Parsing inline product json blocs
    @catch_json_exceptions
    def _parse_main_inline_json(self, response):
        media_data = re.search(r'digitalData =(.*?);<', response.body)
        if media_data:
            return json.loads(media_data.group(1))

    @staticmethod
    def _parse_title(response):
        title = response.xpath(
            '//h1[@itemprop="name"]/descendant::text()'
        ).extract()

        return ''.join(title) if title else None

    def _parse_description(self, response):
        descs = response.xpath('//p[@class="productbyline"]/text()').extract()

        return self._clean_text(''.join(descs)) if descs else None

    def _parse_long_description(self, response):
        long_desc_p = response.xpath('//div[contains(@class, "longdescription")]/p/text()').extract()
        long_desc_p = ''.join(long_desc_p)

        long_desc_ul = response.xpath(
            '//div[contains(@class, "longdescription")]/ul/li/descendant::text()').extract()
        long_desc_ul = ''.join(long_desc_ul)

        desc_from_gameinformer = response.xpath(
            '//div[contains(@class, "longdescription")]'
            '//div[contains(@class, "extra_content_gameinformer_preview")]/p/text()').extract()
        desc_from_gameinformer = ''.join(desc_from_gameinformer)

        long_desc = long_desc_p + long_desc_ul + desc_from_gameinformer

        return self._clean_text(long_desc) if long_desc else None

    @staticmethod
    def _parse_specs(response):
        specs = response.xpath(
            '//div[contains(@class, "extra_content_gameinformer_points")]')

        if specs:
            spec_terms = specs.xpath('./h5/text()').extract()
            spec_defs = specs.xpath('./p/text()').extract()

            if spec_terms and spec_defs and len(spec_terms) == len(spec_defs):
                return dict(zip(spec_terms, spec_defs))

    @staticmethod
    def _parse_images(response):
        product = response.meta.get('item')
        image_urls = product.get('image_urls', [])
        image_url = response.xpath(
            '//img[@id="mainContentPlaceHolder_ProductScreenshotsViewer_SrceenShotImage"]/@src').extract_first()

        if image_url:
            image_urls.append(response.urljoin(image_url))
            product['image_urls'] = image_urls

    @staticmethod
    def _parse_product_id(response):
        origin_url = response.url.split('?')[0]
        return origin_url.split('/')[-1]

    # Price parsing
    @staticmethod
    def _parse_multi_price_amount(data, response):
        price_new = None
        price_preowned = None
        price_download = None
        price_regular = None
        if isinstance(data, dict):
            products = data.get('product', [])
            for product in products:
                attributes = product.get('attributes', {})
                price = re.search(r'\d*\.\d+|\d+', attributes.get('price', ''))
                condition = attributes.get('condition')

                if price:
                    price = float(price.group())
                    if condition == 'New':
                        price_new = price
                    elif condition == 'Pre-Owned':
                        price_preowned = price
                    elif condition == 'Digital':
                        price_download = price
                    elif condition == 'Refurbished':
                        price_regular = price
        else:
            price_boxes = response.xpath('//div[contains(@class, "ats-prodBuy-buyBoxSec")]')
            for price_box in price_boxes:
                price = price_box.xpath('.//h3[@class="ats-prodBuy-price"]/span').re_first(r'\d*\.\d+|\d+')
                condition = price_box.xpath('.//strong[@class="ats-prodBuy-condition"]/text()').extract_first()
                if price:
                    price = float(price)
                else:
                    continue

                if 'NEW' in condition:
                    price_new = price
                elif 'PRE-OWNED' in condition:
                    price_preowned = price
                elif 'DOWNLOAD' in condition:
                    price_download = price
                elif 'REGULAR' in condition:
                    price_regular = price
                else:
                    price_new = price

        return price_new, price_preowned, price_download, price_regular

    @staticmethod
    def _parse_buyer_reviews(response):
        product = response.meta.get('item')
        product.update({'buyer_reviews': BazaarvoiceReviews.parse_reviews(response.body)})

        yield product

    @staticmethod
    def _clean_text(text):
        return re.sub("[\n\t\r]", "", text).strip()

    # Search component
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term)

    def parse_search_term_items(self, response):
        items = response.xpath(
            '//div[contains(@class, "products")]'
            '/div[contains(@class, "product")]'
            '//a[@class="ats-product-title-lnk"]/@href'
        ).extract()

        for item in items:
            yield response.urljoin(item)

    def get_search_term_next_page(self, response):
        next_page = response.xpath(
            '//a[contains(@class, "next_page") and contains(text(), "Next")]/@href'
        ).extract_first()

        if next_page:
            return response.urljoin(next_page)

    def parse_search_term_total_matches(self, response):
        results_text = response.xpath(
            '//div[contains(@class, "result_count_first")]//h3/descendant::text()').extract()
        results_text = ''.join(results_text)

        total_matches = re.search(r'of(.*?)Page', results_text)
        total_matches = re.search(r'\d+', total_matches.group(1).replace(',', '')) if total_matches else None

        return int(total_matches.group()) if total_matches else None

    def parse_search_term_results_per_page(self, response):
        return self.RESULTS_PER_PAGE

    # Shelf component
    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def get_shelf_page_next_page(self, response):
        return self.get_search_term_next_page(response)

    def parse_shelf_page_total_matches(self, response):
        return self.parse_search_term_total_matches(response)

    def parse_shelf_page_results_per_page(self, response):
        return self.parse_search_term_results_per_page(response)
