import re

from content_analytics.spiders import BaseProductsSpider


class RussellathleticProductsSpider(BaseProductsSpider):
    name = 'russellathletic_products'
    allowed_domains = ['www.russellathletic.com']

    custom_settings = {'USER_AGENT': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                                     "Chrome/65.0.3325.162 Safari/537.36"}

    SEARCH_URL = "https://www.russellathletic.com/on/demandware.store/Sites-russell_us-Site/default/Search-Show" \
                 "?q={search_term}&start={start}&sz={n_per_page}"
    N_RESULTS_PER_PAGE = 18

    ################################
    # Search term abstract methods #
    ################################
    def get_search_term_url(self, search_term):
        return self.SEARCH_URL.format(search_term=search_term, start=0, n_per_page=self.N_RESULTS_PER_PAGE)

    def get_search_term_next_page(self, response):
        last_start_index_re = re.search(r'start=(\d+)', response.url)
        if last_start_index_re:
            last_start_index = int(last_start_index_re.group(1))
            return re.sub(r'start=\d+', 'start=' + unicode(last_start_index + self.N_RESULTS_PER_PAGE), response.url)

    def parse_search_term_items(self, response):
        links = response.xpath('//*[@class="name-link"]//@href').extract()
        return [link.split('?')[0] for link in links]

    def parse_search_term_total_matches(self, response):
        total_matches = response.xpath('//*[@class="results-hits"]//text()').re_first(r'\d+')
        return int(total_matches) if total_matches else None

    def parse_search_term_results_per_page(self, response):
        return self.N_RESULTS_PER_PAGE

    ################################
    # Shelf pages abstract methods #
    ################################
    def get_shelf_page_next_page(self, response):
        return response.xpath('//*[@class="infinite-scroll-placeholder"]//@data-grid-url').extract_first()

    def parse_shelf_page_items(self, response):
        return self.parse_search_term_items(response)

    def parse_shelf_page_total_matches(self, response):
        return self.parse_search_term_total_matches(response)

    def parse_shelf_page_results_per_page(self, response):
        return self.N_RESULTS_PER_PAGE

    ###################
    # Parsing methods #
    ###################
    def parse_product(self, response):
        product = response.meta.get('item')

        product.update({
            'sku': response.xpath('//*[@itemprop="productID"]//text()').extract_first(),
            'title': response.xpath('//*[contains(@class, "medium-up-price-name")]//text()').extract_first(),
            'short_description': self._re_script(response, r'var meta = "(.+?)";'),
            'price_amount': float(self._re_script(response, r"'ecomm_totalvalue': '([\S]+?)',", 0.0)),
            'price_currency': 'USD',

            'image_urls': self._parse_images(response),
            'image_url': response.xpath('//*[contains(@class, "product-primary-image")]//a/@href').extract_first(),

            'features': response.xpath('//*[@class="feature-title"]//text()').extract(),
            'brand': self._re_script(response, r"'brand': '(.+?)',"),
            'department': self._re_script(response, r"'category': '(.+?)',"),
            'departments': response.xpath('//*[@class="breadcrumb-element" and @title]//text()').extract(),
            'variants': self._parse_variants(response),

            # Not implemented, web do not show OOS items
            'is_out_of_stock': not response.xpath('//*[contains(@class, "add-to-cart")]//text()').extract(),
            'site_online': True,
            'site_online_out_of_stock': False,
            'in_stores': True,
        })
        yield product

    @staticmethod
    def _re_script(response, re_string, default=None):
        return response.xpath('//script//text()').re_first(re_string, default)

    @staticmethod
    def _parse_images(response):
        images = response.xpath('//*[contains(@class, "swiper-slide") and contains(@class, "thumb")]/a/@href').extract()
        return [image.split('?')[0] for image in images] if images else None

    @staticmethod
    def _parse_variants(response):
        variants = []
        variant_selectors = response.xpath(
            '(//ul[contains(@class, "{}") and contains(@class, "swiper-wrapper")])[1]//li'.format('color'))
        # An additional API request might be required for each variant later. (if size+color product not in stock)
        sizes = response.xpath('//select[@id="va-size"]//option/text()').extract()
        for selector in variant_selectors:
            image_url = selector.xpath('a/img/@src').extract_first('').split('?')[0]
            variant = {'image_url': image_url if image_url else None,
                       'selected': selector.xpath('contains(@class, "selected")').extract_first() == '1',
                       'properties': {
                           'color': selector.xpath('a/img/@alt').extract_first(),
                           'size': ', '.join(map(unicode.strip, sizes[1:(len(sizes) / 2)])) if sizes else None
                       }
                       }

            variants.append(variant)
        return variants
