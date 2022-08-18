import re

import six
import datetime

from scrapy.item import Field, Item

class Price(object):
    REGEXP = re.compile(r'\d{1,3}[,\.\d{3}]*\.?\d*')
    CURRENCY_CODES = [
        'AED', 'AFN', 'ALL', 'AMD', 'ANG', 'AOA', 'ARS', 'AUD', 'AWG', 'AZN', 'BAM', 'BBD', 'BDT',
        'BGN', 'BHD', 'BIF', 'BMD', 'BND', 'BOB', 'BOV', 'BRL', 'BSD', 'BTN', 'BWP', 'BYR', 'BZD',
        'CAD', 'CDF', 'CHE', 'CHF', 'CHW', 'CLF', 'CLP', 'CNH', 'CNY', 'COP', 'COU', 'CRC', 'CUC',
        'CUP', 'CVE', 'CZK', 'DJF', 'DKK', 'DOP', 'DZD', 'EGP', 'ERN', 'ETB', 'EUR', 'FJD', 'FKP',
        'GBP', 'GEL', 'GHS', 'GIP', 'GMD', 'GNF', 'GTQ', 'GYD', 'HKD', 'HNL', 'HRK', 'HTG', 'HUF',
        'IDR', 'ILS', 'INR', 'IQD', 'IRR', 'ISK', 'JMD', 'JOD', 'JPY', 'KES', 'KGS', 'KHR', 'KMF',
        'KPW', 'KRW', 'KWD', 'KYD', 'KZT', 'LAK', 'LBP', 'LKR', 'LRD', 'LSL', 'LTL', 'LYD', 'MAD',
        'MDL', 'MGA', 'MKD', 'MMK', 'MNT', 'MOP', 'MRO', 'MUR', 'MVR', 'MWK', 'MXN', 'MXV', 'MYR',
        'MZN', 'NAD', 'NGN', 'NIO', 'NOK', 'NPR', 'NZD', 'OMR', 'PAB', 'PEN', 'PGK', 'PHP', 'PKR',
        'PLN', 'PYG', 'QAR', 'RON', 'RSD', 'RUB', 'RWF', 'SAR', 'SBD', 'SCR', 'SDG', 'SEK', 'SGD',
        'SHP', 'SLL', 'SOS', 'SRD', 'SSP', 'STD', 'SYP', 'SZL', 'THB', 'TJS', 'TMT', 'TND', 'TOP',
        'TRY', 'TTD', 'TWD', 'TZS', 'UAH', 'UGX', 'USD', 'USN', 'USS', 'UYI', 'UYU', 'UZS', 'VEF',
        'VND', 'VUV', 'WST', 'XAF', 'XAG', 'XAU', 'XBA', 'XBB', 'XBC', 'XBD', 'XCD', 'XDR', 'XFU',
        'XOF', 'XPD', 'XPF', 'XPT', 'XSU', 'XTS', 'XUA', 'XXX', 'YER', 'ZAR', 'ZMW', 'ZWD',
    ]
    price = None
    currency = None

    def __init__(self, currency, price):
        """This class implements standard field for price.

            Args:
                currency (str, unicode): Currency code from list CURRENCY_CODES.
                price (str, float, int): Price in string format (123.00), float or integer.

            Returns:
                Price: price object
                """
        assert (isinstance(currency, six.string_types) and currency in self.CURRENCY_CODES) or currency is None
        assert isinstance(price, (six.string_types, float, int)) or price is None

        self.currency = currency
        if price:
            if isinstance(price, unicode):
                price = price.encode('utf-8')
            price = str(price)
            if re.search(self.REGEXP, str(price)):
                self.price = float(price.replace(',', ''))

    def __repr__(self):
        return u'{}(currency={}, price={})'.format(
            self.__class__.__name__,
            self.currency,
            self.price
        )

    def __str__(self):
        return self.__repr__()

    # "==" operator implementation
    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)

    @staticmethod
    def serializer(value):
        """ This method is required to correctly dump values while using JSON
            output (otherwise we'd have "can not serialize to JSON" error).
            `value` can be a string, number, or a `Price` instance.
        :param value: str, float, int, or a `Price` instance
        :return: str
        """
        if isinstance(value, Price):
            return value.__str__()
        return value


class BuyerReviews(dict):
    AVERAGE_ACCURACY = 2

    def __init__(self, stars, count=None, average=None, last_review_date=None):
        """This class implements standard field for buyer reviews.
            You should pass correct `stars` dictionary and other parameters will be auto-filled based on `stars`
            Also, you can assign your own values for count and average.

            Args:
                stars (dict {int, int}): Dictionary with stars and values, e.g {1: 10, 2: 20, 3: 30, 4: 40, 5: 50}.
                    Be careful with dictionary key `0`, it may cause wrong value of average. Instead use average param.
                count (int, optional): Count of reviews.
                average (str, optional): Average rating.
                last_review_date (datetime.date or str, optional): Last review date.

            Returns:
                BuyerReviews: buyer reviews object
        """
        assert isinstance(stars, dict)
        for k, v in stars.items():
            assert isinstance(k, int) and isinstance(v, int)
        assert isinstance(count, int) or count is None
        assert isinstance(average, (int, float)) or average is None
        assert isinstance(last_review_date, datetime.date) or last_review_date is None

        # Try to get reviews count from stars
        if not count:
            count = self.get_count(stars)

        # Try to get reviews average from stars and count
        if not average and count:
            average = self.get_average(stars, count)

        super(BuyerReviews, self).__init__({
            'min': min(stars.keys() or [None]),
            'max': max(stars.keys() or [None]),
            'stars': stars,
            'count': count,
            'average': round(average, self.AVERAGE_ACCURACY),
            'last_review_date': last_review_date
        })

    # Override for custom behaviour
    def get_count(self, stars):
        return sum(stars.values())

    # Override for custom behaviour
    def get_average(self, stars, count):
        return sum([k * v * 1. for k, v in stars.items()]) / count

    def __repr__(self):
        return u'{}(min={}, max={}, stars={}, count={}, average={}, last_review_date={})'.format(
            self.__class__.__name__,
            self.get('min'),
            self.get('max'),
            self.get('stars'),
            self.get('count'),
            self.get('average'),
            self.get('last_review_date'),
        )

    def __str__(self):
        return self.__repr__()


class HTags(dict):
    def __init__(self, h1=None, h2=None):
        assert isinstance(h1, list) or h1 is None
        assert isinstance(h2, list) or h2 is None

        super(HTags, self).__init__({'h1': h1, 'h2': h2})


class Meta(dict):
    def __init__(
            self,
            charset=None,
            canonical_url=None,
            browser_title=None,
            keywords=None,
            description=None,
            meta_tags=None
    ):
        assert isinstance(charset, six.string_types) or charset is None
        assert isinstance(canonical_url, six.string_types) or canonical_url is None
        assert isinstance(browser_title, six.string_types) or browser_title is None
        assert isinstance(keywords, six.string_types) or keywords is None
        assert isinstance(description, six.string_types) or description is None
        assert isinstance(meta_tags, list) or meta_tags is None

        # TODO: add support for special meta-tags like: charset, http-equiv, custom
        super(Meta, self).__init__({
            'charset': charset,
            'canonical_url': canonical_url,
            'browser_title': browser_title,
            'keywords': keywords,
            'description': description,
            'meta_tags': meta_tags,
        })


class Variants(dict):
    def __init__(self, *args, **kwargs):
        super(Variants, self).__init__(*args, **kwargs)
        raise NotImplementedError


class Marketplace(dict):
    def __init__(self, name=None, price=None, currency=None):
        assert isinstance(name, six.string_types) or name is None
        assert isinstance(price, (float, int)) or price is None
        assert isinstance(currency, six.string_types) or currency is None

        super(Marketplace, self).__init__({
            'name': name,
            'price': price,
            'currency': currency
        })

    # def __repr__(self):
    #     return u'{}(name={}, price={})'.format(
    #         self.__class__.__name__,
    #         self.get('name'),
    #         self.get('price'),
    #     )
    #
    # def __str__(self):
    #     return self.__repr__()

    @staticmethod
    def serializer(values):
        _values = []
        for value in values:
            price = value.get('price', None)
            if isinstance(price, Price):
                value['price'] = price.__str__()
            _values.append(value)
        return _values


class BaseProductItem(Item):
    url = Field()							# (str) valid full product url
    title = Field()							# (str) product title from page


class SiteProductItem(BaseProductItem):

    def __repr__(self):
        """Supress dumping whole item into logs"""
        return "***ITEM***"

    # product general field
    site = Field()							# (str) website domain
    ranking = Field()						# (int) product ranking on a page
    search_term = Field()					# (str) search term which featured product
    total_matches = Field()					# (int) total matches of products for search term
    results_per_page = Field()				# (int) number of actually scraped products/links
    scraped_results_per_page = Field()		# (int) number of products showed on page
    is_single_result = Field()				# may be removed as unnecessary
    invalid_url = Field()                   # (bool) whether url is invalid
    redirect = Field()                      # (bool) url redirects to a different product

    # Those description fields were renamed to match UI naming
    shelf_description = Field()             # (str) product shelf description
    short_description = Field()             # (str) product short description
    long_description = Field()				# (str) product long description, most detailed

    shipping = Field()                      # (bool) product can be shipped
    shipping_speed = Field()                # (str) time to arrive, e.g. '4 days'

    # product identification
    upc = Field()							# (str) product UPC code
    gtin = Field()							# (str) upc with 14 digits
    sku = Field()							# (str) product SKU code
    model = Field()							# (str) model number
    product_id = Field()					# need to agree with PHP frontend devs about this - see technical middleware
    reseller_id = Field()					# id of the product that is used on frontend to identify a product
    site_product_id = Field()               # same as reseller id, but for CH
    walmart_no = Field()                    # (str) walmart item number
    wupc = Field()                          # (str) wupc
    secondary_id = Field()                  # (str) sku in jet, dpci in target, walmart number in walmart shelf

    # product categorization
    brand = Field()							# (str) product brand (e.g. Nike, Adidas etc.)
    department = Field()					# (str) product department (e.g. Running)
    departments = Field()					# (list) product all nested departments (e.g ['Shoes', 'Men\'s', 'Running'])

    # product pricing
    price = Field()                                     # (str) product price with valid currency
    price_amount = Field()					            # (float) product price in float
    price_currency = Field()				            # (str) product valid currency
    price_highest = Field(serializer=Price.serializer)  # (Price) the highest product price, optional
    price_lowest = Field(serializer=Price.serializer)   # (Price) the lowest product price, optional
    special_pricing = Field()
    was_price = Field()                                 # (str) the was price of the was_now field
    now_price = Field()                                 # (str) the now price of the was_now field
    list_price = Field()                                # (str) the list (MSRP) price of the product
    volume_measure = Field()                            # (str) volume measure for price
    price_per_volume = Field()                          # (float) product price per volume in float

    # product availability
    not_found = Field()						# (bool) product is not founded (403, 404?)
    low_stock = Field()						# (bool) is product low stock
    is_out_of_stock = Field()				# (bool) is product out of stock
    is_in_store_only = Field()				# (bool) is product available only in offline store
    no_longer_available = Field()			# (bool) product exists, but no longer available
    temporary_unavailable = Field()			# (bool) product exists, but temporary unavailable
    # item_not_available = Field()			# is it similar to is_out_of_stock? may be removed
    store = Field()                         # (str) store id set, scraped from page if possible
    zip_code = Field()                      # (str) zip_code set, scraped from page if possible

    # product media
    pdf_urls = Field()					    # (list) urls of pdfs available on the product page
    image_url = Field()						# (str) full and valid URL of product main image
    image_urls = Field()					# (list) full and valid URLs of product images
    image_alts = Field()					# (list) all alternative texts of images from list `image_urls`
    image_res = Field()				        # (list(list(int))) image dimensions of each image in `image_urls`
    image_dimensions = Field()				# (list(int)) whether each image in `image_urls` is larger than 500x500
    zoom_image_dimensions = Field()			# (list(int)) whether each image in `image_urls` is zoomable (has larger size)
    video_urls = Field()					# (list) full and valid URL of product videos

    # sometimes it is impossible to extract all video urls,
    # but still possible for the scraper to know how many videos are available
    video_count = Field()					# (int) count of product videos

    # sellpoints and webcollage
    sellpoints = Field()                    # (bool) sellpoints content is present on page
    wc_360 = Field()                        # (bool) webcollage 360 view is present on page
    wc_emc = Field()                        # (bool) webcollage extended manufacturer content is present on page
    wc_pdf = Field()                        # (bool) webcollage pdf is present on page
    wc_prodtour = Field()                   # (bool) webcollage product tour is present on page
    wc_video = Field()                      # (bool) webcollage video is present on page
    webcollage = Field()                    # (bool) any webcollage content is present on page
    webcollage_image_urls = Field()         # (list) webcollage image urls
    webcollage_images_count = Field()       # (int) number of webcollage images
    webcollage_pdfs_count = Field()         # (int) number of webcollage pdfs
    webcollage_video_urls = Field()         # (int) webcollage video urls
    webcollage_videos_count = Field()       # (int) number of webcollage videos

    # technical fields
    _date = Field()							# (datetime.date) current date
    _redirect = Field()						# (bool) page was redirected while parsing
    _response_code = Field()				# (int) valid server response code
    _loaded_in_seconds = Field()			# (float) how many seconds request take
    _statistics = Field()					# (dict) common statistic of host machine

    # search term fields
    search_term_in_title_partial = Field()			# (bool) is search term is in title partial
    search_term_in_title_exactly = Field()			# (bool) is search term is in title exactly
    search_term_in_title_interleaved = Field()		# (bool) is search term is in title interleaved

    # other fields
    meta = Field()							# (Meta) all meta fields from a page
    htags = Field()							# (HTags) all headings from a page
    bundle = Field()                        # (bool) whether it is a bundle page
    bullets = Field()						# (Bullets) all bullets from a page
    directions = Field()					# (str) product directions
    warnings = Field()					    # (str) product warnings
    features = Field()						# (list) all product features
    ingredients = Field()					# (list) product ingredients
    specs = Field()						    # (dict) product specifications
    swatches = Field()						# (Swatches) all product swatches
    variants = Field()
    buyer_reviews = Field()                 # (BuyerReviews) buyer reviews
    nutrition_fact_count = Field()          # (int) number of nutrition facts
    nutrition_fact_text_health = Field()    # (int) 0: nutrition facts not present, 1: nutrition facts present, 2: error
    collection_count = Field()              # (int) number of items on a collection page
    collection_availability = Field()       # (list(bool)) availability of items on a collection page
    rich_content = Field()                  # (bool) rich content is present on page

    # sellers
    in_stores = Field()                     # (bool) whether the product is sold in stores
    in_stores_out_of_stock = Field()        # (bool) whether the product is out of stock in stores
    # CH Marketplace is a temporary field used in exporters.py, but should be removed in the future
    _ch_marketplace = Field()               # (list(dict)) ch marketplace info
    marketplace = Field()                   # (list(dict)) marketplace info
    marketplace_bool = Field()              # (bool) whether the product is sold by a marketplace seller
    owned = Field()                         # (bool) whether the product is owned by the site
    primary_seller = Field()                # (str) primary seller
    seller_id = Field()                     # (str) id of primary seller
    site_online = Field()                   # (bool) whether the product is sold online
    site_online_out_of_stock = Field()      # (bool) whether the product is out of stock online
    temp_price_cut = Field()                # (bool) whether the product is on sale
    us_seller_id = Field()                  # (str) us seller id
    promotions = Field()                    # (bool) whether the product is on promotion

    screenshot = Field()                    # (str) base64-encoded image when run with make_screenshots=True
    crawl_date = Field()                    # (str) date of crawling if cache is turned on: yyyy-mm-dd
