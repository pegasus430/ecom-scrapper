# README #

Repo for new Scrapers

There are (multiple) queues per spider:
* `scraper_<spider_name>_in`                      - Requests for the spider to process
* `scraper_<spider_name>_in_urgent`               - Requests for the spider to process

Build the docker container:
```
docker build -t content-analytics-scrapy .
```

Then to run a specific spider:

```
docker run --rm content-analytics-scrapy -s <spider-name> -k <aws-key> -c <aws-secret>
```

For example, to run walmart spider:
```
docker run --rm content-analytics-scrapy -s walmart -k AZIBI2UZ2GCRUK2M2R -c "uzkesAlsHnUbiEpWAn7ZVm6Oa3bK0KTk3UKttrxf"
```


