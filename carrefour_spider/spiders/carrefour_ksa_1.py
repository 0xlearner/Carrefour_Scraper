import scrapy
from scrapy import signals
import uuid
import json
from urllib.parse import urlencode, unquote
import pandas as pd
import os


API_KEY = os.environ.get("API_KEY")


def get_scraperapi_url(url):
    payload = {
        "api_key": API_KEY,
        "url": url,
    }
    proxy_url = "http://api.scraperapi.com/?" + urlencode(payload)
    return proxy_url


class CarrefourKSA(scrapy.Spider):
    name = "carrefour-ksa-1"

    custom_settings = {
        "FEED_FORMAT": "csv",
        "FEED_URI": "carrefour-ksa-1.csv",
        "LOG_FILE": "carrefour-ksa-1.log",
        # "IMAGES_STORE": catalouge_id,
    }

    headers = {
        "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="100", "Yandex";v="22"',
        "tracestate": "3355720@nr=0-1-3355720-1021845705-72a4dc2922710b2a----1656355603002",
        "env": "prod",
        "newrelic": "eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjMzNTU3MjAiLCJhcCI6IjEwMjE4NDU3MDUiLCJpZCI6IjcyYTRkYzI5MjI3MTBiMmEiLCJ0ciI6ImZmZDkzYzdhNTYxMTlkZTk1ZTBlMjMxYjBmMGZkOGJjIiwidGkiOjE2NTYzNTU2MDMwMDJ9fQ==",
        "lang": "en",
        "userId": "anonymous",
        "X-Requested-With": "XMLHttpRequest",
        "storeId": "mafsau",
        "sec-ch-ua-platform": '"Linux"',
        "traceparent": "00-ffd93c7a56119de95e0e231b0f0fd8bc-72a4dc2922710b2a-01",
        "sec-ch-ua-mobile": "?0",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.143 YaBrowser/22.5.0.1879 (beta) Yowser/2.5 Safari/537.36",
        "langCode": "en",
        "appId": "Reactweb",
    }

    pd_data = []

    def start_requests(self):
        categories = ["NFKSA1200000"]
        languages = ["en", "ar"]

        for lang in languages:
            for category in categories:
                yield scrapy.Request(
                    url=get_scraperapi_url(
                        f"https://www.carrefourksa.com/mafsau/{lang}/c/{category}?currentPage=0&filter=&nextPageOffset=0&pageSize=60&sortBy=relevance"
                    ),
                    headers=self.headers,
                    callback=self.parse_links,
                )

    def parse_links(self, response):

        product_listings = response.css("div.css-1itwyrf ::attr(href)").extract()

        for product_link in product_listings:
            product_url = "https://www.carrefourksa.com/" + product_link

            yield scrapy.Request(
                url=get_scraperapi_url(product_url),
                headers=self.headers,
                callback=self.parse_product,
            )

    def parse_product(self, response):
        data = (
            response.css('script[id="__NEXT_DATA__"]')
            .get()
            .replace('<script id="__NEXT_DATA__" type="application/json">', "")
            .replace("</script>", "")
        )
        json_data = json.loads(data)
        link_url = unquote(response.url)
        LabebStoreId = "6019"
        catalog_uuid = ""
        lang = ""
        if "/en/" in link_url:
            lang = "en"
        if "/ar/" in link_url:
            lang = "ar"
        breadcrumb = response.css("div.css-iamwo8 > a::text").extract()[1:]
        for idx, cat in enumerate(breadcrumb):
            bc_no = f"cat_{idx}_name"
            bc = breadcrumb[idx]
        catalogname = response.css("h1.css-106scfp::text").get()
        try:
            description = ", ".join(response.css("div.css-16lm0vc ::text").getall())
        except:
            description = ""
        try:
            keys = response.css("div.css-pi51ey::text").getall()
            values = response.css("h3.css-1ps12pz::text").getall()
            properties = {keys[i]: values[i] for i in range(len(keys))}
            raw_properties = json.dumps(properties, ensure_ascii=False).encode("utf-8")
            properties = raw_properties.decode()
        except:
            properties = ""
        try:
            price = response.css("h2.css-1i90gmp::text").getall()[2]
        except:
            price = response.css("h2.css-17ctnp::text").getall()[2]
        try:
            price_before_discount = response.css("del.css-1bdwabt::text").getall()[2]
        except:
            price_before_discount = ""
        externallink = link_url.split("=")[2]
        Rating = ""
        delivery = response.css("span.css-u98ylp::text").get()
        try:
            discount = f'{json_data["props"]["initialProps"]["pageProps"]["initialData"]["products"][0]["offers"][0]["stores"][0]["price"]["discount"]["information"]["amount"]}%'
        except:
            discount = ""
        self.pd_data.append(
            {
                "LabebStoreId": LabebStoreId,
                "catalog_uuid": catalog_uuid,
                "lang": lang,
                bc_no: bc,
                "catalogname": catalogname,
                "description": description,
                "properties": properties,
                "price": price,
                "price_before_discount": price_before_discount,
                "externallink": externallink,
                "Rating": Rating,
                "delivery": delivery,
                "discount": discount,
            }
        )

        df = pd.DataFrame(self.pd_data)
        df.to_csv("carrefour-ksa.csv", index=False)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        df_2 = pd.read_csv("carrefour-ksa.csv")
        sorted = df_2.externallink.str[-6:].sort_values()
        sort = sorted.index
        df_orgnized = df_2.iloc[sort]
        df_orgnized.catalog_uuid = df_orgnized.externallink.str[-6:]
        df_orgnized.to_csv("carrefour-ksa-final.csv", encoding="utf-16")
