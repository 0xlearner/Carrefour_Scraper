import scrapy
import json
from urllib.parse import urlencode, unquote


API_KEY = "c88c857a7aa4b2ad0463789c8f4a90f9"


def get_scraperapi_url(url):
    payload = {
        "api_key": API_KEY,
        "url": url,
    }
    proxy_url = "http://api.scraperapi.com/?" + urlencode(payload)
    return proxy_url


class CarrefourKSA(scrapy.Spider):
    name = "carrefour-ksa"

    custom_settings = {
        "LOG_FILE": "carrefour-ksa.log",
        "IMAGES_STORE": "images",
        "ITEM_PIPELINES": {
            "carrefour_spider.pipelines.CustomCarrefourImagesPipeline": 1,
            "carrefour_spider.pipelines.CustomCarrefourCsvPipeline": 300,
        },
        # "FEED_EXPORTERS": {
        #     "csv": "carrefour_spider.exporters.HeadlessCsvItemExporter",
        # },
    }

    headers = {
        "authority": "www.carrefourksa.com",
        "accept": "*/*",
        "accept-language": "en,ru;q=0.9",
        "appid": "Reactweb",
        "credentials": "include",
        "deviceid": "1042241961.1656074849",
        "env": "prod",
        "if-modified-since": "Fri, 01 Jul 2022 22:01:21 GMT",
        "if-none-match": 'W/"23af1-32ToVtn5m6ahtiiBp6hXfviKyZI"',
        "newrelic": "eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjMzNTU3MjAiLCJhcCI6IjEwMjE4NDU3MDUiLCJpZCI6ImVkZjBmYzY1ZjcyMmIwMGIiLCJ0ciI6ImE2ZWViOGIxMDZmYjg5NDFmMWVmNTk4YWFiMmYxNDVmIiwidGkiOjE2NTY3MTMzODg4ODV9fQ==",
        "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="100", "Yandex";v="22"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "storeid": "mafsau",
        "token": "undefined",
        "traceparent": "00-a6eeb8b106fb8941f1ef598aab2f145f-edf0fc65f722b00b-01",
        "tracestate": "3355720@nr=0-1-3355720-1021845705-edf0fc65f722b00b----1656713388885",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.143 YaBrowser/22.5.0.1879 (beta) Yowser/2.5 Safari/537.36",
        "userid": "undefined",
    }

    params = {
        "filter": "",
        "sortBy": "relevance",
        "currentPage": "0",
        "pageSize": "60",
        "maxPrice": "",
        "minPrice": "",
        "areaCode": "Granada - Riyadh",
        "lang": "en",
        "displayCurr": "SAR",
        "latitude": "24.7136",
        "longitude": "46.6753",
        "nextOffset": "0",
        "requireSponsProducts": "true",
        "responseWithCatTree": "true",
        "depth": "3",
    }

    def start_requests(self):
        categories = [
            "NFKSA4000000",
            "NFKSA1200000",
            "NFKSA2000000",
            "NFKSA2300000",
            "FKSA1000000",
            "NFKSA3000000",
            "NFKSA5000000",
            "NFKSA8000000",
            "NFKSA7000000",
            "NFKSA1400000",
        ]
        languages = ["en", "ar"]

        for lang in languages:
            self.params["lang"] = lang
            for category in categories:
                base_url = (
                    f"https://www.carrefourksa.com/api/v7/categories/{category}?"
                    + urlencode(self.params)
                )
                yield scrapy.Request(
                    url=base_url,
                    headers=self.headers,
                    callback=self.parse_links,
                    meta={"category": category},
                )

    def parse_links(self, response):

        product_listings = response.json()["products"]
        current_page = response.json()["pagination"]["currentPage"]
        total_pages = response.json()["pagination"]["totalPages"]
        cat = response.meta.get("category")

        if current_page == 0:
            for i in range(1, total_pages):
                self.params["currentPage"] = i
                next_url = (
                    f"https://www.carrefourksa.com/api/v7/categories/{cat}?"
                    + urlencode(self.params)
                )
                yield scrapy.Request(
                    url=next_url,
                    headers=self.headers,
                    callback=self.parse_links,
                )
        for product_link in product_listings:
            product_url = (
                "https://www.carrefourksa.com/"
                + product_link["links"]["productUrl"]["href"]
            )

            yield scrapy.Request(
                url=get_scraperapi_url(product_url),
                headers=self.headers,
                callback=self.parse_product,
            )

    def parse_product(self, response):
        item = {}
        data = (
            response.css('script[id="__NEXT_DATA__"]')
            .get()
            .replace('<script id="__NEXT_DATA__" type="application/json">', "")
            .replace("</script>", "")
        )
        json_data = json.loads(data)
        link_url = unquote(response.url)
        item["LabebStoreId"] = "5638"
        item["catalog_uuid"] = ""

        item["lang"] = ""
        if "/en/" in link_url:
            item["lang"] = "en"
        if "/ar/" in link_url:
            item["lang"] = "ar"
        breadcrumb = response.css("div.css-iamwo8 > a::text").extract()[1:]
        for idx, cat in enumerate(breadcrumb):
            item[f"cat_{idx}_name"] = breadcrumb[idx]
        item["catalogname"] = response.css("h1.css-106scfp::text").get()
        try:
            item["description"] = ", ".join(
                response.css("div.css-16lm0vc ::text").getall()
            )
        except:
            item["description"] = ""
        raw_images = response.css("div.css-1c2pck7 ::attr(src)").getall()
        clean_image_url = []

        for img_url in raw_images:
            clean_image_url.append(response.urljoin(img_url))

        item["image_urls"] = clean_image_url

        try:
            keys = response.css("div.css-pi51ey::text").getall()
            values = response.css("h3.css-1ps12pz::text").getall()
            properties = {keys[i]: values[i] for i in range(len(keys))}
            raw_properties = json.dumps(properties, ensure_ascii=False).encode("utf-8")
            item["properties"] = raw_properties.decode()
        except:
            item["properties"] = ""
        try:
            item["price"] = response.css("h2.css-1i90gmp::text").getall()[2]
        except:
            item["price"] = response.css("h2.css-17ctnp::text").getall()[2]
        try:
            item["price_before_discount"] = response.css(
                "del.css-1bdwabt::text"
            ).getall()[2]
        except:
            item["price_before_discount"] = ""
        item["externallink"] = link_url.split("=")[2]
        item["catalog_uuid"] = item["externallink"].split("/")[-1]
        item["path"] = f'catalouge_{item["catalog_uuid"]}/'
        item["Rating"] = ""
        item["delivery"] = response.css("span.css-u98ylp::text").get()
        try:
            item[
                "discount"
            ] = f'{json_data["props"]["initialProps"]["pageProps"]["initialData"]["products"][0]["offers"][0]["stores"][0]["price"]["discount"]["information"]["amount"]}%'
        except:
            item["discount"] = ""
        try:
            item["instock"] = response.css("div.css-g4iap9::text").extract()[1]
        except:
            item["instock"] = ""
        yield item
