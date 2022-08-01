# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.http import Request
from scrapy import signals
from urllib.parse import unquote
import os
from scrapy.exporters import CsvItemExporter
import pandas as pd
import csv
import uuid

from scrapy.pipelines.images import ImagesPipeline


def dedup_csv_header(fname, fname_new):
    if not os.path.exists(fname):
        print("csv file not exist:", fname)
        return

    print(f"dedup csv headers, file {fname} to {fname_new}")
    fnew = open(fname_new, "w", encoding="utf8")

    with open(fname, "r", encoding="utf8") as f:
        header = None
        first = True
        for line in f:
            if None == header:
                header = line

            if not first and header == line:
                continue
            fnew.write(line)
            first = False

    fnew.close()


class CarrefourSpiderPipeline:
    def process_item(self, item, spider):
        return item


class CustomCarrefourImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.images_urls_field, [])
        return [Request(u) for u in urls]

    def file_path(self, request, response=None, info=None, *, item=None):
        file_name = os.path.basename(unquote(request.url))
        img_name = ""
        if ".jpg" in file_name:
            split_name = file_name.index(".jpg")
            img_name = file_name[:split_name] + ".jpg"
        if ".jpg" not in file_name:
            img_name = file_name + ".jpg"
        return item["path"] + img_name


class CustomCarrefourCsvPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        self.file = open("%s-items.csv" % spider.name, "ab")
        self.files[spider] = self.file
        self.exporter = CsvItemExporter(self.file)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        dedup_csv_header(
            "%s-items.csv" % spider.name, "%s-items-dropped-headers.csv" % spider.name
        )
        df = pd.read_csv("%s-items-dropped-headers.csv" % spider.name, skiprows=0)
        drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
        sorted_df = drop_cols.sort_values(by=["catalog_uuid"])
        output_path = "%s-items-final.csv" % spider.name
        with open(output_path, "a") as f:
            sorted_df.to_csv(
                output_path,
                mode="a",
                encoding="utf-8",
                header=f.tell() == 0,
                index=False,
            )
