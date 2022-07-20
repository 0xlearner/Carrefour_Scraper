from scrapy.exporters import CsvItemExporter
import os


class HeadlessCsvItemExporter(CsvItemExporter):
    def __init__(self, *args, **kwargs):

        # args[0] is (opened) file handler
        # if file is not empty then skip headers
        if os.fstat(args[0].fileno()).st_size > 0:
            kwargs["include_headers_line"] = False

        super(HeadlessCsvItemExporter, self).__init__(*args, **kwargs)
