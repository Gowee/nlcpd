# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


# class NlccrawlerPipeline:
#     def process_item(self, item, spider):
#         return item

# Adapted from: https://github.com/Gowee/NEMUserCrawler/blob/9f0cc86933937bb965e561523f40962a8eb2a9fc/NEMUserCrawler/pipelines.py

import logging
from urllib.parse import urlparse
import txmongo
from pymongo.uri_parser import parse_uri
from pymongo.errors import DuplicateKeyError, BulkWriteError
from twisted.internet import defer, ssl
from scrapy.exceptions import NotConfigured
from pymongo import InsertOne, UpdateOne


class TxMongoPipeline(object):
    mongo_uri = "mongodb://localhost:27017"  # default

    def __init__(self, mongo_uri, db_name, buffer_size=0):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.mongo_uri = mongo_uri or self.mongo_uri
        self.db_name = db_name or parse_uri(self.mongo_uri)["database"]

        self.buffer_size = buffer_size
        if buffer_size > 0:
            self.buffer = {}
            self.buffer_count = 0

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            db_name=crawler.settings.get("MONGO_DB"),
            buffer_size=crawler.settings.getint("MONGO_BUFFER_SIZE", 0),
        )

    @defer.inlineCallbacks
    def open_spider(self, spider):
        try:
            # TODO: get db_name from `spider.name`
            self.db_name = spider.database_name or self.db_name
        except AttributeError:
            pass
        if self.db_name is None:
            e = (
                "Database name for {} is not specified."
                "It can be specified thru `MONGO_URI` or `MONGO_DB` in settings"
                " or the `database_name` attribute of spiders".format(
                    self.__class__.__name__
                )
            )
            self.logger.error(e)
            raise NotConfigured(e)
        self.logger.info(
            "TxMongoPipeline activated, uri: {}, database: {}, buffer size: {}.".format(
                self.mongo_uri, self.db_name, self.buffer_size
            )
        )
        # https://github.com/twisted/txmongo/issues/236
        self.connection = yield txmongo.connection.ConnectionPool(
            self.mongo_uri,
            ssl_context_factory=ssl.optionsForClientTLS(
                parse_uri(self.mongo_uri)["nodelist"][0][0]
            ),
        )
        self.db = self.connection[self.db_name]

    @defer.inlineCallbacks
    def close_spider(self, spider):
        if hasattr(self, "buffer") and self.buffer:
            yield self.flush_buffer()
        if self.connection:
            yield self.connection.disconnect()

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        try:
            collection_name = item._collection_name
        except AttributeError:
            # not a mongo item
            return item

        processed_item = ItemAdapter(item).asdict()
        if item._to__id:
            # use the field name specified in `_to__id` as `_id` in MongoDB
            _id = processed_item.pop(item._to__id)
            processed_item["_id"] = _id
            # Now, the name of the field specified by `item._to__id` is changed to the value of `_item.to__id`.

        # `upsert` here: denotes whether the insert operation is to use `insert_one` or `update` with `upsert=True`
        # in the former case, DuplicateKeyError may be raised
        upsert_spec = (
            {field: processed_item[field] for field in item._upsert_index}
            if item._upsert_index
            else None
        )
        # TODO: test error handling
        if self.buffer_size:
            # buffer enabled
            if self.buffer_count >= self.buffer_size:
                result = yield self.flush_buffer()
            else:
                operation = (
                    UpdateOne(upsert_spec, {"$set": processed_item}, upsert=True)
                    if upsert_spec
                    else InsertOne(processed_item)
                )
                self.buffer.setdefault(collection_name, []).append(operation)
                self.buffer_count += 1
        else:
            # buffer disabled
            if upsert_spec:
                result = yield self.db[collection_name].update(
                    upsert_spec, processed_item, upsert=True
                )
            else:
                try:
                    result = yield self.db[collection_name].insert_one(processed_item)
                except DuplicateKeyError as e:
                    self.logger.warn(
                        "{!r} raised when handling {}: {}. "
                        "Consider using `to__id` with `upsert=True`".format(
                            e, collection_name, processed_item
                        )
                    )
                    result = e
        spider.crawler.stats.inc_value(
            "pipeline/txmongo/{}".format(collection_name), spider=spider
        )
        defer.returnValue(item)

    @defer.inlineCallbacks
    def flush_buffer(self):
        results = []
        buffer = (
            self.buffer.copy()
        )  # execution flow switched to other coroutines when bulk write
        self.buffer.clear()
        self.buffer_count = 0
        for collection_name, operations in buffer.items():
            try:
                result = yield self.db[collection_name].bulk_write(
                    operations, ordered=False
                )
                self.logger.debug(
                    "Buffer flushed, {} for collection {}: {}".format(
                        len(operations), collection_name, result.bulk_api_result
                    )
                )
            except BulkWriteError as e:
                self.logger.error("{!r} when writing buffer: {}".format(e, e.details))
                result = e.details
                results.append(result)
        defer.returnValue(results)
