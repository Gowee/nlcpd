import scrapy
import re
import logging
from urllib.parse import urljoin, urlparse, parse_qs

from ..items import CategoryItem


class CategorySpider(scrapy.Spider):
    name = "category"
    allowed_domains = ["read.nlc.cn"]
    start_urls = ["http://read.nlc.cn/user/category"]
    # custom_settings = {'ITEM_PIPELINES': {}}

    REGEX_SEARCH_TYPE = re.compile(r"searchType=(\d+)&")

    def parse(self, response):
        body = response.css(".YMH2019_New_ZYFL_body .YMH2019_New_main")[0]
        for module in body.css("[class^=module]"):
            pcategory_name = module.css(".m_top .tt::text").get()
            for category in module.css("ul li a"):
                name = category.css("span::text").get().strip()
                url = category.attrib["href"]
                icon_url = category.css("img")[0].attrib["src"]
                if (m := self.REGEX_SEARCH_TYPE.search(url)) is None:
                    self.log(f"Category {name} has unrecognized URL pattern: {url}")
                    continue
                id = int(m[1])

                yield response.follow(
                    urljoin(self.start_urls[0], url),
                    callback=self.parse_category,
                    meta={
                        "category": CategoryItem(
                            id=id,
                            name=name,
                            collection_name=None,
                            description=None,
                            icon_url=icon_url,
                            parental_category_name=pcategory_name,
                        )
                    },
                )

    def parse_category(self, response):
        category = response.meta["category"]
        description = response.css(".YMH2019_New_GJG_DataJJ .txt p::text").get()
        if description:
            category.description = description.strip()
        first_book_link = response.css(
            'ul > li:first-child a[href*="searchDetail"][href*="indexName"]'
        )
        if first_book_link:
            category.collection_name = parse_qs(
                urlparse(first_book_link.attrib["href"]).query
            ).get("indexName")[0]
        else:
            self.log(f"No book found under {category.name}({category.id})")
        yield category
