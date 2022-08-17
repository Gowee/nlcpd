import scrapy
import logging
import re
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode
from itertools import chain
from copy import copy

from ..items import BookItem, VolumeItem, PageItem


class BookSpider(scrapy.Spider):
    name = "book"
    allowed_domains = ["read.nlc.cn"]

    URL_LIST_PAGE = "http://read.nlc.cn/allSearch/searchList?searchType={category}&showType=1&pageNo={page}"
    URL_VOLUME_READER = "http://read.nlc.cn/OutOpenBook/OpenObjectBook?aid={collection_id}&bid={volume_id}"
    URL_VOLUME_TOC = "http://read.nlc.cn/allSearch/formatCatalog"

    REGEX_PDFNAME_IN_READER = re.compile(r"var pdfname= '(.+?)';")

    PRIO_LIST_PAGE = 10
    PRIO_BOOK_INFO = 20
    PRIO_VOLUME = 30

    def __init__(
        self, category, starting_page=1, no_book=False, no_volume=False, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.category = category
        self.starting_page = starting_page
        self.no_book = no_book
        self.no_volume = no_volume

    def start_requests(self):
        yield scrapy.Request(
            self.URL_LIST_PAGE.format(category=self.category, page=self.starting_page),
            meta={"page": self.starting_page},
            dont_filter=True,
            callback=self.parse_list_page,
        )

    def parse_list_page(self, response):
        # TODO: terminate on 404
        page = response.meta["page"]

        category_name = response.css("input#categoryName").attrib["value"]
        books_in_page = []
        idx = -1
        for idx, book in enumerate(
            response.css('ul > li a[href^="/allSearch/searchDetail"]:nth-of-type(1)')
        ):
            title = book.xpath("..").css(".tt").xpath("text()").get()
            brief = "\n".join(
                filter(
                    None,
                    map(
                        lambda s: s.strip(),
                        book.xpath("..").css(".txt *::text").getall(),
                    ),
                )
            )
            url = book.attrib["href"]
            url_params = dict(parse_qsl(urlparse(url).query))
            url = urljoin(self.URL_LIST_PAGE, url)
            try:
                collection_name = url_params["indexName"]
                book_id = url_params["fid"]
            except KeyError:
                self.log(f"Unrecognized book url {url} on page {page}", logging.INFO)
                continue
            # TODO: filter out placeholder cover image
            cover_image_url = book.css("img::attr(src)").get()

            books_in_page.append(
                {
                    "id": book_id,
                    "name": title,
                    "brief": brief,
                    "of_collection_name": collection_name,
                }
            )
            if not self.no_book:
                yield response.follow(
                    url,
                    meta={
                        "collection_name": collection_name,
                        "page": page,
                        "book_id": book_id,
                        "cover_image_url": cover_image_url,
                    },
                    priority=self.PRIO_BOOK_INFO,
                    callback=self.parse_book_info,
                )

        self.log(f"Got {idx + 1} books on page {page}")
        if idx != -1:
            # page contains > 0 books
            yield PageItem(
                no=page,
                books=books_in_page,
                of_category_id=self.category,
                of_category_name=category_name,
            )
            page += 1
            yield response.follow(
                self.URL_LIST_PAGE.format(category=self.category, page=page),
                meta={"page": page},
                priority=self.PRIO_LIST_PAGE,
                callback=self.parse_list_page,
            )

    def parse_book_info(self, response):
        page = response.meta["page"]
        collection_name = response.meta["collection_name"]
        book_id = response.meta["book_id"]
        cover_image_url = response.meta["cover_image_url"]
        # assert book_id == response.css("input#identifier::attr(value)").get() # input are not filled
        # assert collection_name == response.css("input#indexName::attr(value)").get()
        title = response.css("input#title::attr(value)").get().strip()
        author = response.css("input#author::attr(value)").get()
        keywords = response.css("input#Keyword::attr(value)").get(
            default=response.css("input#subject::attr(value)").get(default=None)
        )
        if keywords:
            keywords = keywords.replace("@@@", "").split("###")
        else:
            self.log(f"No keywrods found for {collection_name}, {book_id}")
            keywords = None if keywords is None else []
        category_name = (
            response.css('.YMH2019_New_MBX a[href*="/allSearch/searchList"]::text')
            .get(default="")
            .strip()
            or None
        )
        introduction = response.css(".SZZY2018_Book .ZhaiYao::text").get().strip()

        misc_metadata = {}
        for entry in response.css(".SZZY2018_Book .XiangXi label").xpath("string()"):
            entry_name, entry_value = map(str.strip, entry.get().split("：", maxsplit=1))
            misc_metadata[entry_name] = entry_value

        volumes = []

        for vidx, volume in enumerate(
            response.css("#multiple ul li") or [response.css("#single")]
        ):
            # possibly empty; aa: 云南图书馆, tt: 宋人文集
            volume_name = volume.css(".aa::text, .tt::text, span::text").get()
            volume_url = volume.css('a[href*="/OpenObjectBook"]::attr(href)').get()
            volume_url_params = dict(parse_qsl(urlparse(volume_url).query))

            assert "data_" + volume_url_params.get("aid") == collection_name
            # volume_id contains a trailing ".0" somewhere
            volume_id = volume_url_params["bid"].removesuffix(".0")
            # volume_url = urljoin(response.url, volume_url)

            if not self.no_volume:
                yield response.follow(
                    self.URL_VOLUME_READER.format(
                        collection_id=collection_name.removeprefix("data_"),
                        volume_id=volume_id,
                    ),
                    priority=self.PRIO_VOLUME,
                    meta={
                        "collection_name": collection_name,
                        "page": page,
                        "book_id": book_id,
                        "volume_id": volume_id,
                        "volume_name": volume_name,
                        "index_in_book": vidx,
                    },
                    callback=self.parse_volume_reader,
                )
            volumes.append((volume_id, volume_name))

        yield BookItem(
            id=book_id,
            name=title,
            author=author,
            cover_image_url=cover_image_url,
            of_category_id=self.category,
            of_category_name=category_name,
            of_collection_name=collection_name,
            introduction=introduction,
            keywords=keywords,
            misc_metadata=misc_metadata,
            volumes=volumes,
        )

    def parse_volume_reader(self, response):
        volume_id = response.meta["volume_id"]
        try:
            file_path = self.REGEX_PDFNAME_IN_READER.search(response.text)[1]
        except TypeError:
            file_path = None
            self.log(f"file_path not found for {volume_id}", level=logging.WARNING)
        response.meta.update({"volume_file_path": file_path})
        yield response.follow(
            self.URL_VOLUME_TOC,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body=urlencode(
                {
                    "id": volume_id,
                    "indexName": response.meta["collection_name"],
                }
            ),
            meta=response.meta,
            priority=self.PRIO_VOLUME,
            callback=self.parse_volume_toc,
        )

    def parse_volume_toc(self, response):
        collection_name = response.meta["collection_name"]
        book_id = response.meta["book_id"]
        index_in_book = response.meta["index_in_book"]
        volume_id = response.meta["volume_id"]
        volume_name = response.meta["volume_name"]
        volume_file_path = response.meta["volume_file_path"]

        d = response.json()
        assert d["success"]
        toc = []
        # self.log(response.meta)
        # self.log(d)
        for chapter in chain(
            *map(
                lambda row: (
                    (row.get("chapter_num1"), row.get("chapter_name1")),
                    (row.get("chapter_num2"), row.get("chapter_name2")),
                ),
                d["obj"] or [],
            )
        ):
            if chapter[0] or chapter[1]:
                toc.append(chapter)

        yield VolumeItem(
            id=volume_id,
            name=volume_name,
            file_path=volume_file_path,
            toc=toc,
            index_in_book=index_in_book,
            of_book_id=book_id,
            of_collection_name=collection_name,
        )
