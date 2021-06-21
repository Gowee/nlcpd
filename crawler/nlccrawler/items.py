# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from dataclasses import dataclass
from typing import Union
import functools


def mongo_item(collection_name=None, to__id=None, upsert_index=None):
    def wrap(cls):
        # TODO: validate fields specified in upsert_index
        # for field in upsert_index:
        #     if not hasattr(cls, field):
        #         raise KeyError(f"The field {field} specified in upsert_index does not exist for {cls}")
        @functools.wraps(cls, updated=())
        class Wrapper(cls):
            _collection_name = collection_name or cls.__name__
            _to__id = to__id
            _upsert_index = upsert_index

        return Wrapper

    return wrap


@mongo_item(collection_name="categories", to__id="id", upsert_index=("id",))
@dataclass
class CategoryItem:
    id: int
    name: str
    description: str
    icon_url: str
    parental_category_name: str


@mongo_item(collection_name="books", upsert_index=("id", "of_collection_name"))
@dataclass
class BookItem:
    """A book consisting of one or more volumes"""

    id: str
    name: str
    author: str
    cover_image_url: str
    introduction: str
    # edition: str # 版本项
    # nlc_no: str # 馆藏书号
    # publish_date: str # 出版发行项
    # edition_note_historical: str # 版本书目史注 chirography
    # edition_note_current: str # 现有藏本附注 chirography_300A
    # rb_no: str # 善本书号
    keywords: list[str]
    misc_metadata: dict[str, str]  # other miscellaneous metadata entries
    volumes: Union[list[(str, str)], list[("Volume")]]
    of_collection_name: str  # aid


@mongo_item(collection_name="volumes", upsert_index=("id", "of_collection_name"))
@dataclass
class VolumeItem:
    """ "A volume of a book, corresponding to a file"""

    id: str  # bid
    name: str
    file_path: str  # internal file path of NLC
    toc: list[(str, str)]  # list of chapter number and capther name pairs
    index_in_book: int
    of_book_id: str
    of_collection_name: str
