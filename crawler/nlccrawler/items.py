# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from dataclasses import dataclass
from typing import Union, Optional, Tuple
import functools


def mongo_item(collection_name=None, to__id=None, upsert_index=None):
    """
    A decorator for Item classes that instructs the TxMongoPipeline to store
    items properly

    Args:
        collection_name : Required. O.W., `TxMongoPipeline` just ignores the
            item.
        to__id : a field to be renamed to `_id`, which is optional.
        upsert_index : a tuple of fields be used as the criteria when upserting.
            Without this, upserting is disabled.
    """

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


@mongo_item(collection_name="categories", to__id="id", upsert_index=("_id",))
@dataclass
class CategoryItem:
    id: int
    name: str
    collection_name: str
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
    keywords: Optional[list[str]]
    misc_metadata: dict[str, str]  # other miscellaneous metadata entries
    volumes: Union[list[Tuple[str, str]], list[("VolumeItem")]]
    of_category_id: Optional[str]  # searchType
    of_category_name: Optional[str]
    of_collection_name: str  # aid


@mongo_item(collection_name="volumes", upsert_index=("id", "of_collection_name"))
@dataclass
class VolumeItem:
    """ "A volume of a book, corresponding to a file"""

    id: str  # bid
    name: str
    file_path: Optional[str]  # internal file path of NLC
    toc: list[Tuple[str, str]]  # list of chapter number and capther name pairs
    index_in_book: int
    of_book_id: str
    of_collection_name: str
