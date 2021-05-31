# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from dataclasses import dataclass


@dataclass
class CategoryItem:
    id: int
    name: str
    description: str
    icon_url: str
    parental_category_name: str


@dataclass
class BookItem:
    """A book consisting of one or more volumes"""

    id: str
    name: str
    author: str
    cover_image_url: str
    collection_name: str  # aid
    introduction: str
    # edition: str # 版本项
    # nlc_no: str # 馆藏书号
    # publish_date: str # 出版发行项
    # edition_note_historical: str # 版本书目史注 chirography
    # edition_note_current: str # 现有藏本附注 chirography_300A
    # rb_no: str # 善本书号
    misc_metadata: dict[str, str]  # other miscellaneous metadata entries
    volumes: list['Volume']


@dataclass
class VolumeItem:
    """ "A volume of a book, corresponding to a file"""

    id: str  # bid
    name: str
    file_path: str  # internal file path of NLC
    toc: list[(str, str)]  # list of chapter number and capther name pairs
