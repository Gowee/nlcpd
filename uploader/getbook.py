import re
import logging
import functools
from pathlib import Path
from io import BytesIO
from urllib.parse import quote as urlquote
import textwrap
from datetime import timezone
import datetime


import img2pdf
from PIL import Image, ImageFont, ImageDraw
import requests
from qrcode import QRCode

URL_READER = "http://read.nlc.cn/OutOpenBook/OpenObjectBook?aid={aid}&bid={bid}"
URL_FILE = "http://read.nlc.cn/menhu/OutOpenBook/getReader?aid={aid}&bid={bid}&kime={kime}&fime={fime}"

REGEX_BOOK_ID = re.compile(r"var id = parseInt\(\'([\d.]+)\'\)")
REGEX_COLLECTION_NAME = re.compile(r"var indexName\s*=\s*\'(\w+)\'")
REGEX_BOOK_TITLE = re.compile(r"var title\s*=\s*\'(.+?)\'")
REGEX_FILE_ID = re.compile(r"var identifier\s*=\s*\'(.+?)\'")
REGEX_FILE_PATH = re.compile(r"var pdfname\s*=\s*\'(.+?)\'")
REGEX_PRESS = re.compile(r"var pressName\s*=\s*\'(.*?)\'")
REGEX_TOKEN_KEY = re.compile(r"tokenKey=\"(\w+)\"")
REGEX_TIME_KEY = re.compile(r"timeKey=\"(\w+)\"")
REGEX_TIME_FLAG = re.compile(r"timeFlag=\"(\w+)\"")

USER_AGENT = "nlcpdbot/0.0 (+https://github.com/gowee/nlcpd)"

FONT_FILE_PATH = Path(__file__).parent / "Aileron-Regular.otf"


logger = logging.getLogger(__name__)


def retry(times=3):
    def wrapper(fn):
        tried = 0

        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            while True:
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    nonlocal tried
                    tried += 1
                    if tried >= times:
                        raise Exception(f"Failed finally after {times} tries") from e
                    logger.info(f"Retrying {fn} {tried}/{times} due to {e}", exc_info=e)

        return wrapped

    return wrapper


@retry(7)
def fetch_file(url, session=None):
    resp = (session and session.get or requests.get)(
        url, headers={"User-Agent": USER_AGENT}
    )
    resp.raise_for_status()
    assert len(resp.content) != 0, "Got empty file"
    if "Content-Length" in resp.headers:
        # https://blog.petrzemek.net/2018/04/22/on-incomplete-http-reads-and-the-requests-library-in-python/
        expected_size = int(resp.headers["Content-Length"])
        actual_size = resp.raw.tell()
        assert (
            expected_size == actual_size
        ), f"Incomplete download: {actual_size}/{expected_size}"
    return resp.content


def construct_failure_page(url, page_name=""):
    qr = QRCode(box_size=3)
    qr.add_data(url)
    qr.make()
    qr_img = qr.make_image()

    # ref: https://stackoverflow.com/a/1970930/5488616
    #      https://stackoverflow.com/a/68648910/5488616
    #      ChatGPT

    font = ImageFont.truetype(str(FONT_FILE_PATH), size=20)

    t = datetime.datetime.now(timezone.utc)
    assert t.tzinfo == timezone.utc
    if page_name:
        page_name = " " + page_name
    texts = [
        f"The page{page_name} links to an broken url:",
        *textwrap.wrap(urlquote(url, safe=":/"), break_on_hyphens=False),
        "Access time: " + str(t),
    ]

    margin = (5, 5)
    spacing = 3
    mask_images = [font.getmask(text, "L") for text in texts]
    width = (
        max(max(mask_image.size[0] for mask_image in mask_images), qr_img.size[0])
        + margin[0] * 2
    )
    height = (
        sum(mask_image.size[1] for mask_image in mask_images)
        + margin[1] * 2
        + (len(mask_images) - 1) * spacing
        + spacing
        + qr_img.size[1]
    )
    # mask_image = font.getmask(text, "L")
    img = Image.new("RGB", (width, height), (255, 255, 255))
    y = margin[1]
    for mask_image in mask_images:
        # need to use the inner `img.im.paste` due to `getmask` returning a core
        img.im.paste(
            (0, 0, 0),
            (margin[0], y, margin[0] + mask_image.size[0], y + mask_image.size[1]),
            mask_image,
        )
        y += mask_image.size[1] + spacing
    img.paste(qr_img, (0, y))

    output = BytesIO()
    img.save(output, format="JPEG")
    return output.getvalue()


@retry(3)
def fetch_image_list(image_urls, file=None):
    session = requests.Session()  # <del>activate connection reuse</del>
    images = []
    failures = 0
    for i, url in enumerate(image_urls):
        logger.debug(f"Downloading {url}")
        assert url.endswith(".jpg"), "Expected JPG: " + url
        try:
            image = fetch_file(url, session)
        except Exception as e:
            logger.warning(
                f"Failed to download {url}, using placeholder image", exc_info=e
            )
            image = construct_failure_page(url, page_name=f"({i+1}/{len(image_urls)})")
            failures += 1
        images.append(image)
    assert failures < len(image_urls), "Failed to download all images"
    blob = img2pdf.convert(images)
    # with cached_path.open("wb") as f:
    #     f.write(blob)
    logger.info(
        f"PDF constructed ({len(image_urls)} images, {failures} failures, {sum(map(len, images))} => {len(blob)} B)"
    )
    # with save_path.open("wb") as f:
    if file:
        file.write(blob)
    else:
        return blob


def getbook(aid: str, bid: str, file_path=None, proxies=None):
    if file_path and not isinstance(file_path, str):
        return fetch_image_list(file_path)
    else:
        resp = requests.get(
            URL_READER.format(aid=aid, bid=bid),
            headers={"User-Agent": USER_AGENT},
            proxies=proxies,
        )
        resp.raise_for_status()
        html = resp.text
        # print(html)
        # print(URL_READER.format(aid=aid, bid=bid))
        (
            book_id,
            collection_id,  # aid prefixed with "data_"
            book_title,
            file_id,
            file_path,
            press,
            token_key,  # part of key
            time_key,  # part of key
            time_flag,  # part of key
        ) = map(
            lambda p: False or p.search(html).group(1),
            [
                REGEX_BOOK_ID,
                REGEX_COLLECTION_NAME,
                REGEX_BOOK_TITLE,
                REGEX_FILE_ID,
                REGEX_FILE_PATH,
                REGEX_PRESS,
                REGEX_TOKEN_KEY,
                REGEX_TIME_KEY,
                REGEX_TIME_FLAG,
            ],
        )

        # Volume(
        #     id=file_id,
        #     file_path=file_path,
        #     book_id=book_id,
        #     book_title=book_title,
        #     press_name=press,
        #     collection_id=collection_id,
        # ),
        # (token_key, time_key, time_flag),
        # print(time_key, time_flag, token_key)
        resp = requests.post(
            URL_FILE.format(aid=aid, bid=bid, kime=time_key, fime=time_flag),
            headers={"User-Agent": USER_AGENT, "myreader": token_key},
            proxies=proxies,
        )
        resp.raise_for_status()
        assert resp.headers.get("Content-Type").endswith("/pdf") or resp.headers.get(
            "Content-Type"
        ).endswith("/octet-stream")
        assert len(resp.content) != 0, "Got empty file"
        if "Content-Length" in resp.headers:
            # https://blog.petrzemek.net/2018/04/22/on-incomplete-http-reads-and-the-requests-library-in-python/
            expected_size = int(resp.headers["Content-Length"])
            actual_size = resp.raw.tell()
            assert (
                expected_size == actual_size
            ), f"Incomplete download: {actual_size}/{expected_size}"
        return resp.content
