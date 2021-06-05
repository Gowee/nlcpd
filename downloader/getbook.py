from typing import Tuple

from aiohttp import web, ClientSession
from contextlib import asynccontextmanager
from dataclasses import dataclass
import re

from .config import USERNAME, PASSWORD, solve_captcha


@dataclass
class Volume:
    id: str
    file_path: str  # internal file path on server
    book_id: str  # bid
    book_title: str  # book title
    press_name: str  # name of the press, possibly empty
    collection_id: str  # aid


class Downloader:
    URL_PORTAL = "http://read.nlc.cn/user/index"
    URL_CAPTCHA_CODE = (
        "https://sso1.nlc.cn/sso/currency/getvalidateImgCode/login?height=100&width=200"
    )
    URL_LOGIN = "https://sso1.nlc.cn/sso/login/reader_user_comein"
    URL_READER = "http://read.nlc.cn/OutOpenBook/OpenObjectBook?aid={aid}&bid={bid}"
    URL_FILE = "http://read.nlc.cn/menhu/OutOpenBook/getReader?aid={aid}&bid={bid}&kime={kime}&fime={fime}"

    session = ClientSession()
    ready = False

    def __init__(self, username, password, solve_captcha):
        self.username = username
        self.password = password
        self.solve_captcha = solve_captcha

    async def ensure_ready(self):
        if not self.ready:
            self.login(self.solve_captcha(self.get_captcha()))
            self.ready = True

    async def get_captcha(self) -> bytes:
        async with self.session.get(self.URL_CAPTCHA_CODE) as resp:
            return await resp.bytes()

    async def login(self, captcha_code: str):
        payload = {
            "username": self.username,
            "password": self.password,
            "imgCode": captcha_code,
            "loginmode": "",
            "action": "",
            "logintype": "0",
            "paramUrl": URL_PORTAL,
            "redirectUrl": "https://sso1.nlc.cn/sso/jsp/mylib-login.jsp",
            "appId": "90037",
            "redflag": "",
        }
        async with self.session.post(self.URL_LOGIN, data=payload) as resp:
            if resp.headers.get("Location") != self.PORTAL_URL:
                raise Exception("Failed to login")

    REGEX_BOOK_ID = re.compile(r"var id = parseInt\(\'([\d.]+)\'\)")
    REGEX_COLLECTION_NAME = re.compile(r"var indexName = \'(\w+)\'")
    REGEX_BOOK_TITLE = re.compile(r"var title = \'(.+?)\'")
    REGEX_FILE_ID = re.compile(r"var identifier = \'(.+?)\'")
    REGEX_FILE_PATH = re.compile(r"var pdfname = \'(.+?)\'")
    REGEX_PRESS = re.compile(r"var pressName = \'(.*?)\'")
    REGEX_TOKEN_KEY = re.compile(r"tokenKey=\"(\w+)\"")
    REGEX_TIME_KEY = re.compile(r"timeKey=\"(\w+)\"")
    REGEX_TIME_FLAG = re.compile(r"timeFLAG=\"(\w+)\"")

    # @asynccontextmanager
    async def get_book_metadata(self, aid: str, bid: str) -> (Volume, (str, str, str)):
        async with self.session.get(URL_READER.format(aid=aid, bid=bid)) as resp:
            html = await resp.text()

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
            lambda p: p.search(html).group(1),
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

        return (
            Volume(
                id=file_id,
                file_path=file_path,
                book_id=book_id,
                book_title=book_title,
                press_name=press,
                collection_id=collection_id,
            ),
            (token_key, time_key, time_flag),
        )

    @asynccontextmanager
    async def download(self, aid: str, bid: str, key_triple: Tuple[str, str, str]):
        req_ctx = self.session.post(
            URL_FILE.format(aid=aid, bid=bid, kime=key_triple[1], fime=key_triple[2]),
            headers={"myreader": key_triple[0]},
        )
        try:
            req = await req_ctx.__aenter__()
            yield req.content
        finally:
            await req_ctx.__aexit__()


downloader = Downloader(USERNAME, PASSWORD, solve_captcha)


async def handle_download(aid, bid):
    await downloader.ensure_ready()
    metadata, key_triple = await downloader.get_book_metadata()


app = web.Application()
app.add_routes(
    [
        web.get("/", lambda: "Up and running..."),
        web.get("/volume/{aid}/{bid}", handle_download),
    ]
)

if __name__ == "__main__":
    import sys

    host, port = None, None
    if len(sys.argv) == 2:
        port = int(sys.argv[1])
    elif len(sys.argv) == 3:
        host = sys.argv[1]
        port = int(sys.argv[2])
    web.run_app(app, host=host, port=port)
