import requests
import re

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


def getbook(aid: str, bid: str, proxies=None):
    resp = requests.get(
        URL_READER.format(aid=aid, bid=bid),
        headers={"User_Agent": USER_AGENT},
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
        headers={"User_Agent": USER_AGENT, "myreader": token_key},
        proxies=proxies,
    )
    resp.raise_for_status()
    return resp.content
