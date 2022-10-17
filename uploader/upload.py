#!/usr/bin/env python3
import os.path
import itertools
import subprocess
import json
import logging
from io import BytesIO
import os
import functools
import re
import sys
from itertools import chain

import requests
import yaml
import mwclient

from getbook import getbook

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "config.yml")
POSITION_FILE_PATH = os.path.join(os.path.dirname(__file__), ".position")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
RETRY_TIMES = 3

USER_AGENT = "nlcpdbot/0.0 (+https://github.com/gowee/nlcpd)"

# RESP_DUMP_PATH = "/tmp/wmc_upload_resp_dump.html"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def call(command, *args, **kwargs):
    kwargs["shell"] = True
    return subprocess.check_call(command, *args, **kwargs)


def load_position(name):
    logger.info(f'Loading position from {POSITION_FILE_PATH + "." + name}')
    if os.path.exists(POSITION_FILE_PATH + "." + name):
        with open(POSITION_FILE_PATH + "." + name, "r") as f:
            return f.read().strip()
    else:
        return None


def store_position(name, position):
    with open(POSITION_FILE_PATH + "." + name, "w") as f:
        f.write(position)


def retry(times=RETRY_TIMES):
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
                    if tried == times:
                        raise Exception(f"Failed finally after {times} tries") from e
                    logger.debug(f"Retrying {fn}")

        return wrapped

    return wrapper


def gen_toc(toc):
    lines = []
    for no, entry in toc:
        # assert not no
        # no is often emtpy, but there are also rare cases such as 外科精要(data_011,411999024120)
        lines.append(" ".join(filter(None, [no, entry])))
    contents = " <br/>\n".join(lines)
    if contents:
        contents = "<p>\n" + contents + "\n</p>"
        contents += "\n<!--The toc provided by NLC may have some volumes missed. To be corrected.-->"
    return contents


@retry()
def getbook_unified(volume, proxies=None):
    logger.debug(f"Fetching {volume}")
    # if "fileiplogger.info("Failed to get file by path: " + str(e), ", fallbacking to getbook")
    return getbook(
        volume["of_collection_name"].removeprefix("data_"), volume["id"], proxies
    )


def split_name_heuristic(name):
    if name.endswith("不分卷"):
        return name[:-3], "不分卷"
    match = re.match(r"^(\S+?)\s*([一二三四五六七八九十]+[卷冊册])$", name)
    if match is None:
        return name, ""
    else:
        return match.group(1), match.group(2)


def split_name_simple(name):
    parts = tuple(name.split(maxsplit=1))
    if len(parts) == 1:
        parts += ("",)
    return parts


def fix_bookname_in_pagename(
    bookname, apply_tortoise_shell_brackets_to_starting_of_title=False
):
    # if bookname.startswith("[") and bookname.endswith("]"):  # [四家四六]
    #     bookname = bookname[1:-1]

    if apply_tortoise_shell_brackets_to_starting_of_title and bookname.startswith("["):
        bookname = re.sub(r"\[(.+?)\]", r"〔\1〕", bookname)  # [宋]...
    bookname = re.sub(r"\[(.+?)\]", r"\1", bookname)
    bookname = bookname.replace(":", "：")  # e.g. 404 00J001624 綠洲:中英文藝綜合月刊
    bookname = bookname.replace("###", " ").replace("@@@", " ")
    bookname = re.sub(r"\s+", " ", bookname)
    bookname = bookname.replace(
        "?", "□"
    )  # WHITE SQUARE, U+25A1, for, e.g. 892 312001039388 筠清?金石文字   五卷"
    return bookname


def main():
    with open(CONFIG_FILE_PATH, "r") as f:
        config = yaml.safe_load(f.read())

    if len(sys.argv) < 2:
        exit(
            f"Not batch specified.\n\nAvailable: {', '.join(list(config['batchs'].keys()))}"
        )
    batch_name = sys.argv[1]

    username, password = config["username"], config["password"]
    site = mwclient.Site("commons.wikimedia.org")
    site.login(username, password)
    site.requests["timeout"] = 125
    site.chunk_size = 1024 * 1024 * 64

    logger.info(f"Signed in as {username}")

    overwriting_categories = {
        (str(item["dbid"]), str(item["bookid"])): item["catname"]
        for item in chain(
            config.get("overwriting_categories", []),
            config["batchs"][batch_name].get("overwriting_categories", []),
        )
    }

    def getopt(item, default=None):  # get batch config or fallback to global config
        return config["batchs"][batch_name].get(item, config.get(item, default))

    nlc_proxies = getopt("nlc_proxies", None)

    with open(os.path.join(DATA_DIR, batch_name + ".json")) as f:
        books = json.load(f)
    template = getopt("template")
    batch_link = getopt("link") or getopt("name")
    global fix_bookname_in_pagename
    fix_bookname_in_pagename = functools.partial(
        fix_bookname_in_pagename,
        apply_tortoise_shell_brackets_to_starting_of_title=getopt(
            "apply_tortoise_shell_brackets_to_starting_of_title", False
        ),
    )
    if getopt("split_name", "").lower() == "simple":
        split_name = split_name_simple
    elif getopt("split_name", "").lower() == "heuristic":
        split_name = split_name_heuristic
    else:
        split_name = lambda s: (s, "")

    last_position = load_position(batch_name)

    if last_position is not None:
        books = iter(books)
        logger.info(f"Last processed: {last_position}")
        next(
            itertools.dropwhile(lambda book: str(book["id"]) != last_position, books)
        )  # lazy!
        # TODO: peek and report?

    failcnt = 0

    for book in books:
        assert "\uf8ff" not in book["author"]
        byline = book["author"]
        byline_enclosing_brackets = False
        if book["author"].startswith("[") and book["author"].endswith("]"):
            byline = byline[1:-1]
            byline_enclosing_brackets = True
            # assert "[" not in byline # disabled due to: 411999012181 [(明)周士佐[等]修]
        if getopt("apply_tortoise_shell_brackets_to_starting_of_byline", False):
            # e.g. "(魏)王弼,(晋)韩康伯撰   (唐)邢璹撰"
            atsb = lambda s: re.sub(
                r"^([（(〔[][题題][]）)〕])?[（(〔[](.{0,3}?)[]）)〕]",
                r"\1〔\2〕",
                s,
            )
        else:
            atsb = lambda s: s
        # e.g. "（英國）韋廉臣（Williams,W.）撰"
        byline = re.sub(r"(（[^）]+?)(,)([^）]+?）)", "\\1\uf8ff\\3", byline)
        byline = " <br />\n".join(
            " ".join(atsb(aauthor) for aauthor in re.split(r"[，,、]", author))
            for author in re.split(
                r"@@@|###@@@|   ",
                byline,
            )
        )
        byline = byline.replace("\uf8ff", ",")
        if byline_enclosing_brackets:
            byline = "[" + byline + "]"
        title, note_in_title = split_name(book["name"].replace("?", "□"))
        title = re.sub(r"\s+", " ", title)
        note_in_title = re.sub(r"\s+", " ", note_in_title)
        if getopt("split_name", None) is not None:
            nit_field = f"  |note_in_title={note_in_title}\n"
        else:
            nit_field = ""
        if "@@@" in title:
            # single \n does not render as expected
            title = title.replace("###@@@", "@@@").replace("@@@", "\n\n")
            # Now line feed is used in place of space, so we do not need this
            # if getopt("apply_gbt3792_7_brackets_to_title", False):
            #     title = "[" + title + "]"
            # http://www.nlc.cn/pcab/gjbhzs/bm/201412/P020150309516939790893.pdf §8.1.4, §8.1.6
        volumes = book["volumes"]
        volumes.sort(key=lambda e: e["index_in_book"])
        metadata = book["misc_metadata"]
        dbid = book["of_collection_name"].removeprefix("data_")
        additional_fields = "\n".join(f"  |{k}={v}" for k, v in metadata.items())
        if (k := (str(dbid), str(book["id"]))) in overwriting_categories:
            category_name = "Category:" + overwriting_categories[k]
        else:
            category_name = "Category:" + fix_bookname_in_pagename(title)
        category_page = site.pages[category_name]
        # TODO: for now we do not create a seperated category suffixed with the edition
        if not category_page.exists:
            category_wikitext = (
                """{{Wikidata Infobox}}
{{Category for book|zh}}
{{zh|%s}}

[[Category:Chinese-language books by title]]
"""
                % title
            )
            category_page.edit(
                category_wikitext,
                f"Creating (batch task; nlc:{book['of_collection_name']},{book['id']})",
            )
        for ivol, volume in enumerate(volumes):
            abstract = book["introduction"].replace("###", "@@@").replace("@@@", "\n")
            toc = gen_toc(volume["toc"])
            volume_name = (
                (
                    volume["name"]
                    .replace("_", "–")
                    .replace("-", "–")
                    .replace("/", "–")
                    .replace("\n", " ")
                    or f"第{ivol+1}冊"
                )
                if (
                    len(volumes) > 1
                    or getopt("always_include_volume_name_in_filename", False)
                )
                else ""
            )
            volume_name_wps = (
                (" " + volume_name) if volume_name else ""
            )  # with preceding space
            volume_wikitext = f"""=={{{{int:filedesc}}}}==
{{{{{template}
  |byline={byline}
  |title={title}
{nit_field}  |volume={volume_name}
  |abstract={abstract}
  |toc={toc}
  |catid={book['of_category_id']}
  |db={volume["of_collection_name"]}
  |dbid={dbid}
  |bookid={book["id"]}
  |volumeid={volume["id"]}
{additional_fields}
}}}}
{"{{Watermark}}" if getopt("watermark_tag", False) else ""}

[[{category_name}]]
"""
            comment = f"Upload {book['name']}{volume_name_wps} ({1+ivol}/{len(volumes)}) by {book['author']} (batch task; nlc:{book['of_collection_name']},{book['id']},{volume['id']}; {batch_link}; [[{category_name}|{title}]])"
            filename = f'NLC{dbid}-{book["id"]}-{volume["id"]} {fix_bookname_in_pagename(book["name"])}{volume_name_wps}.pdf'
            assert all(char not in set(r'["$*|\]</^>@#') for char in filename)
            pagename = "File:" + filename
            page = site.pages[pagename]
            try:
                if not page.exists:
                    logger.info(f'Downloading {dbid},{book["id"]},{volume["id"]}')
                    binary = getbook_unified(volume, nlc_proxies)
                    logger.info(f"Uploading {pagename} ({len(binary)} B)")

                    @retry()
                    def do1():
                        r = site.upload(
                            BytesIO(binary),
                            filename=filename,
                            description=volume_wikitext,
                            comment=comment,
                        )
                        assert (r or {}).get("result", {}) == "Success" or (
                            r or {}
                        ).get("warnings", {}).get("exists"), f"Upload failed {r}"

                    do1()
                else:
                    if getopt("skip_on_existing", False):
                        logger.debug(f"{pagename} exists, skipping")
                    else:
                        logger.info(f"{pagename} exists, updating wikitext")

                        @retry()
                        def do2():
                            r = page.edit(
                                volume_wikitext, comment + " (Updating metadata)"
                            )
                            assert (r or {}).get(
                                "result", {}
                            ) == "Success", f"Update failed {r}"

                        do2()
            except Exception as e:
                failcnt += 1
                logger.warning("Upload failed", exc_info=e)
                if not getopt("skip_on_failures", False):
                    raise e
        store_position(batch_name, book["id"])
    logger.info(f"Batch done with {failcnt} failures.")


if __name__ == "__main__":
    main()
