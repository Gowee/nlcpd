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

import requests
import yaml
import mwclient

from getbook import getbook

CONFIG_FILE_PATH = "./config.yml"
POSITION_FILE_PATH = "./.position"
DATA_DIR = os.path.join(os.getcwd(), "data")

USER_AGENT = "nlcpdbot/0.0 (+https://github.com/gowee/nlcpd)"

# RESP_DUMP_PATH = "/tmp/wmc_upload_resp_dump.html"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def call(command, *args, **kwargs):
    kwargs["shell"] = True
    return subprocess.check_call(command, *args, **kwargs)


def load_position():
    if os.path.exists(POSITION_FILE_PATH):
        with open(POSITION_FILE_PATH, "r") as f:
            return f.read().strip()
    else:
        return None


def store_position(position):
    with open(POSITION_FILE_PATH, "w") as f:
        f.write(position)


def retry(times=3):
    def wrapper(fn):
        tried = 0

        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                nonlocal tried
                tried += 1
                if tried == times:
                    raise Exception(f"Failed finally after {times} tries") from e

        return wrapped

    return wrapper


def gen_toc(toc):
    lines = []
    for no, entry in toc:
        assert not no
        lines.append(entry)
    contents = " <br/>\n".join(lines)
    if contents:
        contents = "<p>\n" + contents + "\n</p>"
        contents += "\n<!--The toc provided by NLC may have some volumes missed. To be corrected.-->"
    return contents


def getbook_unified(volume):
    # if "fileiplogger.info("Failed to get file by path: " + str(e), ", fallbacking to getbook")
    return BytesIO(
        getbook(volume["of_collection_name"].removeprefix("data_"), volume["id"])
    )


def fix_bookname_in_pagename(bookname):
    if bookname.startswith("[") and bookname.endswith("]"):  # [四家四六]
        bookname = bookname[1:-1]
    if bookname.startswith("["):
        bookname = re.sub(r"\[(.+?)\]", r"〔\1〕", bookname)  # [宋]...
    return bookname


def main():
    import sys, os

    sys.path.append(DATA_DIR)

    with open(CONFIG_FILE_PATH, "r") as f:
        config = yaml.safe_load(f.read())
    username, password = config["username"], config["password"]
    site = mwclient.Site("commons.wikimedia.org")
    site.login(username, password)
    site.requests["timeout"] = 125
    site.chunk_size = 1024 * 1024 * 64

    with open(os.path.join(DATA_DIR, config["batch"] + ".json")) as f:
        batch = json.load(f)
    template = config["template"]
    batch_link = config["batch_link"] or config["batch"]

    last_position = load_position()

    if last_position is not None:
        batch = iter(batch)
        print(f"Last processed: {last_position}")
        next(
            itertools.dropwhile(lambda book: str(book["id"]) != last_position, batch)
        )  # lazy!
        # TODO: peek and report?

    for book in batch:
        authors = book["author"].split("@@@")
        if config.get('apply_tortoise_shell_brackets_to_starting_of_byline', False):
            authors = [re.sub(r"^[（(〔](.{0,3}?)[）)〕]", r"〔\1〕", author) for author in authors]
        byline = "\n".join(authors)
        title = book["name"].replace("@@@", " ")
        volumes = book["volumes"]
        volumes.sort(key=lambda e: e["index_in_book"])
        metadata = book["misc_metadata"]
        dbid = book["of_collection_name"].removeprefix("data_")
        additional_fields = "\n".join(f"  |{k}={v}" for k, v in metadata.items())
        category_name = "Category:" + fix_bookname_in_pagename(title)
        category_page = site.pages[category_name]
        # TODO: for now we do not create a seperated category suffixed with the edition
        if not category_page.exists:
            category_wikitext = """{{Wikidata Infobox}}
{{Category for book|zh}}
{{zh|%s}}

[[Category:Chinese-language books by title]]
""" % title
            category_page.edit(
                category_wikitext,
                f"Creating (batch task; nlc:{book['of_collection_name']},{book['id']})",
            )
        for ivol, volume in enumerate(volumes):
            description = "\n".join(
                filter(lambda v: v, [book["introduction"], gen_toc(volume["toc"])])
            )
            volume_name = (volume["name"] or f"第{ivol+1}冊") if len(volumes) > 1 else ""
            volume_name_wps = (
                (" " + volume_name) if volume_name else ""
            )  # with preceding space
            volume_wikitext = f"""=={{{{int:filedesc}}}}==
{{{{{template}
  |byline={byline}
  |title={title}
  |volume={volume_name}
  |description={description}
  |catid={book['of_category_id']}
  |db={volume["of_collection_name"]}
  |dbid={dbid}
  |bookid={book["id"]}
  |volumeid={volume["id"]}
{additional_fields}
}}}}

[[{category_name}]]
"""
            comment = f"Upload {book['name']}{volume_name_wps} ({1+ivol}/{len(volumes)}) by {byline} (batch task; nlc:{book['of_collection_name']},{book['id']},{volume['id']}; {batch_link}; [[Category:{title}|{title}]])"
            filename = f'NLC{dbid}-{book["id"]}-{volume["id"]} {fix_bookname_in_pagename(book["name"])}{volume_name_wps}.pdf'
            pagename = "File:" + filename
            page = site.pages[pagename]
            if not page.exists:
                logger.info(f"Uploading {pagename}")

                @retry(3)
                def do1():
                    return site.upload(
                        getbook_unified(volume),
                        filename=filename,
                        description=volume_wikitext,
                        comment=comment,
                    )

                do1()
            else:
                logger.info(f"{pagename} exists, updating wikitext")

                @retry(3)
                def do2():
                    page.edit(volume_wikitext, comment + " (Updating metadata)")

                do2()
        store_position(book["id"])


if __name__ == "__main__":
    main()
