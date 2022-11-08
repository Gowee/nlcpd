#!/usr/bin/env python3
import yaml
import os
import re
import json
import sys
import functools

import mwclient
from more_itertools import peekable

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "config.yml")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def fix_bookname_in_pagename(
    bookname, apply_tortoise_shell_brackets_to_starting_of_title=False
):
    # if bookname.startswith("[") and bookname.endswith("]"):  # [四家四六]
    #     bookname = bookname[1:-1]

    if apply_tortoise_shell_brackets_to_starting_of_title and bookname.startswith("["):
        bookname = re.sub(r"\[(.+?)\]", r"〔\1〕", bookname)  # [宋]...
    bookname = re.sub(r"\[(.+?)\]", r"\1", bookname)
    bookname = bookname.replace(":", "：")  # e.g. 404 00J001624 綠洲:中英文藝綜合月刊
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

    def getopt(item, default=None):  # get batch config or fallback to global config
        return config["batchs"][batch_name].get(item, config.get(item, default))

    with open(os.path.join(DATA_DIR, batch_name + ".json")) as f:
        batch = json.load(f)
    template = "Template:" + getopt("template")
    batch_link = getopt("link") or getopt("name")
    category_name = re.search(r"(Category:.+?)[]|]", batch_link).group(1)
    global fix_bookname_in_pagename
    fix_bookname_in_pagename = functools.partial(
        fix_bookname_in_pagename,
        apply_tortoise_shell_brackets_to_starting_of_title=getopt(
            "apply_tortoise_shell_brackets_to_starting_of_title", False
        ),
    )

    booknavi = getopt("booknavi")

    for book in batch:
        book["name"] = book["name"].replace("@@@", " ")
        book["author"] = book["author"].replace("@@@", " ")
        dbid = book["of_collection_name"].removeprefix("data_")
        # lines.append(
        #     f'* 《{book["name"]}》 {book["author"]} {{{{NLC-Book-Link|{dbid}|{book["id"]}|catid={book["of_category_id"]}}}}}'
        # )
        volumes = book["volumes"]
        volumes.sort(key=lambda e: e["index_in_book"])
        def genvols():
            for ivol, volume in enumerate(volumes):
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
                filename = f'NLC{dbid}-{book["id"]}-{volume["id"]} {fix_bookname_in_pagename(book["name"])}{volume_name_wps}.pdf'
                pagename = "File:" + filename
                yield volume, filename, pagename
        prev_filename = None
        volsit = peekable(genvols())
        for volume, filename, pagename in volsit:
            print(f"Checking {pagename}")
            try:
                next_filename = volsit.peek()[1]
            except StopIteration:
                next_filename = None
            page = site.pages[pagename]
            wikitext = page.text()
            if "{{booknavi|" not in wikitext:
                navitext = f'''{{{{{booknavi}|prev={prev_filename or ""}|next={next_filename or ""}|nth={volume['index_in_book'] + 1}|total={len(volumes)}|catid={book['of_category_id']}|db={volume["of_collection_name"]}|dbid={dbid}|bookid={book["id"]}|volumeid={volume["id"]}}}}}'''
                _wikitext = wikitext
                needle = r"{{" + template
                wikitext = wikitext.replace(needle, navitext + "\n" + needle)
                # wikitext = re.sub(r"{{" + template, navitext + r"\n\0" , wikitext)
                if wikitext == _wikitext:
                    print(wikitext)
                    raise Exception(f"failed to add navibar to {pagename}")
                page.edit(wikitext, f"Add {booknavi}")
                print(f"Updated {pagename}")
            else:
                print(f"Skipped {pagename}")
            prev_filename = filename
    print("All done")

    # lines.append("")
    # lines.append("[[" + category_name + "]]")
    # lines.append("")


        # site.pages[pagename].edit(
        #     "\n".join(lines), f"Writing file list for batch {batch_name} to {pagename}"
        # )


if __name__ == "__main__":
    main()
