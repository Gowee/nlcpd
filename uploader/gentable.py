#!/usr/bin/env python3
import yaml
import os
import re
import json
import sys

import mwclient

CONFIG_FILE_PATH = "./config.yml"
DATA_DIR = os.path.join(os.getcwd(), "data")


def fix_bookname_in_pagename(bookname):
    if bookname.startswith("[") and bookname.endswith("]"):  # [四家四六]
        bookname = bookname[1:-1]
    if bookname.startswith("["):
        bookname = re.sub(r"\[(.+?)\]", r"〔\1〕", bookname)  # [宋]...
    bookname = bookname.replace(":", "：")
    return bookname


def main():

    with open(CONFIG_FILE_PATH, "r") as f:
        config = yaml.safe_load(f.read())

    if len(sys.argv) < 2:
        exit(
            f"Not batch specified.\n\nAvailable: {', '.join(list(config['batchs'].keys()))}"
        )
    batch_name = sys.argv[1]

    def getopt(item, default=None):  # get batch config or fallback to global config
        return config["batchs"][batch_name].get(
            item, config.get(item, default)
        )

    with open(os.path.join(DATA_DIR, batch_name + ".json")) as f:
        batch = json.load(f)
    template = getopt("template")
    batch_link = getopt("link") or getopt("name")
    category_name = re.search(r"(Category:.+?)[]|]", batch_link).group(1)

    lines = [
        f'== {getopt("name")} ==',
        f"Category: {batch_link}, Template: [[{template}|{template}]], Books: {len(batch)}, Files: {sum(map(lambda e: len(e['volumes']), batch))}\n",
    ]

    for book in batch:
        book["name"] = book["name"].replace("@@@", " ")
        book["author"] = book["author"].replace("@@@", " ")
        dbid = book["of_collection_name"].removeprefix("data_")
        lines.append(
            f'* 《{book["name"]}》 {book["author"]} {{{{NLC-Book-Link|{dbid}|{book["id"]}|catid={book["of_category_id"]}}}}}'
        )
        volumes = book["volumes"]
        volumes.sort(key=lambda e: e["index_in_book"])
        for ivol, volume in enumerate(volumes):
            volume_name = (
                (volume["name"].replace("_", "–").replace("-", "–") or f"第{ivol+1}冊")
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
            lines.append(f"** [[:{pagename}]]")
    lines.append("")
    lines.append("[[" + category_name + "]]")
    lines.append("")

    if len(sys.argv) < 3:
        print("\n".join(lines))
    else:
        print(f"Writing file list for batch {batch_name}")
        pagename = sys.argv[2]

        username, password = config["username"], config["password"]
        site = mwclient.Site("commons.wikimedia.org")
        site.login(username, password)
        site.requests["timeout"] = 125
        site.chunk_size = 1024 * 1024 * 64

        site.pages[pagename].edit(
            "\n".join(lines), f"Writing file list for batch {batch_name} to {pagename}"
        )


if __name__ == "__main__":
    main()
