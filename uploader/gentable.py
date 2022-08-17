#!/usr/bin/env python3
import yaml
import os
import re
import json

CONFIG_FILE_PATH = "./config.yml"
DATA_DIR = os.path.join(os.getcwd(), "data")


def fix_bookname_in_pagename(bookname):
    if bookname.startswith("[") and bookname.endswith("]"):  # [四家四六]
        bookname = bookname[1:-1]
    if bookname.startswith("["):
        bookname = re.sub(r"\[(.+?)\]", r"〔\1〕", bookname)  # [宋]...
    return bookname


def main():

    with open(CONFIG_FILE_PATH, "r") as f:
        config = yaml.safe_load(f.read())

    with open(os.path.join(DATA_DIR, config["batch"] + ".json")) as f:
        batch = json.load(f)
    template = config["template"]
    batch_link = config["batch_link"] or config["batch"]

    lines = [
        f'== {config["batch"]} ==',
        f"Category: {batch_link}, Template: [[{template}|{template}]], Books: {len(batch)}, Files: {sum(map(lambda e: len(e['volumes']), batch))}\n",
    ]

    for book in batch:
        dbid = book["of_collection_name"].removeprefix("data_")
        lines.append(
            f'* 《{book["name"]}》 {book["author"]} {{{{NLC-Book-Link|{dbid}|{book["id"]}|catid={book["of_category_id"]}}}}}'
        )
        volumes = book["volumes"]
        volumes.sort(key=lambda e: e["index_in_book"])
        for ivol, volume in enumerate(volumes):
            volume_name = (volume["name"] or f"第{ivol+1}冊") if len(volumes) > 1 else ""
            volume_name_wps = (
                (" " + volume_name) if volume_name else ""
            )  # with preceding space
            filename = f'NLC{dbid}-{book["id"]}-{volume["id"]} {fix_bookname_in_pagename(book["name"])}{volume_name_wps}.pdf'
            pagename = "File:" + filename
            lines.append(f"** [[:{pagename}]]")

    print("\n".join(lines))


if __name__ == "__main__":
    main()
