#!/usr/bin/env python3
import json
import sys
import os
import yaml
import re

import mwclient

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "config.yml")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


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

    username, password = config["username"], config["password"]
    site = mwclient.Site("commons.wikimedia.org")
    site.login(username, password)
    site.requests["timeout"] = 125
    site.chunk_size = 1024 * 1024 * 64

    books = []
    for n in range(1, 10 + 1):
        with open(os.path.join(DATA_DIR, f"民國期刊.{n}.json"), "r") as f:
            books.extend(json.load(f))
    for book in books:
        # if len(book["volumes"]) == 1:
        for volume in book["volumes"]:
            volume_name = volume["name"].replace("_", "–").replace("-", "–")
            volume_name_wps = (
                (" " + volume_name) if volume_name else ""
            )  # with preceding space
            # old_filename = f'NLC{book["of_collection_name"].removeprefix("data_")}-{book["id"]}-{volume["id"]} {fix_bookname_in_pagename(book["name"])}.pdf'
            filename = f'NLC{book["of_collection_name"].removeprefix("data_")}-{book["id"]}-{volume["id"]} {fix_bookname_in_pagename(book["name"])}{volume_name_wps}.pdf'
            if "/" not in filename:
                continue
            new_filename = filename.replace("/", "–")
            print(new_filename)
            page = site.pages["File:" + filename.replace("/", "-")]
            assert page.exists
            text = page.text()
            text = re.sub(r"\{\{Rename|.+?\}\}\n", "", text)
            req = f"{{{{Rename|File:{new_filename}|1|Replace hyphen to en dash to keep file naming consistent in the batch upload task. Thanks.}}}}\n"
            text = req + text
            page.edit(text, "Rename request: replace hypen with en dash.")
            # print(f"Requesting to move {old_filename} to {filename}")
            # req = f"{{{{Rename|File:{filename}|1|Per uploader's request. Correct incomplete name. Thanks.}}}}\n"
            # # text = site.pages["File:" + old_filename].text()
            # text = re.sub(r"\{\{Move.+\}\}", "", text)
            # text = (
            #     req
            #     + text
            # )

            # pagename = "File:" + filename
            # print("Updating metadata of ", pagename)
            # page = site.pages[pagename]
            # if not page.exists:
            #     input("Invalid File:" + filename + ", press any key to skip")
            #     continue
            # text = page.text()
            # text = re.sub(r"\{\{Rename|.+?\}\}", "", text )
            # text = re.sub(
            #     r"\|volume=\s*$", f"|volume={volume_name}", text, flags=re.MULTILINE
            # )
            # text = re.sub(r"^==$", "=={{int:filedesc}}==", text, flags=re.MULTILINE)
            # site.pages[pagename].edit(text, "Rename request: hyphen to en dash")

            # site.pages["File:" + old_filename].prepend(req)

        # for volume in book["volumes"]:
        #     volume_name = volume["name"].replace("_", "–").replace("-", "–")
        #     volume_name_wps = (
        #         (" " + volume_name) if volume_name else ""
        #     )  # with preceding space
        #     # old_filename = f'NLC{book["of_collection_name"].removeprefix("data_")}-{book["id"]}-{volume["id"]} {fix_bookname_in_pagename(book["name"])}.pdf'
        #     filename = f'NLC{book["of_collection_name"].removeprefix("data_")}-{book["id"]}-{volume["id"]} {fix_bookname_in_pagename(book["name"])}{volume_name_wps}.pdf'
        #     if "/" not in filename:
        #         continue
        #     new_filename = filename.replace("/", "–")  # en dash
        #     filename = filename.replace("/", "-")  # hyphen
        #     print(f"Requesting to move {filename} to {new_filename}")
        #     req = f"{{{{Rename|File:{filename}|1|Replace hyphen to en dash to keep file naming consistent in the batch upload task. Thanks.}}}}\n"
        #     page = site.pages["File:" + filename]
        #     assert page.exists, "Invalid: " + filename
        #     page.prepend(req)


if __name__ == "__main__":
    main()
