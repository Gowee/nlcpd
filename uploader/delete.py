#!/usr/bin/env python3

""""Request deletion for files in batch"""

import yaml
import os
import json
import mwclient

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "config.yml")
DELETE_FILELIST_PATH = os.path.join(os.path.dirname(__file__), "todelete.json")

# select img_name from image
# where img_actor = FOOBAR and img_height = 0;


def main():
    with open(CONFIG_FILE_PATH, "r") as f:
        config = yaml.safe_load(f.read())

    username, password = config["username"], config["password"]
    site = mwclient.Site("commons.wikimedia.org")
    site.login(username, password)
    site.requests["timeout"] = 125
    site.chunk_size = 1024 * 1024 * 64

    with open(DELETE_FILELIST_PATH, "r") as f:
        d = json.load(f)
    l = ["File:" + e[0] for e in d["rows"]]

    for p in l:
        page = site.pages[p]
        if page.exists:
            print("Request deletion for", p)
            page.prepend(
                "{{CSD|Per uploader's request. File truncated or corrupted due to network issues when uploading.}}",
                summary="Request deleteion",
            )
        else:
            print("Page nonexistent:", p)


if __name__ == "__main__":
    main()
