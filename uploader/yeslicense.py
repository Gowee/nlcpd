#!/usr/bin/env python3
import json
import sys
import os
import yaml
import re

import mwclient

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "config.yml")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def main():
    with open(CONFIG_FILE_PATH, "r") as f:
        config = yaml.safe_load(f.read())

    username, password = config["username"], config["password"]
    site = mwclient.Site("commons.wikimedia.org")
    site.login(username, password)
    site.requests["timeout"] = 125
    site.chunk_size = 1024 * 1024 * 64

    with open(sys.argv[1]) as f:
        q = json.load(f)

    for row in q["rows"]:
        pagename = "File:" + row[2].replace(".PDF", ".pdf")
        print("Fix", pagename)
        page = site.pages[pagename]
        assert page
        text = page.text()
        text = re.sub(r"{{User:AntiCompositeBot/NoLicense/tag.+?}}\n", "", text)
        page.edit(text, "The license template should be present.")


if __name__ == "__main__":
    main()
