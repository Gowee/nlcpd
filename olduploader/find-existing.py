#!/usr/bin/env python3
import mwclient
import sys
import json
import re

import yaml

site = mwclient.Site("commons.wikimedia.org")


def split_name(name):
    if name.endswith("不分卷"):
        return name[:-3], "不分卷"
    match = re.match(r"^(\S+?)\s*([一二三四五六七八九十]+[卷冊册])$", name)
    if match is None:
        return name, ""
    else:
        return match.group(1), match.group(2)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        exit(f"Usage: python {sys.argv[0]} DATA_JSON [YAML_TO_IGNORE]")
    with open(sys.argv[1], "r") as f:
        d = json.loads(f.read())
    if len(sys.argv) >= 3:
        with open(sys.argv[2], "r") as f:
            ignored = yaml.load(f.read()).keys()
    # TODO: find dups in the current dataset
    renaming = {}
    for book in d:
        if int(book["id"]) in ignored or len(book["volumes"]) <= 1:
            continue
        title, note = split_name(book["name"])
        if len(book["volumes"]) <= 1:
            continue
        wiki_category = "Category:" + title
        #  (
        #     mapping[0]
        #     if mapping and mapping[0]
        #     else title
        # )
        if site.pages[wiki_category].exists:
            # filename = book["name"]
            # if " " not in {book["misc_metadata"]["版本项"]}:
            #     filename += f" {book['misc_metadata']['版本项']}"
            # else:
            #     filename += f" ({book['misc_metadata']['版本项']})"
            filename = None # data.py will handle this
            renaming[int(book["id"])] = [
                f"{title} ({book['misc_metadata']['版本项']})",
                filename,
            ]
        # NOTE: duplicated filename are not checked
    print(yaml.dump(renaming, allow_unicode=True, sort_keys=False))
