#!/usr/bin/env python3
import json
import re
import os
from tempfile import TemporaryFile
import sys
from math import log, floor
from collections import OrderedDict

import requests
import yaml
from opencc import OpenCC

try:
    from getbook import getbook
except ImportError:
    from data.getbook import getbook

__all__ = ("tasks",)

TARGET = "雲南圖書館古籍"
DATA_FILE_PATH = f"{TARGET}.json"
MAPPINGS_FILE_PATH = f"{TARGET}.yml"

if not os.path.exists(DATA_FILE_PATH):
    DATA_FILE_PATH = "data/" + DATA_FILE_PATH
    MAPPINGS_FILE_PATH = "data/" + MAPPINGS_FILE_PATH


USER_AGENT = "nlcpdbot/0.0 (+https://github.com/gowee/nlcpd)"

S2T = OpenCC("s2t")


def s2t(text):
    return S2T.convert(text)


# actions = []
# books = [process_book(book) for book in books]


def split_name(name):
    if name.endswith("不分卷"):
        return name[:-3], "不分卷"
    match = re.match(r"^(\S+?)\s*([一二三四五六七八九十]+[卷冊册])$", name)
    if match is None:
        return name, ""
    else:
        return match.group(1), match.group(2)


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


def fix_historial_edition_note(note):
    last = None
    parts = []
    for part in note.split():
        if part != last:
            parts.append(part)
        last = part
    return " ".join(parts)


def format_byline(byline):
    byline = byline.replace("（", "(").replace("）", ")").strip()
    return byline


def tasks():
    with open(DATA_FILE_PATH, "r") as f:
        books = json.loads(f.read())

    with open(MAPPINGS_FILE_PATH, "r") as f:
        mappings = yaml.load(f.read())

    gen_filename_base = lambda book: f"{book['name']}"

    for book in books:
        if int(book["id"]) == 290100101:
            # only starting this book, we use the new naming convention
            # previous filenames are kept as-is
            gen_filename_base = (
                lambda book: (f"{book['name']} ({book['misc_metadata']['版本项']})")
                if " " in book["misc_metadata"]["版本项"]
                else f"{book['name']} {book['misc_metadata']['版本项']}"
            )
        title, note = split_name(book["name"])
        if book["author"].strip() != book["misc_metadata"]["责任者"].strip():
            print(book)
        byline = format_byline(book["author"])
        print(title, note, byline)
        introduction = book["introduction"]
        # if re.sub("\s+", " ", book['author']).strip() != book['misc_metadata']['责任者']:
        #     print(book, re.sub("\s+", " ", book['author']).strip(), book['misc_metadata']['责任者'])
        wiki_category = None
        category_wikitext = None
        mapping = mappings.get(int(book["id"]), None)
        if len(book["volumes"]) > 1:
            wiki_category = "Category:" + (
                mapping[0] if mapping and mapping[0] else title
            )
            # TODO: if the category is not with edition name, there might be troubles in the future
            # if we categorize them into NLC/YNL
            category_wikitext = """{{Category for book|zh}}
{{zh-hant|%s}}
%s
{{Wikidata Infobox}}
[[Category:Books in Chinese]]
[[Category:Books in the Yunnan Provincial Library]]
[[Category:Books from the National Library of China]]
%s""" % (
                title
                + (
                    (" " + book["misc_metadata"]["版本项"])
                    if int(book["id"]) in mappings and mapping[0]
                    else ""
                ),
                introduction if introduction else "",
                f"[[Category:{title}]]\n" if mapping and mapping[0] else "",
            )
            yield {
                "name": wiki_category,
                "text": category_wikitext,
                "comment": f"Create category for {title} (batch task; nlc:{book['of_collection_name']},{book['id']}; [[Category:National_Library_of_China-Yunnan_Provincial_Library_Ancient_Books|雲南圖書館古籍]])",
            }
        volumes = book["volumes"]
        for idx, volume in zip(range(1, len(volumes) + 1), volumes):
            volume["name"] = s2t(volume["name"].strip())
            description = "\n".join(
                filter(lambda v: v, [book["introduction"], gen_toc(volume["toc"])])
            )
            metadata = {k: s2t(v.strip()) for k, v in book["misc_metadata"].items()}
            metadata["版本书目史注"] = fix_historial_edition_note(
                metadata["版本书目史注"]
            )  # + f"<!--original: {metadata['版本书目史注']}-->"
            filename = (
                (mapping[1] if mapping and mapping[1] else gen_filename_base(book))
                + (" " + volume["name"] if len(volumes) > 1 else "")
                + ".pdf"
            )

            def file_getter():
                if volume.get("file_path"):
                    url = "http://202.106.125.217/doc3" + volume["file_path"]
                    # APIError: copyuploadbaddomain
                    # To upload by URL, the domain must be whitelisted
                    # ref: https://phabricator.wikimedia.org/T210330
                    # resp = requests.head(url, headers={"User-Agent": USER_AGENT})
                    # if (
                    #     200 <= resp.status_code < 300
                    #     and int(resp.headers["Content-Length"]) >= 256
                    # ):
                    #     return url
                    # print("Downloading from proxy...", end="")
                    sys.stdout.flush()
                    resp = requests.get(url, headers={"User-Agent": USER_AGENT})
                    resp.raise_for_status()
                    return resp.content
                    # print("\r", " " * 30, "\rDownloaded!")
                    # f = open("/tmp/" + filename, "w+b")
                    # f.write(resp.content)
                    # return f
                else:
                    return getbook(
                        aid=int(volume["of_collection_name"].removeprefix("data_")),
                        bid=volume["id"],
                    )

            sort_key = (
                "|"
                + ((metadata["版本项"] if not (mapping and mapping[0]) else "") or "")
                + str(idx).zfill(floor(log(len(volumes), 10)) + 1)
            )
            volume_wikitext = f"""=={{{{int:filedesc}}}}==
{{{{National_Library_of_China-Yunnan_Provincial_Library_Ancient_Books
  |byline={byline}
  |title={title}
  |note={note}
  |volume={volume['name'] if len(volumes) > 1 else ""}
  |description={description}
  |edition={metadata['版本项']}
  |historical edition note={metadata['版本书目史注']}
  |current edition note={metadata['现有藏本附注']}
  |state={metadata['出版发行项']}
  |四部分類={metadata['四部分类号']}
  |db={volume["of_collection_name"]}
  |bookid={book["id"]}
  |volumeid={volume["id"]}
}}}}

=={{{{int:license-header}}}}==
{{{{PD-NLC-Ancient Books}}}}
""" + (
                f"\n[[{wiki_category}{sort_key}]]\n" if wiki_category else ""
            )
            yield {
                "name": filename,
                "text": volume_wikitext,
                "comment": f"Upload {book['name']} by {byline} (batch task; nlc:{volume['of_collection_name']},{book['id']},{volume['id']}; [[Category:National_Library_of_China-Yunnan_Provincial_Library_Ancient_Books|雲南圖書館古籍]]"
                + (f"; [[{wiki_category}|{title}]])" if len(volumes) > 1 else ""),
                "file": file_getter,
            }


# with open(f"./{TARGET}-actions.json", "w") as f:
#     f.write(json.dumps(actions, ensure_ascii=False, indent=2))
if __name__ == "__main__":
    for task in tasks():
        print(task)
