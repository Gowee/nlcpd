#!/usr/bin/env python3
import yaml
import os
import re
import json
import sys
import functools

import mwclient

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "config.yml")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
NAME_CAP_FIX_PATH = os.path.join(DATA_DIR, "namecapfix.yml")


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
    bookname = re.sub(r'"([^"]+)"', "“(\1)”", bookname)  # e.g. NLC-511-09000049
    return bookname


# TODO: implement secondary_volume support


def main():
    with open(CONFIG_FILE_PATH, "r") as f:
        config = yaml.safe_load(f.read())

    if len(sys.argv) < 2:
        exit(
            f"Not batch specified.\n\nAvailable: {', '.join(list(config['batchs'].keys()))}"
        )
    batch_name = sys.argv[1]

    def getopt(item, default=None):  # get batch config or fallback to global config
        return config["batchs"][batch_name].get(item, config.get(item, default))

    with open(os.path.join(DATA_DIR, batch_name + ".json")) as f:
        batch = json.load(f)
    template = getopt("template")
    batch_link = getopt("link") or getopt("name")
    category_name = re.search(r"(Category:.+?)[]|]", batch_link).group(1)
    global fix_bookname_in_pagename
    fix_bookname_in_pagename = functools.partial(
        fix_bookname_in_pagename,
        apply_tortoise_shell_brackets_to_starting_of_title=getopt(
            "apply_tortoise_shell_brackets_to_starting_of_title", False
        ),
    )

    with open(NAME_CAP_FIX_PATH, "r") as f:
        name_fixes = yaml.safe_load(f.read())
    name_fixes = {
        (str(entry["dbid"]), str(entry["bookid"])): entry for entry in name_fixes
    }

    # if pubdate_as_suffix := getopt("pubdate_as_suffix"):
    #     pubdate_as_suffix["incl"] = re.compile(pubdate_as_suffix["incl"])
    #     pubdate_as_suffix["excls"] = [
    #         re.compile(exc) for exc in pubdate_as_suffix["excls"]
    #     ]

    # def should_use_pubdate_as_suffix(title):
    #     return (
    #         pubdate_as_suffix
    #         and pubdate_as_suffix["incl"].search(title)
    #         and not any(exc.search(title) for exc in pubdate_as_suffix["excls"])
    #     )

    lines = [
        f"== {batch_name} ==",
        f"Category: {batch_link}, Template: {{{{Template|{template}}}}}, Books: {len(batch)}, Volumes: {sum(map(lambda e: len(e['volumes']), batch))}\n",
    ]

    lines.append(
        """\
{| class="wikitable sortable"
! file !! name !! byline !! pubdate !! publisher
|-"""
    )

    for book in batch:
        if '"' in (
            author := book["misc_metadata"].get(
                "責任者", book["misc_metadata"].get("责任者", "")
            )
        ):
            book["author"] = author.replace("   ", " ")
        book["name"] = book["name"].replace("@@@", " ")
        book["author"] = book["author"].replace("@@@", " ")
        dbid = book["of_collection_name"].removeprefix("data_")

        capping = name_fixes.get((str(dbid), str(book["id"])))
        book_name_capped = capping["name"] if capping else book["name"]
        # cap_category_name = bool(capping) and capping.get("cap_category_name", False)
        shorten_volume_name = bool(capping) and capping.get(
            "shorten_volume_name", False
        )

        # lines.append(
        #     f'| 《{book["name"]}》 {book["author"]} {{{{NLC-Book-Link|{dbid}|{book["id"]}|catid={book["of_category_id"]}}}}}'
        # )
        volumes = book["volumes"]
        volumes.sort(key=lambda e: e["index_in_book"])
        book_name_suffix_wps = ""
        # if should_use_pubdate_as_suffix(book["name"]) and (
        #     pubdate := book["misc_metadata"].get("出版时间")
        # ):
        #     book_name_suffix_wps = " " + pubdate.replace("[", "(").replace("]", ")")
        rowspan = ""
        if len(volumes) > 1:
            rowspan = f'rowspan="{len(volumes)}" | '
        for ivol, volume in enumerate(volumes):
            if not (
                len(volumes) > 1
                or getopt("always_include_volume_name_in_filename", False)
            ):
                volume_name = ""
            elif shorten_volume_name:
                assert not volume["name"] or re.match(
                    r"^第\d+[册冊卷]$", volume["name"]
                ), volume["name"]
                volume_name = str(volume["index_in_book"] + 1)
            else:
                volume_name = (
                    volume["name"]
                    .replace("_", "–")
                    .replace("-", "–")
                    .replace("/", "–")
                    .replace("\n", " ")
                    or f"第{volume['index_in_book'] + 1}冊"
                )

            volume_name_wps = (
                (" " + volume_name) if volume_name else ""
            )  # with preceding space
            filename = f'NLC{dbid}-{book["id"]}-{volume["id"]} {fix_bookname_in_pagename(book_name_capped)}{book_name_suffix_wps}{volume_name_wps}.pdf'
            pagename = "File:" + filename
            # if secondary_volume := volume.get("secondary_volume"):
            #     secondary_filename = f'NLC{dbid}-{book["id"]}-{secondary_volume["id"]} {fix_bookname_in_pagename(book["name"])}{book_name_suffix_wps}{volume_name_wps}.pdf'
            #     secondary_pagename = "File:" + secondary_filename
            #     lines.append(f"** [[:{pagename}]] ⇔ [[:{secondary_pagename}]]")
            # else:
            #     lines.append(f"** [[:{pagename}]]")
            if ivol == 0:
                assert re.sub(r"(\s|#|@)+", " ", book["author"].strip()) == re.sub(
                    r"(\s|#|@)+",  # .e.g NLC511-027032016010287
                    " ",
                    book["misc_metadata"]
                    .get("責任者", book["misc_metadata"].get("责任者", ""))
                    .strip(),
                ), f"""{repr(book['author'])} != {repr(book['misc_metadata'].get("責任者", book['misc_metadata'].get("责任者", "")))} for NLC{dbid}-{book['id']}"""
                lines.append(
                    f"""| [[:{pagename}]] || {rowspan}{book['name']} {{{{NLC-Book-Link|{dbid}|{book["id"]}|catid={book["of_category_id"]}}}}} || {rowspan}{book['author']} || {rowspan}{book['misc_metadata'].get('出版時間', book['misc_metadata'].get('出版时间', book['misc_metadata'].get('出版年', '')))} || {rowspan}{book['misc_metadata'].get('出版者', '')}
|-"""
                )
            else:
                lines.append(
                    f"""| [[:{pagename}]]
|-"""
                )
    lines.append("|}")
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
