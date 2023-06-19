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
import sys
from itertools import chain
from functools import lru_cache
from datetime import datetime, timezone
from unicodedata import name
from more_itertools import peekable
from typing import Literal

import requests
import yaml
import mwclient
import internetarchive as ia
from mwclient_contenttranslation import CxTranslator

from getbook import getbook

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "config.yml")
POSITION_FILE_PATH = os.path.join(os.path.dirname(__file__), ".position")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
NAME_CAP_FIX_PATH = os.path.join(DATA_DIR, "namecapfix.yml")
RETRY_TIMES = 3

USER_AGENT = "nlcpdbot/0.0 (+https://github.com/gowee/nlcpd)"

# https://stackoverflow.com/a/17280876/5488616
MINIMUM_VALID_PDF_SIZE = 67

# RESP_DUMP_PATH = "/tmp/wmc_upload_resp_dump.html"

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
logger = logging.getLogger(__name__)


def call(command, *args, **kwargs):
    kwargs["shell"] = True
    return subprocess.check_call(command, *args, **kwargs)


def load_position(name):
    logger.info(f'Loading position from {POSITION_FILE_PATH + "." + name}')
    if os.path.exists(POSITION_FILE_PATH + "." + name):
        with open(POSITION_FILE_PATH + "." + name, "r") as f:
            return f.read().strip()
    else:
        return None


def store_position(name, position):
    with open(POSITION_FILE_PATH + "." + name, "w") as f:
        f.write(position)


def retry(times=RETRY_TIMES):
    def wrapper(fn):
        tried = 0

        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            while True:
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    nonlocal tried
                    tried += 1
                    if tried >= times:
                        raise Exception(f"Failed finally after {times} tries") from e
                    logger.info(f"Retrying {fn} {tried}/{times} due to {e}", exc_info=e)

        return wrapped

    return wrapper


def stp(val):
    """Safe template param"""
    if val is None:
        return None
    return val.replace("[[", "[-{}-[")


def gen_toc(toc):
    lines = []
    for no, entry in toc:
        # assert not no
        # no is often emtpy, but there are also rare cases such as 外科精要(data_011,411999024120)
        lines.append(" ".join(filter(None, [no, entry])))
    contents = " <br/>\n".join(lines)
    if contents:
        contents = "<p>\n" + contents + "\n</p>"
        contents += "\n<!--The toc provided by NLC may have some volumes missed. To be corrected.-->"
    return contents


@retry()
def getbook_unified(volume, secondary=False, proxies=None):
    logger.debug(f"Fetching {volume}")
    # if "fileiplogger.info("Failed to get file by path: " + str(e), ", fallbacking to getbook")
    volume_id = volume["id"] if not secondary else volume["secondary_volume"]["id"]
    file_path = (
        volume["file_path"]
        if not secondary
        else volume["secondary_volume"]["file_path"]
    )
    return getbook(
        volume["of_collection_name"].removeprefix("data_"),
        volume_id,
        file_path,
        proxies,
    )


def split_name_heuristic(name):
    if name.endswith("不分卷"):
        return name[:-3], "不分卷"
    match = re.match(r"^(\S+?)\s*([一二三四五六七八九十百]+[卷冊册])$", name)
    if match is None:
        return name, ""
    else:
        return match.group(1), match.group(2)


def split_name_more(name):
    if name.endswith("不分卷"):
        return name[:-3], "不分卷"
    # match = re.match(r"^(\S+)( \S+[册冊卷])*", name)
    match = re.match(r"^(?P<a>.+?)( [(（]?(?P<b>\S+[册冊卷])?[)）]?)?$", name)
    assert match, "Invalid Book Title"
    return match.group("a"), match.group("b") or ""


def split_name_simple(name):
    parts = tuple(name.split(maxsplit=1))
    if len(parts) == 1:
        parts += ("",)
    return parts


def fix_bookname_in_pagename(
    bookname, apply_tortoise_shell_brackets_to_starting_of_title=False
):
    # if bookname.startswith("[") and bookname.endswith("]"):  # [四家四六]
    #     bookname = bookname[1:-1]

    if apply_tortoise_shell_brackets_to_starting_of_title and bookname.startswith("["):
        bookname = re.sub(r"\[(.+?)\]", r"〔\1〕", bookname)  # [宋]...
    bookname = re.sub(r"\[(.+?)\]", r"\1", bookname)
    bookname = bookname.replace(":", "：")  # e.g. 404 00J001624 綠洲:中英文藝綜合月刊
    bookname = bookname.replace("###", " ").replace("@@@", " ")
    bookname = re.sub(r"\s+", " ", bookname)
    bookname = bookname.replace("?", "□").replace(
        "○", "〇"
    )  # WHITE SQUARE, U+25A1, for, e.g. 892 312001039388 筠清?金石文字   五卷"
    bookname = re.sub(r'"([^"]+)"', r"“\1”", bookname)  # e.g. NLC-511-09000049
    return bookname


def main():
    with open(CONFIG_FILE_PATH, "r") as f:
        config = yaml.safe_load(f.read())

    if len(sys.argv) < 2:
        exit(
            f"Not batch specified.\n\nAvailable: {', '.join(list(config['batchs'].keys()))}"
        )
    batch_name = sys.argv[1]
    up2ia = len(sys.argv) >= 3 and sys.argv[2].strip() == "--ia"

    site: mwclient.Site = None  # to make linter happy
    cxtrans: CxTranslator = None
    translate_bookname_and_byline = lambda a, b: None
    if not up2ia:
        username, password = config["username"], config["password"]
        site = mwclient.Site("commons.wikimedia.org")
        site.login(username, password)
        site.requests["timeout"] = 125
        site.chunk_size = 1024 * 1024 * 64
        logger.info(f"Signed in as {username} on Commons")
    else:
        username, password = config["username"], config["password"]
        iausername, iapassword = config["iausername"], config["iapassword"]
        ia.configure(iausername, iapassword, config_file="./.ia.ini")
        logger.info(f"Uploading to IA as {username}")
        site = mwclient.Site("zh.wikipedia.org")
        site.login(username, password)
        cxtrans = CxTranslator(site)

        @lru_cache(16)
        def translate_bookname_and_byline(bookname, byline):
            n, b = cxtrans.translate_text(
                (f"《{bookname}》", f"本書由：{byline}"), "zh", "en"
            )
            if n.startswith('"') and n.endswith('"'):
                n = n[1:-1]
            if (i := b.find("by:")) != -1:
                b = b[i + 3 :]
            elif ":" not in bookname and "：" not in bookname and (i := b.find(":")):
                b = b[i + 1 :]
            elif m := re.split(r"[Bb]ook (is |was )?([a-z]+ )by"):
                b = m[1]
            return n.strip().title(), b.strip().title()

    overwriting_categories = {
        (str(item["dbid"]), str(item["bookid"])): item["catname"]
        for item in chain(
            config.get("overwriting_categories", []),
            config["batchs"][batch_name].get("overwriting_categories", []),
        )
    }

    def getopt(item, default=None):  # get batch config or fallback to global config
        return config["batchs"][batch_name].get(item, config.get(item, default))

    nlc_proxies = getopt("nlc_proxies", None)

    with open(os.path.join(DATA_DIR, batch_name + ".json")) as f:
        books = json.load(f)
    template = getopt("template")
    batch_link = getopt("link") or getopt("name")
    global fix_bookname_in_pagename
    fix_bookname_in_pagename = functools.partial(
        fix_bookname_in_pagename,
        apply_tortoise_shell_brackets_to_starting_of_title=getopt(
            "apply_tortoise_shell_brackets_to_starting_of_title", False
        ),
    )
    if getopt("split_name", "").lower() == "simple":
        split_name = split_name_simple
    elif getopt("split_name", "").lower() == "heuristic":
        split_name = split_name_heuristic
    elif getopt("split_name", "").lower() == "more":
        split_name = split_name_more
    else:
        split_name = lambda s: (s, "")

    with open(NAME_CAP_FIX_PATH, "r") as f:
        name_fixes = yaml.safe_load(f.read())
    name_fixes = {
        (str(entry["dbid"]), str(entry["bookid"])): entry for entry in name_fixes
    }

    booknavi = getopt("booknavi", "BookNaviBar2")

    if pubdate_as_suffix := getopt("pubdate_as_suffix"):
        pubdate_as_suffix["incl"] = re.compile(pubdate_as_suffix["incl"])
        pubdate_as_suffix["excls"] = [
            re.compile(exc) for exc in pubdate_as_suffix["excls"]
        ]

    def should_use_pubdate_as_suffix(title):
        return (
            pubdate_as_suffix
            and pubdate_as_suffix["incl"].search(title)
            and not any(exc.search(title) for exc in pubdate_as_suffix["excls"])
        )

    watermark_tag = getopt("watermark_tag", False)
    watermark_tag_for_secondary = getopt("watermark_tag_for_secondary", None)

    log_page_name = getopt("logpage", f'User:{config["username"].split("@")[0]}/log')

    iacollection = getopt("iacollection", "test_collection")
    ia_subject_from_metadata_field = getopt("ia_subject_from_metadata_field", "主題")
    ia_publisher_from_metadata_field = getopt("ia_publisher_from_metadata_field", "出版者")
    ia_pubdate_from_metadata_field = getopt("ia_pubdate_from_metadata_field", "出版時間")
    ia_title_pinyin_from_metadata_field = getopt(
        "ia_title_pinyin_from_metadata_field", "拼音題名"
    )
    abstract_from_metadata_field = getopt("abstract_from_metadata_field", "摘要")

    def log_to_remote(l):
        if up2ia:
            return  # TODO: no remote logging when up2ia
        # NOTE: possible racing condition since no sync & lock
        d = str(datetime.now(timezone.utc))
        page = site.pages[log_page_name]
        wikitext = ""
        if page.exists:
            wikitext = page.text()
        wikitext += f"\n* <code>{d} - {batch_name}</code> " + l + "\n"
        logger.debug(f"add log to wiki: {l}")
        page.edit(wikitext, f"Log (batch:nlc; {batch_link}): {l}")

    last_position = load_position(batch_name)

    if last_position is not None:
        books = iter(books)
        logger.info(f"Last processed: {last_position}")
        next(
            itertools.dropwhile(lambda book: str(book["id"]) != last_position, books)
        )  # lazy!
        # TODO: peek and report?

    failcnt = 0

    for book in books:
        assert "\uf8ff" not in book["author"]
        if '"' in (
            author := book["misc_metadata"].get(
                "責任者", book["misc_metadata"].get("责任者", "")
            )
        ):
            # book["author"] is extracted from input[type=text] while the latter is extracted from
            # HTML text
            # the NLC system failed to do espace properly, resulted in malformed input tag when
            # there is a double quote in the value
            # so just fallback to the latter field here
            book["author"] = author.replace("   ", " ")
        byline = book["author"]
        byline_enclosing_brackets = False
        if book["author"].startswith("[") and book["author"].endswith("]"):
            byline = byline[1:-1]
            byline_enclosing_brackets = True
            # assert "[" not in byline # disabled due to: 411999012181 [(明)周士佐[等]修]
        if getopt("apply_tortoise_shell_brackets_to_starting_of_byline", False):
            # e.g. "(魏)王弼,(晋)韩康伯撰   (唐)邢璹撰"
            atsb = lambda s: re.sub(
                r"^([（(〔[][题題][]）)〕])?[（(〔[](.{0,3}?)[]）)〕]",
                r"\1〔\2〕",
                s,
            )
        else:
            atsb = lambda s: s
        # e.g. "（英國）韋廉臣（Williams,W.）撰"
        byline = re.sub(r"(（[^）]+?)(,)([^）]+?）)", "\\1\uf8ff\\3", byline)
        byline = " <br />\n".join(
            " ".join(atsb(aauthor) for aauthor in re.split(r"[，,、]", author))
            for author in re.split(
                r"@@@|###@@@|   ",
                byline,
            )
        )
        byline = byline.replace("\uf8ff", ",")
        if byline_enclosing_brackets:
            byline = "[" + byline + "]"
        title, note_in_title = split_name(
            book["name"].replace("?", "□").replace("○", "〇")
        )
        title = re.sub(r"\s+", " ", title)
        note_in_title = re.sub(r"\s+", " ", note_in_title)
        if getopt("split_name", None) is not None:
            nit_field = f"  |note_in_title={note_in_title}\n"
        else:
            nit_field = ""
        if "@@@" in title:
            # single \n does not render as expected
            title = title.replace("###@@@", "@@@").replace("@@@", "\n\n")
            # Now line feed is used in place of space, so we do not need this
            # if getopt("apply_gbt3792_7_brackets_to_title", False):
            #     title = "[" + title + "]"
            # http://www.nlc.cn/pcab/gjbhzs/bm/201412/P020150309516939790893.pdf §8.1.4, §8.1.6

        assert not (
            book["misc_metadata"].get(abstract_from_metadata_field)
            and book["introduction"]
        )

        dbid = book["of_collection_name"].removeprefix("data_")

        capping = name_fixes.get((str(dbid), str(book["id"])))
        book_name_capped = capping["name"] if capping else book["name"]
        cap_category_name = bool(capping) and capping.get("cap_category_name", False)
        shorten_volume_name = bool(capping) and capping.get(
            "shorten_volume_name", False
        )

        book_name_suffix_wps = ""
        if should_use_pubdate_as_suffix(book["name"]) and (
            pubdate := book["misc_metadata"].get("出版时间")
        ):
            book_name_suffix_wps = " " + pubdate.replace("[", "(").replace("]", ")")

        volumes = book["volumes"]
        volumes.sort(key=lambda e: e["index_in_book"])

        def get_volume_name_for_filename(volume, last_volume):
            if not (
                len(volumes) > 1
                or getopt("always_include_volume_name_in_filename", False)
            ):
                return ""
            if shorten_volume_name:
                assert not volume["name"] or re.match(
                    r"^第\d+[册冊卷]$", volume["name"]
                ), volume["name"]
                return str(volume["index_in_book"] + 1)
            if volume["name"]:
                return (
                    volume["name"]
                    .replace("_", "–")
                    .replace("-", "–")
                    .replace("/", "–")
                    .replace("\n", " ")
                )
            else:
                if last_volume and last_volume["name"]:
                    assert re.match(r"^第\d+[册冊卷]$", last_volume["name"]), last_volume[
                        "name"
                    ]
                    unit = last_volume["name"][-1]
                else:
                    unit = "冊"
                return f"第{volume['index_in_book'] + 1}{unit}"

        metadata = book["misc_metadata"]
        if not up2ia:
            additional_fields = "\n".join(
                f"  |{k}={stp(v)}" for k, v in metadata.items()
            )
            if cap_category_name:
                category_name = (
                    "Category:" + book_name_capped
                )  # Does not handle for nit for now
            elif (k := (str(dbid), str(book["id"]))) in overwriting_categories:
                category_name = "Category:" + overwriting_categories[k]
            else:
                category_name = "Category:" + fix_bookname_in_pagename(title)
            category_page = site.pages[category_name]
            # TODO: for now we do not create a seperated category suffixed with the edition
            if not category_page.exists:
                category_wikitext = (
                    """{{Wikidata Infobox}}
{{Category for book|zh}}
{{zh|%s}}

[[Category:Chinese-language books by title]]
"""
                    % title
                )
                category_page.edit(
                    category_wikitext,
                    f"Creating (batch task; nlc:{book['of_collection_name']},{book['id']})",
                )

            def genvols():
                seen_file_paths = set()
                for ivol, volume in enumerate(volumes):
                    abstract = metadata.get(
                        abstract_from_metadata_field,
                        book["introduction"].replace("###", "@@@").replace("@@@", "\n")
                        or None,
                    )
                    toc = gen_toc(volume["toc"])
                    volume_name = get_volume_name_for_filename(
                        volume, volume[ivol - 1] if ivol >= 1 else None
                    )
                    volume_name_wps = (
                        (" " + volume_name) if volume_name else ""
                    )  # with preceding space
                    filename = f'NLC{dbid}-{book["id"]}-{volume["id"]} {fix_bookname_in_pagename(book_name_capped)}{book_name_suffix_wps}{volume_name_wps}.pdf'
                    pagename = "File:" + filename

                    if volume["file_path"] in seen_file_paths:
                        logger.warning(
                            f"{filename} ({ivol + 1}/{len(volumes)}) duplicate with a previous volume of the book: {volume['file_path']} of {len(seen_file_paths)})"
                        )
                    seen_file_paths.add(volume["file_path"])

                    secondary_task = None
                    if secondary_volume := volume.get("secondary_volume"):
                        secondary_filename = f'NLC{dbid}-{book["id"]}-{secondary_volume["id"]} {fix_bookname_in_pagename(book_name_capped)}{book_name_suffix_wps}{volume_name_wps}.pdf'
                        secondary_pagename = "File:" + secondary_filename
                        comment = f"Upload {book['name']}{volume_name_wps} ({1+ivol}/{len(volumes)}) by {book['author']} (batch task; nlc:{book['of_collection_name']},{book['id']},{volume['id']},primary_to:[[{secondary_pagename}|{secondary_volume['id']}]]; {batch_link}; [[{category_name}|{title}]])"
                        secondary_comment = f"Upload {book['name']}{volume_name_wps} ({1+ivol}/{len(volumes)}) by {book['author']} (batch task; nlc:{book['of_collection_name']},{book['id']},{secondary_volume['id']},secondary_to:[[{pagename}|{volume['id']}]]; {batch_link}; [[{category_name}|{title}]])"
                        secondary_task = (
                            secondary_volume,
                            secondary_comment,
                            secondary_filename,
                            secondary_pagename,
                        )
                    else:
                        comment = f"Upload {book['name']}{volume_name_wps} ({1+ivol}/{len(volumes)}) by {book['author']} (batch task; nlc:{book['of_collection_name']},{book['id']},{volume['id']}; {batch_link}; [[{category_name}|{title}]])"

                    yield (
                        volume,
                        volume_name,
                        abstract,
                        toc,
                        comment,
                        filename,
                        pagename,
                        secondary_task,
                    )

            volsit = peekable(genvols())
            prev_filename = None
            prev_secondary_filename = None
            for (
                volume,
                volume_name,
                abstract,
                toc,
                comment,
                filename,
                pagename,
                secondary_task,
            ) in volsit:
                try:
                    next_filename = volsit.peek()[5]
                except StopIteration:
                    next_filename = None
                if secondary_task is not None:
                    (
                        secondary_volume,
                        secondary_comment,
                        secondary_filename,
                        secondary_pagename,
                    ) = secondary_task
                    try:
                        next_secondary_task = volsit.peek()[-1]
                        (
                            _next_secondary_volume,
                            _next_secondary_comment,
                            next_secondary_filename,
                            _next_secondary_pagename,
                        ) = next_secondary_task
                    except StopIteration:
                        next_secondary_task = None

                def do_upload(
                    filename, pagename, volume_wikitext, comment, secondary=False
                ):
                    nonlocal failcnt
                    assert all(char not in set(r'["$*|\]</^>@#') for char in filename)
                    page = site.pages[pagename]
                    try:
                        if not page.exists or not page.imageinfo:
                            volume_id = (
                                volume["id"]
                                if not secondary
                                else volume["secondary_volume"]["id"]
                            )
                            note = " (secondary)" if secondary else ""
                            logger.info(
                                f'Downloading {dbid},{book["id"]},{volume_id}{note}'
                            )
                            binary = getbook_unified(volume, secondary, nlc_proxies)
                            # https://stackoverflow.com/a/17280876/5488616
                            if len(binary) < MINIMUM_VALID_PDF_SIZE:
                                log_to_remote(
                                    f"[[:{pagename}]] is too small ({len(binary)} < {MINIMUM_VALID_PDF_SIZE}) to be well-formed"
                                )
                                raise Exception(
                                    f"PDF is too small ({len(binary)} < {MINIMUM_VALID_PDF_SIZE})"
                                )
                            logger.info(f"Uploading {pagename} ({len(binary)} B)")

                            @retry()
                            def do1():
                                r = site.upload(
                                    BytesIO(binary),
                                    filename=filename,
                                    description=volume_wikitext,
                                    comment=comment,
                                )
                                r = r or {}
                                if r.get("warnings", {}).get("exists"):
                                    logger.warning(
                                        "Conflicts with existing page. Is there another worker running in parallel?"
                                    )
                                elif dup := r.get("warnings", {}).get("duplicate"):
                                    assert len(dup) == 1, f"{dup}"
                                    dup = dup[0]
                                    if dup.startswith(
                                        re.match(r"NLC\d+-[\w-]+-\d+", filename).group(
                                            0
                                        )
                                    ):
                                        logger.warning(
                                            f"duplicate volume files in a single book: {dup} = {filename}"
                                        )
                                    else:
                                        r = page.edit(
                                            f"#REDIRECT [[File:{dup}]]",
                                            comment
                                            + f" (Redirecting to [[File:{dup}]])",
                                        )
                                    assert (
                                        r.get("result") == "Success"
                                    ), f"Redirection failed {r}"
                                    log_to_remote(
                                        f"[[:{pagename}]] duplicates with the existing [[:File:{dup}]] ({len(binary)}B)"
                                    )
                                else:
                                    assert (
                                        r.get("result")
                                        or r.get("upload", {}).get("result")
                                    ) == "Success", f"Upload failed {r}"

                            do1()
                        else:
                            if getopt("skip_on_existing", False):
                                logger.debug(f"{pagename} exists, skipping")
                            else:
                                logger.info(f"{pagename} exists, updating wikitext")

                                @retry()
                                def do2():
                                    r = page.edit(
                                        volume_wikitext,
                                        comment + " (Updating metadata)",
                                    )
                                    assert (r or {}).get(
                                        "result", {}
                                    ) == "Success", f"Update failed {r}"

                                do2()
                    except Exception as e:
                        failcnt += 1
                        log_to_remote(f"[[:{pagename}]] upload failed")
                        logger.warning("Upload failed", exc_info=e)
                        if not getopt("skip_on_failures", False):
                            raise e

                nth = volume["index_in_book"] + 1
                common_fields = f"""\
  |byline={stp(byline)}
  |title={title}
{nit_field}  |volume={stp(volume_name)}
  |abstract={stp(abstract) or ""}
  |toc={stp(toc) or ""}
  |catid={book['of_category_id']}
  |db={volume["of_collection_name"]}
  |dbid={dbid}
  |bookid={book["id"]}
  |volumenth={nth}
  |volumetotal={len(volumes)}\
"""
                if secondary_task is None:
                    primary_volume_wikitext = f"""=={{{{int:filedesc}}}}==
{{{{{booknavi}|prev={prev_filename or ""}|next={next_filename or ""}|nth={nth}|total={len(volumes)}|catid={book['of_category_id']}|db={volume["of_collection_name"]}|dbid={dbid}|bookid={book["id"]}|volumeid={volume["id"]}}}}}
{{{{{template}
{common_fields}
  |volumeid={volume["id"]}
{additional_fields}
}}}}
{"{{Watermark}}" if watermark_tag else ""}

[[{category_name}]]
"""

                    do_upload(filename, pagename, primary_volume_wikitext, comment)
                else:
                    primary_volume_wikitext = f"""=={{{{int:filedesc}}}}==
{{{{{booknavi}|prev={prev_filename or ""}|next={next_filename or ""}|secondaryvolume={secondary_filename}|nth={volume['index_in_book'] + 1}|total={len(volumes)}|catid={book['of_category_id']}|db={volume["of_collection_name"]}|dbid={dbid}|bookid={book["id"]}|volumeid={volume["id"]}|secondaryvolumeid={secondary_volume["id"]}}}}}
{{{{{template}
{common_fields}
  |volumeid={volume["id"]}
  |secondaryvolume={secondary_filename}
  |secondaryvolumeid={secondary_volume["id"]}
{additional_fields}
}}}}
{"{{Watermark}}" if watermark_tag else ""}

[[{category_name}]]
"""
                    secondary_volume_wikitext = f"""=={{{{int:filedesc}}}}==
{{{{{booknavi}|prev={prev_secondary_filename or ""}|next={next_secondary_filename or ""}|primaryvolume={filename}|nth={volume['index_in_book'] + 1}|total={len(volumes)}|catid={book['of_category_id']}|db={volume["of_collection_name"]}|dbid={dbid}|bookid={book["id"]}|volumeid={secondary_volume["id"]}|primaryvolumeid={volume["id"]}}}}}
{{{{{template}
{common_fields}
  |volumeid={secondary_volume["id"]}
  |primaryvolume={filename}
  |primaryvolumeid={volume["id"]}
{additional_fields}
}}}}
{"{{Watermark}}" if watermark_tag and watermark_tag_for_secondary != False else ""}

[[{category_name}]]
"""
                    do_upload(
                        filename,
                        pagename,
                        primary_volume_wikitext,
                        comment,
                        secondary=False,
                    )
                    do_upload(
                        secondary_filename,
                        secondary_pagename,
                        secondary_volume_wikitext,
                        secondary_comment,
                        secondary=True,
                    )
                prev_secondary_filename = filename
                prev_filename = filename
        else:  # up2ia
            identifier = f"nlc{dbid}-{book['id']}"
            description = []
            if abstract := metadata.get(
                abstract_from_metadata_field, book["introduction"] or None
            ):  # but book['introduction'] always empty?
                abstract = abstract.replace("###", "@@@").replace("@@@", "\n")
                description.append(f'<section id="abstract">{abstract}</section>')
            if len(book["volumes"]) > 1:
                sections = []
                for ivol, volume in enumerate(volumes):
                    volume_name = volume["name"]
                    lines = []
                    if volume["toc"]:
                        for line in volume["toc"]:
                            lines.append(
                                f'<li><span class="chapter-no">{line[0]}</span>&nbsp;<span class="chapter-title">{line[1]}</span></li>'
                            )
                    sections.append(
                        f'<li data-volume-name="{volume_name}"><h4>{volume_name}</h4><ol class="chapter-list" style="list-style: inherit; padding-left: 0.5em;">'
                        + "".join(lines)
                        + "</ol></li>"
                    )
                toc = (
                    '<ol style="list-style: none; padding-left: 0;">'
                    + "".join(sections)
                    + "</ol>"
                )
            else:
                volume_name = volumes[0]["name"]
                lines = []
                for line in volumes[0]["toc"]:
                    lines.append(
                        f'<li><span class="chapter-no">{line[0]}</span><span class="chapter-title">{line[1]}</span></li>'
                    )
                toc = (
                    f'<ol class="chapter-list" data-volume-name="{volume_name}" style="list-style: none; padding-left: 0.5em;">  '
                    + "  ".join(lines)
                    + "</ol>"
                )
            if toc:
                toc = '<section id="toc"><h3>目錄 / ToC</h3>' + toc + "</section>"
            description.append(toc)
            description = "\n".join(description)
            # print(description)
            title_alt, creator_alt = translate_bookname_and_byline(
                fix_bookname_in_pagename(book["name"]), byline
            )
            if pinyin := metadata.get(ia_title_pinyin_from_metadata_field):
                title_alt += f" ({pinyin})"
            creator_alt = re.sub(r"\s*(<br>\s*(</br>)?|<br\s*/>)\s*", "\n", creator_alt)
            iametadata = {
                "collection": [
                    iacollection,
                    "community",  # default collection
                ],
                "mediatype": "texts",
                "title": fix_bookname_in_pagename(book["name"]) + book_name_suffix_wps,
                "title-alt-script": title_alt,
                "creator": re.sub(r"<br ?/>\n", "\n", byline),
                "creator-alt-script": creator_alt,
                "publisher": metadata.get(ia_publisher_from_metadata_field),
                "date": metadata.get(ia_pubdate_from_metadata_field),
                "language": "Chinese (Traditional)",
                "contributor": "National Library of China",
                "subject": metadata.get(ia_subject_from_metadata_field),
                "identifier-bib": identifier,  # ?
                "scanner": f"nlcpdbot/0.0 via Internet Archive Python library {ia.__version__}",
                "source": f"http://read.nlc.cn/allSearch/searchDetail?searchType=&showType=1&indexName={book['of_collection_name']}&fid={book['id']}",
                "description": description,
            }
            existing_item = None
            try:
                existing_item = next(
                    iter(
                        ia.search_items("identifier-bib:" + identifier).iter_as_items()
                    )
                )
            except StopIteration:
                pass
            if existing_item:
                assert existing_item.metadata["identifier-bib"] == identifier
                logger.info(
                    f"Updating metadata for {identifier} (IA: {existing_item.identifier})"
                )
                r = existing_item.modify_metadata(existing_item.metadata | iametadata)
                if r.status_code != 200:
                    logger.warning(
                        f"Failed to update metadata for {identifier}: "
                        + r.content.decode("utf-8")
                    )
                existing_item.filemap = {
                    f["identifier-bib"]: f
                    for f in existing_item.files
                    if f["source"] == "original" and "identifier-bib" in f
                } | {
                    f["name"]: f
                    for f in existing_item.files
                    if f["source"] == "original"
                }

            for ivol, volume in enumerate(volumes):
                volume_identifier = f'nlc{dbid}-{book["id"]}-{volume["id"]}'
                if f := (
                    existing_item and existing_item.filemap.get(volume_identifier)
                ):
                    logger.debug(
                        f"{volume_identifier} exists in {identifier} as {f['title']}"
                    )
                    continue
                volume_name = get_volume_name_for_filename(
                    volume, volume[ivol - 1] if ivol >= 1 else None
                )
                volume_name_wps = (
                    (" " + volume_name) if volume_name else ""
                )  # with preceding space
                filename = f'NLC{dbid}-{book["id"]}-{volume["id"]} {fix_bookname_in_pagename(book["name"])}{book_name_suffix_wps}{volume_name_wps}.pdf'
                if f := (existing_item and existing_item.filemap.get(filename)):
                    logger.debug(f"{f['title']} exists in {identifier}")
                    continue
                iafilemetadata = {
                    "title": volume_name or None,
                    # actually, not a speciaal field in file metadata
                    "identifier-bib": volume_identifier,
                    "track": f"{ivol + 1}/{len(volumes)}",
                    # "comment": gen_toc(volume["toc"]),
                }

                @retry()
                def do_upload():
                    logger.info(
                        f'Downloading {dbid},{book["id"]},{volume["id"]} ({ivol + 1}/{len(volumes)})'
                    )
                    binary = getbook_unified(volume)
                    # https://stackoverflow.com/a/17280876/5488616
                    if len(binary) < MINIMUM_VALID_PDF_SIZE:
                        log_to_remote(
                            f"[[:{pagename}]] is too small ({len(binary)} < {MINIMUM_VALID_PDF_SIZE}) to be well-formed"
                        )
                        raise Exception(
                            f"PDF is too small ({len(binary)} < {MINIMUM_VALID_PDF_SIZE})"
                        )

                    logger.info(
                        f"Uploading {filename} as {volume_name or 'the only volume'} to {identifier} ({len(binary)} B)"
                    )
                    r = ia.upload(
                        identifier,
                        [
                            {
                                "name": [filename, BytesIO(binary)],
                            }
                            | iafilemetadata
                        ],
                        metadata=iametadata,
                    )[0]
                    if r.status_code != 200 or r.json().get("success") != True:
                        logger.warning(
                            f"HTTP error {r.status_code} when uploading: {r.content}"
                        )
                        raise Exception(f"HTTP error {r.status_code} when uploading")

                try:
                    do_upload()
                except Exception as e:
                    failcnt += 1
                    log_to_remote(f"[[:{pagename}]] upload failed")  # TODO: <-
                    logger.warning("Upload failed", exc_info=e)
                    if not getopt("skip_on_failures", False):
                        raise e
        store_position(batch_name, book["id"])
    logger.info(f"Batch done with {failcnt} failures.")
    log_to_remote(f"{batch_name} finished with {failcnt} failures.")


if __name__ == "__main__":
    main()
