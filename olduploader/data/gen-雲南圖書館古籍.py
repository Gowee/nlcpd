#!/usr/bin/env python3
import json
import re
import os
import sys
import csv
import datetime
from math import log, floor
from collections import defaultdict
from dataclasses import dataclass

import yaml
from zhconv_rs import zhconv, is_hans
from more_itertools import peekable

TARGET = "雲南圖書館古籍"
DATA_FILE_PATH = f"{TARGET}-20260105.json"
MAPPINGS_FILE_PATH = f"{TARGET}.yml"

if not os.path.exists(DATA_FILE_PATH):
    DATA_FILE_PATH = "data/" + DATA_FILE_PATH
    MAPPINGS_FILE_PATH = "data/" + MAPPINGS_FILE_PATH


@dataclass
class IndexVolume:
    filename: str
    label: str | None = None   # 卷名；单卷为 None

@dataclass
class IndexBook:
    title: str
    volumes: list[IndexVolume]

class IndexBuilder:
    def __init__(
        self,
        base_page: str,
        page_limit: int = 2000,
        size_limit: int = 2 * 1024 * 1024,
    ):
        self.base_page = base_page.rstrip("/")
        self.page_limit = page_limit
        self.size_limit = size_limit

        self.pages: list[list[str]] = [[]]
        self.counts: list[int] = [0]

    def _ensure_page(self):
        if len(self.pages[-1]) >= self.page_limit:
            self.pages.append([])
            self.counts.append(0)

    def add_book(self, book: IndexBook):
        self._ensure_page()

        if len(book.volumes) == 1:
            v = book.volumes[0]
            self.pages[-1].append(f"* [[:File:{v.filename}]]")
            self.counts[-1] += 1
            return

        # multi-volume
        self.pages[-1].append(f"* {book.title}")
        for v in book.volumes:
            self._ensure_page()
            # label = f"：{v.label}" if v.label else ""
            self.pages[-1].append(f"** [[:File:{v.filename}]]")
            self.counts[-1] += 1

    def write_tsv(self, path: str):
        with open(path, "w", encoding="utf-8") as f:
            if len(self.pages) > 1:
                w = csv.writer(f, delimiter="\t", lineterminator="\n")
                total = sum(self.counts)
                for i, page in enumerate(self.pages):
                    text = "\n".join(page) + "\n"
                    assert (
                        len(text.encode("utf-8")) <= self.size_limit
                    ), f"Index page {i} exceeds size limit"
                    w.writerow(
                        [
                            f"{self.base_page}/{i+1:02}",
                            text,
                            f"Count: {self.counts[i]}/{total}",
                            None,
                        ]
                    )
            else:
                f.write(f"{self.base_page}\t\"")
                for line in self.pages[0]:
                    f.write(line + "\n")
                f.write(f"\"\tCount: {self.counts[0]}\t")

def s2t(text):
    return zhconv(text, "zh-hant")

def s2tc(text):
    # s2t is not idempotent, so sometimes we want to be conservative
    # e.g. s2t(s2t(墓志铭)) = s2t(墓志銘) = 墓誌銘
    return zhconv(text, "zh-hant") if is_hans(text) else text

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
    bookname = re.sub(
        r"(?<=\d)\*(?=\d)", "×", bookname
    )  # e.g. 511 006031402010229 新湖北（14.7*21.6）1
    bookname = re.sub(r'"([^"]+)"', r"“\1”", bookname)  # e.g. NLC-511-09000049
    return bookname


def split_name(name):
    if name.endswith("不分卷"):
        return name[:-3].rstrip(), "不分卷"
    m = re.match(r"^(\S+?)\s*([一二三四五六七八九十]+[卷冊册])$", name)
    return (m.group(1), m.group(2)) if m else (name, "")

def gen_toc(toc):
    lines = []
    for no, entry in toc:
        # assert not no
        # no is often emtpy, but there are also rare cases such as 外科精要(data_011,411999024120)
        lines.append(" ".join(filter(None, [no, entry])))
    contents = " <br/>\n".join(lines)
    if contents:
        contents = "<p>\n" + contents + "\n</p>"
        # contents += "\n{{^|The toc provided by NLC may have some volumes missed. To be corrected.}}"
    return contents

def fix_historial_edition_note(note):
    last, parts = None, []
    for part in note.split():
        if part != last:
            parts.append(part)
        last = part
    return " ".join(parts)

def prev_cur_next(iterable):
    it = peekable(iterable)

    prev = None
    for cur in it:
        nxt = it.peek(None)
        yield prev, cur, nxt
        prev = cur

# def format_byline(byline):
#     return byline.replace("（", "(").replace("）", ")").strip()

def main():
    with open(DATA_FILE_PATH) as f:
        books = json.loads(f.read().replace("禦制", "御製").replace("石〓", "石臞").replace("〓堂集", "𢈪堂集"))

    with open(MAPPINGS_FILE_PATH) as f:
        mappings = yaml.safe_load(f.read())

    # read nyj3hi.csv
    external_metadata_map = defaultdict(dict)
    if os.path.exists("nyj3hi.csv"):
        with open("nyj3hi.csv", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                external_metadata_map[row["国家图书馆id"]][row["冊次"]] = row
    # read nlc_ypl_existing_ancient_books.csv
    page_metadata_map = {}
    if os.path.exists("nlc_ypl_existing_ancient_books.csv"):
        with open("nlc_ypl_existing_ancient_books.csv", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                page_metadata_map[row["volumeid"]] = row
    uploads = []
    index = IndexBuilder(
        base_page="Commons:Library back up project/file list/NLC/雲南圖書館古籍"
    )

    renames = []

    gen_filename_base = lambda book: book["name"]

    for book in books:
        book["name"] = s2tc(book["name"]).replace("北徵集", "北征集").replace("詩抄", "詩鈔").replace("文抄", "文鈔").replace("摘抄", "摘鈔").replace("自雲齋", "自芸齋").replace("娵隅烺集", "娵隅集").replace("泛舟吟", "汎舟吟").replace("摘抄", "摘鈔").replace("顂古堂", "賴古堂").replace("拙攸庵", "拙脩庵").replace("吾亦愛吾廬寤", "吾亦愛吾廬寤語").replace("滇南礦廠圖略", "滇南礦廠圖畧").replace("雲薖潑墨", "雲薖淡墨").replace("夜余詩草", "夜餘詩草").replace("抱瓮軒詩文匯稿", "抱甕軒詩文彙稿").replace("劉矩堂太史詩", "劉榘堂太史詩").replace("沈餘小草", "瀋餘小草")
        if str(book["id"]) == "042001001024":
            book["name"] = book["name"].replace("月川未是稿", "月川未是稾")
        external_metadata_by_nth = external_metadata_map.get(str(book["id"]), {})
        # if external_metadata_by_nth and (bookname := next(iter(external_metadata_by_nth.values())).get("title")):
        #     book["name"] = bookname.strip()
        assert "北征集" in book["name"] or not is_hans(book["name"]), f"Book name not in traditional Chinese: '{book['name']}'"

        if int(book["id"]) == 290100101:
            gen_filename_base = (
                lambda book: (
                    f"{book['name']} ({book['misc_metadata']['版本项']})"
                    if " " in book["misc_metadata"]["版本项"]
                    else f"{book['name']} {book['misc_metadata']['版本项']}"
                )
            )

        title, extent = split_name(book["name"])
        # if book["name"] == "晚學集八卷未穀詩集四卷":
        #     # we cannot split it
        #     title, extent = "晚學集八卷未穀詩集四卷", ""
        # elif book["name"] == "紅蕉吟館詩餘   一卷,附紅豆剩曲一卷":
        #     title, extent = "紅蕉吟館詩餘", "一卷,附紅豆剩曲一卷"
        # elif book["name"] == "孫髯翁先生詩集一卷續輯一卷":
        #     title, extent = "孫髯翁先生詩集", "一卷續輯一卷"
        # else:
        if "卷" in title:
            # sometimes, there are multiple books bundled, we just keep the whole name as is
            title, extent = book["name"], ""
            # assert "卷" not in title, f"Title has 卷 in it: '{title}' '{extent}'"
        elif title == "紅樓夢   一百二十回":
            title, extent = "紅樓夢", "一百二十回"
        else:
            assert title in ("嶺南集   (不詳)",) or " " not in title, f"Title has space in it: '{title}'"
        author = s2tc(book["author"].strip())
        if external_metadata_by_nth:
            author = (next(iter(external_metadata_by_nth.values())).get("作者") or author).strip()

        # if author != book['misc_metadata']['责任者'].strip():
        #     print(f"Author mismatch for book id {book['id']}: '{author}' vs '{book['misc_metadata']['责任者']}'", file=sys.stderr)
        mapping = mappings.get(int(book["id"]))
        volumes = book["volumes"]

        category_name = None
        if mapping and mapping[0]: # manually specified old category name
            assert mapping[0] != title or str(book["id"]) in ("290100160",), f"Mapping category name same as generated title for book id {book['id']}"
            category_name = mapping[0]
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
                book["introduction"] or "",
                f"[[Category:{title}]]\n" if mapping and mapping[0] else "",
            )
            uploads.append(("Category:" + category_name, category_wikitext, f"Creating (batch task; nlc:{book['of_collection_name']},{book['id']})", None))

        try:
            external_category = next(iter(external_metadata_by_nth.values())).get("category","").replace("未穀詩集", "未谷詩集").replace("滇南礦場圖略", "滇南礦廠圖畧").replace("巖泉山人", "岩泉山人").replace("孫髯翁先生詩集續輯", "孫髯翁先生詩集").replace("嶰穀詩鈔", "嶰谷詩鈔").replace("彩雲百詠續詠", "彩雲百詠").replace("汎舟呤", "汎舟吟").replace("鷓鴣呤", "鷓鴣吟").replace("藩餘小草","瀋餘小草").replace("紅蕉吟館詩餘", "紅蕉吟館詩存")
        except StopIteration:
            external_category = ""
        new_category_names = re.findall(r"\[\[Category:(.+?)\]\]", external_category)
        if not new_category_names:
            # print(f"Category missing in external metadata for book id {book['id']}, using title {title} as category instead", file=sys.stderr)
            match title:
                case "紅蕉吟館詩餘   一卷,附紅豆剩曲一卷":
                    new_category_names = ["紅蕉吟館詩餘"]
                case _:
                    assert " " not in title, f"Title as category has space in it: '{title}'"
                    new_category_names = [title]
            # assert any(map(lambda s: s in title, ("晚學集八卷未穀詩集", "孫髯翁先生詩集", "龍華山草"))) or new_category_name == f"[[Category:{title}]]", f"Category mismatch for book id {book['id']} {title}"
        for new_category_name in new_category_names:
            assert new_category_name in ("彩雲續詠","寧瘦居續集", "焦氏澹園續集") or "續" not in new_category_name, f"Category name has 續 in it: '{new_category_name}'"
            new_category_wikitext = (
        """\
{{Wikidata Infobox}}
{{Category for book|zh}}
{{zh|%s}}

[[Category:Chinese-language books by title]]
"""
        % new_category_name
    )
            uploads.append(("Category:" + new_category_name, new_category_wikitext, f"Creating (batch task; nlc:{book['of_collection_name']},{book['id']})", None))

        index_vols: list[IndexVolume] = []

        def gen_filenames():
            for volume in volumes:
                volume_name = s2tc(volume["name"].strip())
                edition =  s2tc(book['misc_metadata']['版本项']).strip() #s2t(book['misc_metadata']['版本项'].strip())
                if edition == "null":
                    # for http://read.nlc.cn/allSearch/searchDetail?searchType=all&showType=1&indexName=data_892&fid=290100160
                    edition = ""
                parts = (
                    f'NLC892-{book["id"]}-{volume["id"]}',
                    fix_bookname_in_pagename(book["name"]),
                    edition,
                    volume_name if len(volumes) > 1 else "",
                )
                filename = " ".join(part for part in parts if part) + ".pdf"
                assert "null" not in filename, f"Filename has 'null': '{filename}'"

                assert any(map(lambda s: s in filename, ("墓志", "咏", "北征集"))) or s2t(filename) == filename, f"Filename not in traditional Chinese: '{filename}' vs '{s2t(filename)}', {book['misc_metadata']['版本项']} {is_hans(book['misc_metadata']['版本项'])}"
                assert filename.strip() == filename, f"Filename has leading/trailing spaces: '{filename}'"
                assert "  " not in filename, f"Filename has double spaces: '{filename}'"
                assert len(filename.encode("utf-8")) <= 240, f"Filename too long: '{filename}'"
                assert "〓" not in filename, f"Filename has '〓': '{filename}'"

                yield filename
        
        filenames_it = gen_filenames()

        for nth, ((prev_filename, new_filename, next_filename), volume) in enumerate(
            zip(
                prev_cur_next(filenames_it),
                volumes,
            ),
            start=1,
        ):
            prev_filename = prev_filename or ""
            next_filename = next_filename or ""

            volume_name = s2tc(volume["name"].strip())

            # s2t is intentionally chosen here based on observation of generated data  
            metadata = {s2tc(k): s2t(v.strip()) for k, v in book["misc_metadata"].items()}
            metadata["版本書目史注"] = fix_historial_edition_note(
                metadata["版本書目史注"]
            )

            # if len(external_metadata_by_nth) != 0 and len(external_metadata_by_nth) != len(volumes):
            #     print(f"Volume mismatch for {book['id']}: {len(volumes)} vs {len(external_metadata_by_nth)}", file=sys.stderr)
            external_metadata = external_metadata_by_nth.get(volume_name if len(volumes) > 1 else "", {})
            metadata["yplid"] = external_metadata.get("云南省图书馆id")
            for k in ("普查編號", "索書號", "加工記錄標識號", "板式", "版框", "開本", "其它題名"):
                if k in external_metadata:
                    metadata[k] = external_metadata[k]

            page_metadata = page_metadata_map.get(str(volume["id"]), {})
            for k in ("yplid", "板框", "開本", "普查編號", "索書號", "加工記錄標識號"):
                if page_metadata.get(k):
                    assert k not in metadata or not metadata[k], f"Conflict in existing metadata for {k} for book id {book['id']} volume '{volume_name}': {metadata[k]} vs {page_metadata[k]}"
                    metadata[k] = page_metadata[k]
            
            toc = external_metadata.get("目录") or page_metadata.get("description") or gen_toc(volume.get("toc", []))

            if metadata.get("yplid") is None:
                print(f"YPL id missing for book {book['name']} id {book['id']} volume '{volume_name}' {external_metadata} {page_metadata}", file=sys.stderr)

                # print(f"[http://read.nlc.cn/allSearch/searchDetail?searchType=10025&showType=1&indexName=data_892&fid={book['id']} {book['name']} {volume_name} ({book['id']})]", file=sys.stderr)

            old_filename = (
                (mapping[1] if mapping and mapping[1] else gen_filename_base(book))
                + (" " + volume_name if len(volumes) > 1 else "")
                + ".pdf"
            )
            # assert "  " not in old_filename, f"Old filename has double spaces: '{old_filename}'"

            fields_wikitext = "\n".join(
                [f"  |{k}={'' if v is None else v}" for k, v in metadata.items()]
            )
            navibar_wikitext = ""
            if len(volumes) > 1:
                navibar_wikitext = f"""
{{{{NLCBookNaviBar
  |prev={prev_filename}
  |next={next_filename}
  |nth={nth}
  |total={len(volumes)}
  |catid=10025<!-- searchType -->
  |db={volume["of_collection_name"]}
  |bookid={book["id"]}
  |volumeid={volume["id"]}
}}}}"""
            if category_name:
                categories_wikitext = f"[[Category:{category_name}]]"
            else:
                categories_wikitext = "\n".join(f"[[Category:{new_category_name}]]" for new_category_name in new_category_names)
            wikitext = f"""=={{{{int:filedesc}}}}=={navibar_wikitext}
{{{{National_Library_of_China-Yunnan_Provincial_Library_Ancient_Books
  |db={volume["of_collection_name"]}
  |bookid={book["id"]}
  |title={title}
  |extent={extent}
  |author={author}
  |volumeid={volume["id"]}
  |volumename={volume_name}
  |volumenth={nth}
  |volumetotal={len(volumes)}
  |introduction={book["introduction"] or ""}
  |toc={toc}
{fields_wikitext}
}}}}

=={{{{int:license-header}}}}==
{{{{PD-NLC-Ancient Books}}}}

{categories_wikitext}
"""
#   |edition={metadata['版本项']}
#   |historical edition note={metadata['版本书目史注']}
#   |current edition note={metadata['现有藏本附注']}
#   |state={metadata['出版发行项']}
#   |四部分類={metadata['四部分类号']}

            # source_url = (
            #     # "http://202.106.125.217/doc3" + volume["file_path"]
            #     "http://219.141.182.220/doc3" + volume["file_path"]
            #     if volume.get("file_path")
            #     else None
            # )
            source_url = f"https://nlc2.bamboo.workers.dev/getbook/892/{volume['id']}?redirect=1"

            comment = (
                f"Upload {book['name']} {s2tc(volume['name'])} by {author} "
                f"(batch task; nlc:{volume['of_collection_name']},"
                f"{book['id']},{volume['id']})"
            )

            uploads.append(
                ("File:" + new_filename, wikitext, comment, source_url)
            )

            index_vols.append(
                IndexVolume(
                    filename=new_filename,
                    label=volume_name if len(volumes) > 1 else None,
                )
            )

            renames.append(("File:" + old_filename, "File:" + new_filename))

        assert index_vols, f"No volumes for book id {book['id']}"

        index.add_book(IndexBook(title=title, volumes=index_vols))

    ts = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")

    uploads_path = f"ynlib-uploads-{ts}.tsv"
    with open(uploads_path, "w", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerows(uploads)

    indices_path = f"ynlib-indices-{ts}.tsv"
    index.write_tsv(indices_path)

    renames_path = f"ynlib-renames-{ts}.csv"
    with open(renames_path, "w", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerows(renames)

    print(f"Wrote {uploads_path}, {indices_path}, {renames_path}", file=sys.stderr)

    print("metadata fields:", ", ".join(set(k for k in book["misc_metadata"].keys() for book in books)), file=sys.stderr)

if __name__ == "__main__":
    main()
