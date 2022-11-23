#!/usr/bin/env python3

import re
import json
from glob import glob
from split import dosplit

OUT_FILE = "民國圖書-PD2022.json"


def main():
    books = []
    for p in glob("民國圖書.*.json"):
        with open(p, "r") as f:
            books += json.load(f)

    def f(book):
        if re.search(
            r"19[01][0-9]|192[0-6]|1[0-8][0-9][0-9]",
            book["misc_metadata"].get("出版時間", ""),
        ):
            return True
        if re.search(
            r"[部委廳省縣處廠會組局院社所團館隊室署教場報賑隸]|[公國]民|校(?!堪阅)|救國|青年|故宮|文獻|中心|政[府治策]|行政|法[律學]|公共|租界|籌備|[大小中]學|學校|師範|研究|考核|附[中小]|銀行|警[察務]|稅務|公司|聯合|經濟|貿易|國際|問題|中共|共產|少年|[國省市私公]立|[陸海空三]軍|公園|代表|工作|中央|[全中][國華]|中南|訓練|宣傳|小組|特別|事業|鐵路|建設|討論|協會|社會|管理|基金|董事|講習會|救濟|大使|互助|讀書|立|[分總]會|生活|聯盟|檢查|現代|保安|軍訓|紀念|黨部|識字|特種|教育|善後|籌備|監獄|書店|工[業會程]|水利|水災|農事|秘書|[實試]驗|\([南北]?[隋秦漢唐宋元明清]\)|第[一二三四五六七八九十]|^中華民國|郭衛[編輯|校勘]|俱樂部|學會",
            book["author"],
        ):
            return True
        return False

    books = list(filter(f, books))
    print("count", len(books))
    with open(OUT_FILE, "w") as f:
        json.dump(books, f)
    dosplit(OUT_FILE, 8500, True)  # limit=10000 results in too large wikitext for .2


if __name__ == "__main__":
    main()
