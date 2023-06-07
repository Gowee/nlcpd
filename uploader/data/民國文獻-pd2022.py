#!/usr/bin/env python3

import re
import json
from glob import glob
from split import dosplit

OUT_FILE = "民國文獻-PD2022.json"


def main():
    books = []
    for p in glob("民國文獻.*.json"):
        with open(p, "r") as f:
            books += json.load(f)

    def f(book):
        if len(book["volumes"]) > 15:
            print(book['name'], book['author'])
            return True  # mostly copyright held by non-natural persons
        if re.search(
            r"19[01][0-9]|192[0-6]|1[0-8][0-9][0-9]",
            book["misc_metadata"].get("出版時間", ""),
        ):
            return True
        non_natural_person_or_misc = r"[部委廳省縣處廠會組局院社所團館隊室署場報賑隸]|(?<!主)教|天主|[公人國平]民|民衆|救國|青年|女[子中校]|[分總各]校|故宮|文獻|中心|政[府治策事]|[行民內]政|法[律學]|公共|租界|籌備|[大小中]學|學校|專[科修]|大專|師範|研究|統計|考核|附[中小]|銀行|警[察務]|[治公]安|司令|禁菸|海關|交通|运输|邮政|[稅財]務|外交|公司|聯合|經濟|貿易|紡織|國際|問題|中共|共產|少年|[國省市私公]立|[陸海空三]軍|軍[人事官隊需政]|公園|代表|工作|中央|[全中][國華]|中南|訓練|宣傳|小組|特別|事業|鐵[路道]|建設|討論|協會|社會|[管經]理|基金|董事|講習會|救[濟災]|戰時|大使|互助|讀書|審判|會計|參謀|公報|人文|文書|[分總]會|生活|[聯同]盟|檢查|現代|保安|軍訓|紀念|黨部|識字|特種|教育|設計|僑[務團民胞]|互助|華僑|善後|參議|籌備|復興|監獄|書店|工[業會程]|[大總公]會|促進|實業|水[利災利]|農[事業務林商]|秘書|[實試]驗|第[一二三四五六七八九十]|郭衛[編輯|校勘]|俱樂部|[學商]會|企業|稅|[總內]務|科學|審查|印铸|醫|兄弟會|聖母會|基督|浸會|文學|團體|同志|三民|教養|講[練習]|服務|慰勞|婦女|管理|同[窗學]|佛教|居士|[教禮]堂|慈幼|校[友慶刊]|[後聲支]援|自由|印書館|海[關務]"
        # historical_dynasty = r"\([南北]?[魏蜀吳隋秦漢唐宋元明清朝]\)"
        # contributed by ChatGPT
        historical_dynasty = (
            r"\([新舊前後東南西北]?([楚燕趙魯韓晉魏漢梁宋陳莽唐涼商蜀夏周隋明吳元齊清秦金遼]|春秋戰國|春秋|戰國|五代|十國|五代十國)?朝?\)"
        )
        pd_author = "|".join([non_natural_person_or_misc, historical_dynasty])
        if m := re.search(
            pd_author,
            book["author"],
        ):
            return m.group(0) != "()"
        # FP: ([約刑民]|土地|訴訟|憲|組織|保險|商事?|選舉|六)法$
        if re.search(
            r"中華.+法$|六法|法案|(法令|章程|規則|細則|法規|紀錄|報告書?)(分類)?([匯選簡合彙續][編集]|[概輯譯提]要|[輯一]覽表?|集|全書|補錄|[總簡年]表|年[報鑑]|手冊|(報告)?書?|[匯選簡合彙續]報)?$|工作報告|[特專彙匯]刊$|(大會|選舉|判例)彙刊$|令$|參[議政][會院]|大?會議?[紀記]?錄$|決議案$|聯合會|省憲法|[時新公日周月年旬季政][報刊]",
            book["name"],
        ) and not re.search(
            r"^(土地問題與土地法|政治學與比較憲法|比較憲法)$", book["name"]
        ):  # 比较民法 - 李祖荫 expired
            return True
        return False

    books = list(filter(f, books))
    print("count", len(books))
    with open(OUT_FILE, "w") as f:
        json.dump(books, f)
    dosplit(OUT_FILE, 8500, True)


if __name__ == "__main__":
    main()
