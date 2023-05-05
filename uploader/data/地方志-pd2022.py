#!/usr/bin/env python3

import os
import re
import json
from glob import glob
from split import dosplit

TARGET = os.path.split(__file__)[1].removesuffix("-pd2022.py")
OUT_FILE = TARGET + "-PD2022.json"


def main():
    with open(f"original/{TARGET}.json", "r") as f:
        books = json.load(f)

    def f(book):
        if re.search(
            r"19(7[4-9]|[89][0-9])",
            book["misc_metadata"].get("出版年", ""),
        ):
            return False
        return True

    origcnt = len(books)
    books = list(filter(f, books))
    print(f"count {len(books)}/{origcnt}")
    with open(OUT_FILE, "w") as f:
        json.dump(books, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
