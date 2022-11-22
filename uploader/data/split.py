#!/usr/bin/env python3
import sys
import json


def dosplit(path, limit, sort_books_by_id):
    with open(path) as f:
        d = json.load(f)

    if sort_books_by_id:
        print("sorting book")
        l = max(len(str(b["id"])) for b in d)
        print("  padding book id to ", l, "digits")
        d.sort(key=lambda b: str(b["id"]).rjust(l, "0"))

    buffer = []
    cnt = 0
    n = 1

    def dump_buffer():
        nonlocal n, cnt
        print(f"{n}: {len(buffer)} {cnt}")
        with open(path.replace(".json", f".{n}.json"), "w") as f:
            json.dump(buffer, f, ensure_ascii=False, indent=2)
        buffer.clear()
        cnt = 0
        n += 1

    for b in d:
        if cnt + len(b["volumes"]) > limit:
            # print("Overflowing: ", len(b['volumes']))
            dump_buffer()
        cnt += len(b["volumes"])
        buffer.append(b)
    if buffer:
        dump_buffer()


if __name__ == "__main__":
    try:
        path = sys.argv[1]
        limit = int(sys.argv[2])
    except IndexError:
        exit(f"Usage: {sys.argv[0]} INPUT_JSON CHUNK_SIZE [sort]")

    try:
        sort_books_by_id = sys.argv[3].lower() == "sort"
    except:
        sort_books_by_id = False

    dosplit(path, limit, sort_books_by_id)
