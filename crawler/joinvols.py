#!/usr/bin/env python3
# Join book items with their volume items
# (VIBED)
import json
import sys
from collections import defaultdict

def main():
    books = []
    volumes_by_key = defaultdict(list)

    # Read JSONL from stdin
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        item = json.loads(line)

        # Book item: has "volumes"
        if "volumes" in item:
            books.append(item)
        else:
            key = (
                item.get("of_book_id"),
                item.get("of_collection_name"),
            )
            volumes_by_key[key].append(item)

    # Process books
    for book in books:
        book_id = book.get("id")
        collection = book.get("of_collection_name")
        key = (book_id, collection)

        expected_volume_ids = {
            vid for vid, _ in book.get("volumes", [])
        }

        all_volumes = volumes_by_key.get(key, [])

        matched_volumes = [
            v for v in all_volumes
            if v.get("id") in expected_volume_ids
        ]

        # Redundant volumes: belong to book but not declared
        redundant_volumes = [
            v.get("id") for v in all_volumes
            if v.get("id") not in expected_volume_ids
        ]

        # Sort by index_in_book
        matched_volumes.sort(
            key=lambda v: v.get("index_in_book", 0)
        )

        # Report missing volumes
        found_ids = {v.get("id") for v in matched_volumes}
        missing_ids = expected_volume_ids - found_ids
        if missing_ids:
            print(
                f"[WARN] Book {book_id}: missing volumes {sorted(missing_ids)}",
                file=sys.stderr,
            )

        # Report redundant volumes
        if redundant_volumes:
            print(
                f"[WARN] Book {book_id}: redundant volumes {sorted(redundant_volumes)}",
                file=sys.stderr,
            )

        # Report index gaps
        indexes = [v.get("index_in_book") for v in matched_volumes]
        if indexes:
            expected = list(range(min(indexes), min(indexes) + len(indexes)))
            if indexes != expected:
                print(
                    f"[WARN] Book {book_id}: index_in_book gap {indexes}",
                    file=sys.stderr,
                )

        # Replace volumes field
        book["volumes"] = matched_volumes

        # Output updated book
        print(json.dumps(book, ensure_ascii=False))

if __name__ == "__main__":
    main()
