#!/usr/bin/env python3
import sys, json, re, os, logging, traceback
from itertools import count

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
logger = logging.getLogger(__name__)


def main():
    inplace = False

    if len(sys.argv) == 1:
        exit(f"Usage: {sys.argv[0]} [--inplace] <input_files>..")

    args = sys.argv[1:]
    if args[0] == "--inplace":
        logger.info("Fixing in place")
        args = args[1:]
        inplace = True

    dups = []
    for f in args:
        cnt = 0
        logger.info(f"Processing {f}")
        with open(f) as file:
            books = json.load(file)
        for book in books:
            try:
                volumes = book["volumes"][:]  # copy it to avoid re-order it
                volumes.sort(key=lambda e: e["index_in_book"])
                file_paths = [volume["file_path"] for volume in volumes]
                if len(file_paths) == len(set(file_paths)):
                    continue
                assert (
                    len(set(file_paths)) == 1
                ), f"Duplicate file paths in {book['id']} {book['name']} with more than 1 distinct file_paths: {', '.join(file_paths)}"
                first_file_path = volumes[0]["file_path"]
                for nth, volume in zip(count(2), volumes[1:]):
                    new_file_path = re.sub(
                        r"(?<=[^\d])001(?=[^\d])", str(nth).zfill(3), first_file_path
                    )
                    logger.info(
                        f"Set {book['id']} {book['name']} {nth}/{len(volumes)} file path: {new_file_path}"
                    )
                    cnt += 1
                    assert (
                        new_file_path != first_file_path
                    ), f"Invalid first file_path: {', '.join(file_paths)}"
                    volume["file_path"] = new_file_path
                dups.append(book)
            except Exception as e:
                traceback.print_exc()
        if inplace:
            with open(f, "w") as file:
                json.dump(books, file, ensure_ascii=False, indent=2)
            logger.info(f"{cnt} fixed in {f}")
    logger.info(f"{len(dups)} fixed in total")
    if not inplace:
        print(json.dumps(dups, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
