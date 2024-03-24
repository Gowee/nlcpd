#!/usr/bin/env python3
import sys, json, re, os, logging, traceback
from itertools import count
import functools
import requests

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
logger = logging.getLogger(__name__)

FILE_URL = "http://read.nlc.cn/{server}{file_path}"

VACANT_VOLUME_ID_STARTING = 999990015


def retry(times=3):
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
                    if tried == times:
                        raise Exception(f"Failed finally after {times} tries") from e
                    logger.info(f"Retrying {fn} due to {e}", exc_info=e)

        return wrapped

    return wrapper


def main():
    # inplace = False

    if len(sys.argv) <= 2:
        exit(f"Usage: {sys.argv[0]} [server] <input_files>..")

    args = sys.argv[1:]
    server = args[0]
    assert server in ("doc1", "doc2", "doc3"), "Invalid server specified"
    args = args[1:]
    # if args[1] == "--inplace":
    #     logger.info("Fixing in place")
    #     args = args[2:]
    #     inplace = True
    vacant_volume_id = VACANT_VOLUME_ID_STARTING
    for f in args:
        problematics = []
        vcnt = 0
        bcnt = 0
        logger.info(f"Processing {f}")
        with open(f) as file:
            books = json.load(file)
        # books = iter(books)
        # for book in books:
        #     if book['id'] == '08jh003256':
        #         break
        for book in books:
            logger.info(f"Processing {book['id']} {book['name']}")
            try:
                volumes = book["volumes"][:]  # copy it to avoid re-order it
                volumes.sort(key=lambda e: e["index_in_book"])
                file_paths = [volume["file_path"] for volume in volumes]
                assert len(file_paths) == len(
                    set(file_paths)
                ), f"Duplicate file paths in {book['id']} {book['name']}"
                first_file_path = min(volumes, key=lambda v: v["file_path"])[
                    "file_path"
                ]  # volumes[0]['file_path']
                nth = 1
                for nth, volume in zip(count(2), volumes[1:]):
                    file_path = volume["file_path"]
                    new_file_path = re.sub(
                        r"(?<=[^\d])001(?=[^\d])", str(nth).zfill(3), first_file_path
                    )
                    assert (
                        new_file_path != first_file_path
                    ), f"Invalid first file_path: {', '.join(file_paths)}"
                    assert (
                        new_file_path == file_path
                    ), f"Irregular file paths in {book['id']} {book['name']} ({nth} / {len(volumes)}), expected: {new_file_path}, actual: {file_path}"
                    # logger.info(f"Set {book['id']} {book['name']} {nth}/{len(volumes)} file path: {new_file_path}")
                    # cnt += 1
                    # volume["file_path"] = new_file_path
                missing = False
                for nth in count(len(volumes) + 1):
                    next_file_path = re.sub(
                        r"(?<=[^\d])001(?=[^\d])", str(nth).zfill(3), first_file_path
                    )
                    assert (
                        next_file_path != first_file_path
                    ), f"Invalid first file_path: {', '.join(file_paths)}"
                    next_url = FILE_URL.format(server=server, file_path=next_file_path)
                    if (_resp := retry(3)(requests.get)(next_url)).ok:
                        logger.info(
                            f"New file found in {book['id']} {book['name']} ({nth} / {len(volumes)}): {next_file_path}, volume id {vacant_volume_id} used"
                        )
                        book["volumes"].append({
                            "id": str(vacant_volume_id),
                            "name": None,
                            "file_path": next_url,
                            "toc": [],
                            "index_in_book": nth - 1,
                        })
                        vacant_volume_id += 1
                        vcnt += 1
                        missing = True
                    else:
                        break
                if missing:
                    bcnt += 1
                    problematics.append(book)
            except Exception as e:
                traceback.print_exc()
        # if inplace:
        logger.info(f"{vcnt} volumes {bcnt} books fixed in {f}")
        with open(f, "w") as file:
            json.dump(books, file, ensure_ascii=False, indent=2)
        with open(f.replace(".json", ".fixedmissing.json"), "w") as file:
            json.dump(problematics, file, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    main()
