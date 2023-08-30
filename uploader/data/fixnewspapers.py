#!/usr/bin/env python3
#!/usr/bin/env python3

import sys, json, re,os, logging,traceback
from typing import Sequence
from collections import defaultdict
from itertools import count
from concurrent.futures import ThreadPoolExecutor

import requests
from concurrent.futures import as_completed

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
            for volume in book["volumes"]:
                logger.info("%s - %s", book["name"], volume["name"])
                # assert len(set(volume["file_path"])) % 2 == 0, volume
                assert isinstance(volume["file_path"], Sequence)
                sorted_distinct_urls = sorted(set(volume["file_path"]))
                if (a := len(sorted_distinct_urls)) != (b := len(volume["file_path"])):
                    logger.warn("Repeated urls in %s - %s: %d > %d", book["name"], volume["name"], b, a)
                elif sorted_distinct_urls != volume["file_path"]:
                    logger.debug("Unordered urls in %s - %s", book["name"], volume["name"])
                if any(url.endswith("Thumbs.db") for url in sorted_distinct_urls):
                    logger.warn("Thumbs.db in %s - %s", book["name"], volume["name"])
                    sorted_distinct_urls = [url for url in sorted_distinct_urls if not url.endswith("Thumbs.db")]
                if any(not url.endswith(".jpg") for url in sorted_distinct_urls):
                    logger.warn("Non-jpg in %s - %s", book["name"], volume["name"])
                    sorted_distinct_urls = [url for url in sorted_distinct_urls if url.endswith(".jpg")]
                # print(sorted_distinct_urls)

                url_base = sorted_distinct_urls[0].rsplit("/", maxsplit=1)[0]
                if not all(url.startswith(url_base) for url in sorted_distinct_urls):
                    logger.warn("Non-base url in %s - %s: %r", book["name"], volume["name"], sorted_distinct_urls)
                    assert False
                url_stems = [url.rsplit("/", maxsplit=1)[-1].rsplit(".", maxsplit=1)[0] for url in sorted_distinct_urls]
                pages = []
                additional_pages = defaultdict(list)
                for stem in url_stems:
                    if stem[0].isdigit():
                        pages.append(stem)
                    else:
                        # _ = int(stem[1:])
                        assert stem[0] in "HTZF"
                        additional_pages[stem[0]].append(stem)
                if not list(map(int, pages)) == list(range(1, len(pages) + 1)):
                    logger.warn("Non-consecutive pages in %s - %s: %r, first: %s", book["name"], volume["name"], pages, sorted_distinct_urls[0])
                for kind, aps in additional_pages.items():
                    if not list(map(lambda p: int(p[1:]), aps)) == list(range(1, len(aps) + 1)):
                        logger.warn("Non-consecutive %s pages in %s - %s: %r", kind, book["name"], volume["name"], aps)
                with ThreadPoolExecutor(max_workers=32) as executor:
                    found = []
                    for resp in map(requests.head, (f"{url_base}/{str(i).zfill(3)}.jpg" for i in range(1, max(len(sorted_distinct_urls), 20) + 1))):
                        if resp.status_code == 200:
                            found.append(resp.url)
                        else:
                            if resp.status_code != 404:
                                logger.warn("Invalid status code {} in %s - %s: %r", book["name"], volume["name"], resp.status_code, resp.url)
                                # assert False
                    # found = [i for i in range(1, max(len(sorted_distinct_urls), 20) + 1) if requests.head(f"{url_base}/{str(i).zfill(3)}.jpg").status_code == 200]
                if len(found) > len(pages):
                    logger.warn("Retrieved unlisted pages in %s - %s: %r > %r", book["name"], volume["name"], found, pages)
                    sorted_distinct_urls.extend(set(found) - set(sorted_distinct_urls))
                    sorted_distinct_urls.sort()
                elif len(found) < len(pages):
                    logger.warn("Filtered invalid listed pages in %s - %s: %r < %r", book["name"], volume["name"], found, pages)
                volume["file_path"] = sorted_distinct_urls
                # if [int(url.rsplit("/", maxsplit=1)[-1].rsplit(".", maxsplit=1)[0]) for url in sorted_distinct_urls] != list(range(1, len(sorted_distinct_urls) + 1)):
                #     logger.warn("Non-consecutive urls in %s - %s: %r", book["name"], volume["name"], volume["file_path"])
                #     sorted_distinct_urls = [f"{base_url}/{str(i).zfill(3)}.jpg" for i in range(1, len(sorted_distinct_urls) + 1)]
                
                        
            # try:
            #     volumes = book["volumes"][:] # copy it to avoid re-order it
            #     volumes.sort(key=lambda e: e["index_in_book"])
            #     file_paths = [volume['file_path'] for volume in volumes]
            #     if len(file_paths) == len(set(file_paths)):
            #         continue
            #     assert len(set(file_paths)) == 1, f"Duplicate file paths in {book['id']} {book['name']} with more than 1 distinct file_paths: {', '.join(file_paths)}"
            #     first_file_path = volumes[0]['file_path']
            #     for nth, volume in zip(count(2), volumes[1:]):
            #         new_file_path = re.sub(r"(?<=[^\d])001(?=[^\d])", str(nth).zfill(3), first_file_path)
            #         logger.info(f"Set {book['id']} {book['name']} {nth}/{len(volumes)} file path: {new_file_path}")
            #         cnt += 1
            #         assert new_file_path != first_file_path, f"Invalid first file_path: {', '.join(file_paths)}"
            #         volume["file_path"] = new_file_path
            #     dups.append(book)
            # except Exception as e:
            #     traceback.print_exc()
        if inplace:
            with open(f, "w") as file:
                json.dump(books, file, ensure_ascii=False, indent=2)
            logger.info(f"{cnt} fixed in {f}")
    logger.info(f"{len(dups)} fixed in total")
    if not inplace:
        print(json.dumps(dups, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()


