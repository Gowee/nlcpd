#!/usr/bin/env python3
#(VIBED)

import csv
import os
import sys
import mwclient
from mwclient.errors import APIError


SITE_HOST = "commons.wikimedia.org"   # e.g. "commons.wikimedia.org"


def die(msg):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def main():
    if len(sys.argv) != 2:
        die(f"Usage: {sys.argv[0]} pages.csv")

    csv_path = sys.argv[1]

    username = os.getenv("MW_USERNAME")
    password = os.getenv("MW_PASSWORD")

    if not username or not password:
        die("MW_USERNAME or MW_PASSWORD not set in environment")

    site = mwclient.Site(SITE_HOST)
    site.login(username, password)

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)

        for lineno, row in enumerate(reader, start=1):
            if len(row) < 2:
                print(f"[line {lineno}] SKIP: fewer than 2 columns")
                continue

            old_title = row[0].strip()
            new_title = row[1].strip()

            if not old_title or not new_title:
                print(f"[line {lineno}] SKIP: empty title")
                continue

            if old_title == new_title:
                print(f"[line {lineno}] SKIP: source and target titles are identical")
                continue

            old_page = site.pages[old_title]
            new_page = site.pages[new_title]

            if not old_page.exists:
                print(f"[line {lineno}] ERROR: source page does not exist: '{old_title}'")
                continue

            if new_page.exists:
                print(f"[line {lineno}] ERROR: target page already exists: '{new_title}'")
                continue

            try:
                old_page.move(
                    new_title,
                    reason="Apply consistent naming convention for books from NLC", #"Batch rename via mwclient",
                    move_talk=True,
                    no_redirect=False,
                )
                print(f"[line {lineno}] OK: '{old_title}' → '{new_title}'")

            except APIError as e:
                print(
                    f"[line {lineno}] API ERROR: '{old_title}' → '{new_title}': "
                    f"{e.code} – {e.info}"
                )

            except Exception as e:
                print(
                    f"[line {lineno}] UNEXPECTED ERROR: '{old_title}' → '{new_title}': {e}"
                )


if __name__ == "__main__":
    main()
