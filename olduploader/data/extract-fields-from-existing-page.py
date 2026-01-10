#!/usr/bin/env python3
# VIBED
import os
import csv
import re
import logging
from time import sleep
from mwclient import Site

API_TOKEN = os.getenv("MW_API_TOKEN")
SLEEP = 0

CATEGORY = "National_Library_of_China-Yunnan_Provincial_Library_Ancient_Books"
TEMPLATE_NAME = "National_Library_of_China-Yunnan_Provincial_Library_Ancient_Books"
OUTPUT_CSV = "nlc_ypl_existing_ancient_books.csv"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Match the whole template block (non-greedy, multiline)
REGEX_TEMPLATE = re.compile(
    r"\{\{\s*" + re.escape(TEMPLATE_NAME) + r"\s*(?P<body>.*?)\n\}\}",
    re.DOTALL | re.IGNORECASE,
)

# Match |key=value pairs (value may span multiple lines)
REGEX_PARAM = re.compile(
    r"  \|(?P<key>.+?)=(?P<value>.*?)(?=(\n  \||\Z))",
    # r"\|\s*(?P<key>[^=\|\n]+)\s*=\s*(?P<value>.*?)(?=\n\||\n\}\}|$)",
    re.DOTALL,
)


def connect_site():
    headers = {"Authorization": f"Bearer {API_TOKEN}"} if API_TOKEN else {}
    return Site(
        "commons.wikimedia.org",
        clients_useragent="nlc-yunnan-csv/0.1 (+PencakeBot)",
        custom_headers=headers,
    )


def extract_template_fields(text):
    """
    Return dict of template parameters, or None if template not found.
    """
    assert "加工記錄標識號" in text, "Input text does not contain expected marker"
    m = REGEX_TEMPLATE.search(text)
    if not m:
        return None

    body = m.group("body")
    fields = {}

    for pm in REGEX_PARAM.finditer(body):
        key = pm.group("key").strip()
        value = pm.group("value").strip()
        # normalize whitespace
        value = re.sub(r"\s+\n", "\n", value)
        fields[key] = value

    return fields


def iter_file_pages(site):
    cat = site.categories[CATEGORY]
    for page in cat.members(namespace=6):  # File namespace
        yield page


def main():
    site = connect_site()

    rows = []
    all_keys = set()

    for idx, page in enumerate(iter_file_pages(site), start=1):
        logger.info(f"Processing {idx}: {page.name}")
        try:
            sleep(SLEEP)
            text = page.text()
            fields = extract_template_fields(text)

            if not fields:
                logger.warning(f"Template not found on {page.name}")
                continue

            # Add some implicit metadata
            fields["_page"] = page.name
            fields["_title"] = page.page_title

            rows.append(fields)
            all_keys.update(fields.keys())
            logger.info(f"Extracted: {fields.items()}")

        except Exception:
            logger.exception(f"Failed to process {page.name}")

    if not rows:
        logger.warning("No data collected.")
        return

    # Stable column order: metadata first, then sorted template keys
    meta_keys = ["_page", "_title"]
    template_keys = sorted(k for k in all_keys if k not in meta_keys)
    fieldnames = meta_keys + template_keys

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    logger.info(f"Wrote {len(rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
