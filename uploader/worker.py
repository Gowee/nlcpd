#!/usr/bin/env python3
import os.path
import itertools
import subprocess
import json
import logging
from io import BytesIO

import yaml
import mwclient

CONFIG_FILE_PATH = "./config.yml"
POSITION_FILE_PATH = "./.position"

# RESP_DUMP_PATH = "/tmp/wmc_upload_resp_dump.html"

logging.basicConfig(level=logging.INFO)


def call(command, *args, **kwargs):
    kwargs["shell"] = True
    return subprocess.check_call(command, *args, **kwargs)


# def load_or_ask_for_credentials():
#     d = None
#     if os.path.exists(CREDENTIALS_FILE_PATH):
#         with open(CREDENTIALS_FILE_PATH, "r") as f:
#             config = yaml.load(f)
#         username = config["username"]
#         password = config["password"]
#     else:
#         username = input("Username: ")
#         password = input("Password: ")
#         with open(CREDENTIALS_FILE_PATH, "w") as f:
#             f.write(yaml.dump({username, password}))
#     return username, password


def load_position():
    if os.path.exists(POSITION_FILE_PATH):
        with open(POSITION_FILE_PATH, "r") as f:
            return f.read().strip()
    else:
        return None


def store_position(position):
    with open(POSITION_FILE_PATH, "w") as f:
        f.write(position)


def main():
    import sys, os

    sys.path.append(os.path.join(os.getcwd(), "data"))

    with open(CONFIG_FILE_PATH, "r") as f:
        config = yaml.load(f.read())
    username, password = config["username"], config["password"]
    site = mwclient.Site("commons.wikimedia.org")
    site.login(username, password)
    site.requests["timeout"] = 125
    site.chunk_size = 1024 * 1024 * 20
    # print(f"data.{config['batch']}")
    # print(dir(__import__(f"data.{config['batch']}")))
    tasks = __import__(config["batch"]).tasks()
    last_position = load_position()

    if last_position is not None:
        print(f"Last processed: {last_position}")
        next(
            itertools.dropwhile(lambda task: task["name"] != last_position, tasks)
        )  # lazy!
        # TODO: peek and report?

    for task in tasks:
        page_name = task["name"]
        if "file" in task:
            page_name = "File:" + page_name
        page = site.pages[page_name]
        rewriting = False
        if page.exists:  # or ('file' in task and page.imageinfo != {}):
            if (
                input(
                    f"{page_name} is already existing. Skip or Rewrite wikitext?[S/r]"
                )
                == "r"
            ):
                rewriting = True
            else:
                store_position(task["name"])
                continue
        if not rewriting and (file := task.get("file")):
            print(f"Uploading file {task['name']}")
            if callable(file):
                file = task.get("file")()
            if type(file) == str and (
                file.startswith("https:") or file.startswith("http:")
            ):
                site.upload(
                    file=None,
                    filename=task["name"],
                    description=task["text"],
                    comment=task["comment"],
                    url=file,
                )
            else:
                if type(file) == bytes:
                    print("File size: ", len(file))
                    file = BytesIO(file)
                if hasattr(file, "read"):
                    site.upload(
                        file,
                        filename=task["name"],
                        description=task["text"],
                        comment=task["comment"],
                    )
                else:
                    assert False
        else:
            print(f'{"Updating" if rewriting else "Creating"} page {task["name"]}')
            # page = site.pages[task['name']]
            assert not page.exists or rewriting
            page.edit(task["text"], task["comment"] + " (Rewriting)")
        store_position(task["name"])
        input("< Paused. Press to proceed. >")

    # for book in books:
    #     to_create_category = len(volumes) > 1 # TODO: refactor
    #     volumes = iter(volumes)
    #     if last_position is not None:
    #         next(
    #             itertools.dropwhile(lambda volume: volume[0] != last_position[1], volumes)
    #         ) # lazy!
    #         last_position = None
    #     if to_create_category:
    #         logger.info(f"Creating category Category:{title}")
    #         category = site.categories[title]
    #         if category.exists and len(category.member()) > 0:
    #             logger.warn(f"Category:{title} is existing and not empty")
    #             call(f"xdg-open \"https://commons.wikimedia.org/wiki/Category:{title}\"")
    #             input("< Paused. Press to proceed. >")
    #         else:
    #             category.edit
    #     for filename, wikitext, comment in volumes:
    #         site.upload(payload, filename, description=wikitext, comment=comment)
    #         assert page[filename].exists
    #         store_position(title, filename)
    #         logger.info()


if __name__ == "__main__":
    main()
