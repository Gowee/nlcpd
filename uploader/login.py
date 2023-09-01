import yaml
import mwclient

from upload import CONFIG_FILE_PATH


with open(CONFIG_FILE_PATH, "r") as f:
    config = yaml.safe_load(f.read())


def login(host="commons.wikimedia.org"):
    username, password = config["username"], config["password"]
    site = mwclient.Site(host)
    site.login(username, password)
    return site
