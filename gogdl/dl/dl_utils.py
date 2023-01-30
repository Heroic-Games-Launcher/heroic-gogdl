import json
import zlib
import os
import gogdl.constants as constants
import shutil
from sys import exit

PATH_SEPARATOR = os.sep


def get_json(api_handler, url):
    x = api_handler.session.get(url)
    if not x.ok:
        return
    return x.json()


def get_zlib_encoded(api_handler, url):
    x = api_handler.session.get(url)
    if not x.ok:
        return
    try:
        decompressed = json.loads(zlib.decompress(x.content, 15))
    except zlib.error:
        return json.loads(x.content), x.headers
    return decompressed, x.headers


def prepare_location(path, logger=None):
    os.makedirs(path, exist_ok=True)
    if logger:
        logger.debug(f"Created directory {path}")


# V1 Compatible
def galaxy_path(manifest: str):
    galaxy_path = manifest
    if galaxy_path.find("/") == -1:
        galaxy_path = manifest[0:2] + "/" + manifest[2:4] + "/" + galaxy_path
    return galaxy_path


def get_secure_link(api_handler, path, gameId, generation=2):
    url = ""
    if generation == 2:
        url = f"{constants.GOG_CONTENT_SYSTEM}/products/{gameId}/secure_link?_version=2&generation=2&path={path}"
    elif generation == 1:
        url = f"{constants.GOG_CONTENT_SYSTEM}/products/{gameId}/secure_link?_version=2&type=depot&path={path}"
    r = api_handler.session.get(url)
    if not r.ok:
        print(f"ERROR getting secure link for {path}")
        exit(1)
    js = r.json()

    endpoint = classify_cdns(js["urls"], generation)
    url_format = endpoint["url_format"]
    parameters = endpoint["parameters"]
    if generation == 1:
        if parameters.get("path"):
            parameters["path"] = parameters["path"] + "/main.bin"

        return merge_url_with_params(url_format, parameters)

    return endpoint


def get_dependency_link(api_handler, path):
    data = get_json(
        api_handler,
        f"{constants.GOG_CONTENT_SYSTEM}/open_link?generation=2&_version=2&path=/dependencies/store/"
        + path,
    )
    endpoint = classify_cdns(data["urls"])
    url = endpoint["url"]
    return url


def merge_url_with_params(url, parameters):
    for key in parameters.keys():
        url = url.replace("{" + key + "}", str(parameters[key]))
        if not url:
            print(f"Error ocurred getting a secure link: {url}")
    return url


def parent_dir(path: str):
    return path[0: path.rindex(PATH_SEPARATOR)]


def classify_cdns(cdns, generation=2):
    best = None
    for cdn in cdns:
        if generation not in cdn["supports_generation"]:
            continue
        if not best:
            best = cdn
        else:
            if best["priority"] < cdn["priority"]:
                best = cdn

    return best


def calculate_sum(path, function, read_speed_function=None):
    with open(path, "rb") as f:
        calculate = function()
        while True:
            chunk = f.read(16 * 1024)
            if not chunk:
                break
            if read_speed_function:
                read_speed_function(len(chunk))
            calculate.update(chunk)

        return calculate.hexdigest()


def get_readable_size(size):
    power = 2 ** 10
    n = 0
    power_labels = {0: "", 1: "K", 2: "M", 3: "G"}
    while size > power:
        size /= power
        n += 1
    return size, power_labels[n] + "B"


def check_free_space(size, path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    _, _, available_space = shutil.disk_usage(path)

    return size < available_space


def get_range_header(offset, size):
    from_value = offset
    to_value = (int(offset) + int(size)) - 1
    return f"bytes={from_value}-{to_value}"
