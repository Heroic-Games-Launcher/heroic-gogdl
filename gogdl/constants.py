import os
from sys import platform

GOG_CDN = "https://gog-cdn-fastly.gog.com"
GOG_CONTENT_SYSTEM = "https://content-system.gog.com"
GOG_EMBED = "https://embed.gog.com"
GOG_AUTH = "https://auth.gog.com"
GOG_API = "https://api.gog.com"
GOG_CLOUDSTORAGE = "https://cloudstorage.gog.com"
DEPENDENCIES_URL = "https://content-system.gog.com/dependencies/repository?generation=2"
DEPENDENCIES_V1_URL = "https://content-system.gog.com/redists/repository"

NON_NATIVE_SEP = "\\" if os.sep == "/" else "/"

if platform == 'linux':
    CONFIG_DIR = os.path.join(
        os.getenv("XDG_CONFIG_HOME", os.path.expanduser("~/.config")), 'heroic_gogdl'
    )
elif platform == 'win32':
    CONFIG_DIR = os.path.join(
        os.getenv("APPDATA"), 'heroic_gogdl'
    )
elif platform == 'darwin':
    CONFIG_DIR = os.path.join(
        os.path.expanduser("~/Library"), "Application Support", "heroic_gogdl"
    )

# Use only for Linux
CACHE_DIR = (
    os.path.join(
        os.getenv("XDG_CACHE_HOME", os.path.join(os.path.expanduser("~"), ".cache")),
        "heroicGOGdl",
    )
    if platform == "linux"
    else ""
)
