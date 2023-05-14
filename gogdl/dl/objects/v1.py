import json
import os
from gogdl.dl import dl_utils
from gogdl import constants

class Depot:
    def __init__(self, target_lang, depot_data):
        self.target_lang = target_lang
        self.languages = depot_data["languages"]
        self.game_ids = depot_data["gameIDs"]
        self.size = int(depot_data["size"])
        self.manifest = depot_data["manifest"]

    def check_language(self):
        status = True
        for lang in self.languages:
            status = lang == "Neutral" or lang == self.target_lang
            if status:
                break
        return status

class Directory:
    def __init__(self, item_data):
        self.path = item_data["path"].replace(constants.NON_NATIVE_SEP, os.sep).rstrip(os.sep)

class Dependency:
    def __init__(self, data):
        self.id = data["redist"]
        self.size = data.get("size")
        self.target_dir = data["targetDir"]


class File:
    def __init__(self, data):
        self.offset = data["offset"]
        self.hash = data["hash"]
        self.url = data["url"]
        self.path = data["path"].lstrip("/")
        self.size = data["size"]
        self.support = data.get("support")

class Manifest:
    def __init__(self, meta, language, dlcs, api_handler, dlc_only):
        self.data = meta
        self.data["HGLInstallLanguage"] = language
        self.data["HGLdlcs"] = dlcs
        self.product_id = meta["product"]["rootGameID"]
        self.dlcs = dlcs
        self.dlc_only = dlcs
        self.all_depots = [] 
        self.depots = self.parse_depots(language, meta["product"]["depots"])
        self.dependencies_ids = [depot['redist'] for depot in meta["product"]["depots"] if depot.get('redist')]

        self.api_handler = api_handler

        self.files = []
        self.dirs = []

    @classmethod
    def from_json(cls, meta, api_handler):
        manifest = cls(meta, meta['HGLInstallLanguage'], meta["HGLdlcs"], api_handler, False)
        return manifest
    
    def serialize_to_json(self):
        return json.dumps(self.data)

    def parse_depots(self, language, depots):
        parsed = []
        dlc_ids = [dlc["id"] for dlc in self.dlcs]
        for depot in depots:
            if depot.get("redist"):
                continue
            
            for g_id in depot["gameIDs"]:
                if g_id in dlc_ids or (not self.dlc_only and self.product_id == g_id):
                    new_depot = Depot(language, depot)
                    parsed.append(new_depot)
                    self.all_depots.append(new_depot)
                    break
        return list(filter(lambda x: x.check_language(), parsed))

    def list_languages(self):
        languages_dict = set()
        for depot in self.all_depots:
            for language in depot.languages:
                if language != "Neutral":
                    languages_dict.add(language)

        return list(languages_dict)

    def calculate_download_size(self):
        download_size = 0

        for depot in self.depots:
            download_size += depot.size
        
        return download_size, download_size

    
    def get_files(self):
        for depot in self.depots:
            manifest = dl_utils.get_json(self.api_handler, f"{constants.GOG_CDN}/content-system/v1/manifests/{self.product_id}/{self.platform}/{self.data['product']['timestamp']}/{depot['manifest']}")
            for record in manifest["depot"]["files"]:
                if "directory" in record:
                    self.dirs.append(Directory(record)) 
                else:
                    self.files.append(File(record))

    