import json
from gogdl.dl import dl_utils
from gogdl import constants


class DepotFile:
    def __init__(self, item_data, product_id):
        self.path = item_data["path"].replace("\\", "/")
        self.chunks = item_data["chunks"]
        self.flags = item_data.get("flags")
        self.md5 = item_data.get("md5")
        self.sha256 = item_data.get("sha256")
        self.product_id = product_id


# That exists in some depots, indicates directory to be created, it has only path in it
# Yes that's the thing
class DepotDirectory:
    def __init__(self, item_data):
        self.path = item_data["path"]


class Depot:
    def __init__(self, target_lang, depot_data):
        self.target_lang = target_lang
        self.languages = depot_data["languages"]
        self.bitness = depot_data.get("osBitness")
        self.product_id = depot_data["productId"]
        self.compressed_size = depot_data.get("compressedSize")
        self.size = depot_data["size"]
        self.manifest = depot_data["manifest"]

    def check_language(self):
        status = False
        for lang in self.languages:
            status = (
                lang == "*"
                or self.target_lang == lang
                or self.target_lang.split("-")[0] == lang
            )
            if status:
                break
        return status


class Manifest:
    def __init__(self, meta, language, dlcs, api_handler, dlc_only):
        self.data = meta
        self.data["HGLInstallLanguage"] = language
        self.data["HGLdlcs"] = dlcs
        self.product_id = meta["baseProductId"]
        self.dlcs = dlcs
        self.dlc_only = dlc_only
        self.depots = self.parse_depots(language, meta["depots"])
        self.dependencies_ids = meta.get("dependencies")
        self.install_directory = meta["installDirectory"]

        self.api_handler = api_handler

        self.files = []
        self.dirs = []

    @classmethod
    def from_json(cls, meta, api_handler):
        manifest = cls(meta, meta["HGLInstallLanguage"], meta["HGLdlcs"], api_handler)
        return manifest

    def serialize_to_json(self):
        return json.dumps(self.data)

    def parse_depots(self, language, depots):
        parsed = []
        dlc_ids = [dlc["id"] for dlc in self.dlcs]
        for depot in depots:
            if depot["productId"] in dlc_ids or (
                not self.dlc_only and self.product_id == depot["productId"]
            ):
                parsed.append(Depot(language, depot))

        return filter(lambda x: x.check_language(), parsed)

    def list_languages(self):
        languages_dict = dict()
        for depot in self.depots:
            for language in depot.languages:
                if language != "*":
                    languages_dict[language] = True

        return list(languages_dict.keys())

    def calculate_download_size(self):
        download_size = 0
        disk_size = 0

        for depot in self.depots:
            download_size += depot.compressed_size
            disk_size += depot.size

        return download_size, disk_size

    def get_files(self):
        for depot in self.depots:
            manifest = dl_utils.get_zlib_encoded(
                self.api_handler,
                f"{constants.GOG_CDN}/content-system/v2/meta/{dl_utils.galaxy_path(depot.manifest)}",
            )[0]
            for item in manifest["depot"]["items"]:
                obj = None
                if item["type"] == "DepotFile":
                    self.files.append(DepotFile(item, depot.product_id))
                else:
                    self.dirs.append(DepotDirectory(item))


class FileDiff:
    def __init__(self):
        self.file = None

    @classmethod
    def compare(cls, new, old):
        diff = cls()

        old_offset = 0
        for new_chunk in new.chunks:
            for old_chunk in old.chunks:
                if old_chunk["md5"] == new_chunk["md5"]:
                    new_chunk["old_offset"] = old_offset
                old_offset += old_chunk["size"]
        diff.file = new
        return diff


class ManifestDiff:
    def __init__(self):
        self.deleted = []
        self.new = []
        self.changed = []

    @classmethod
    def compare(cls, manifest, old_manifest=None):
        comparison = cls()

        if not old_manifest:
            comparison.new = manifest.files
            return comparison

        new_files = dict()
        for file in manifest.files:
            new_files.update({file.path: file})

        old_files = dict()
        for file in old_manifest.files:
            old_files.update({file.path: file})

        for old_file in old_files.values():
            if not new_files.get(old_file.path):
                comparison.deleted.append(old_file)

        for new_file in new_files.values():
            old_file = old_files.get(new_file.path)
            if not old_file:
                comparison.new.append(new_file)
            else:
                if len(new_file.chunks) == 1:
                    if new_file.chunks[0]["md5"] != old_file.chunks[0]["md5"]:
                        comparison.changed.append(new_file)
                else:
                    if new_file.md5 != old_file.md5:
                        comparison.changed.append(FileDiff.compare(new_file, old_file))
        return comparison

    def __str__(self):
        return f"Deleted: {len(self.deleted)} New: {len(self.new)} Changed: {len(self.changed)}"
