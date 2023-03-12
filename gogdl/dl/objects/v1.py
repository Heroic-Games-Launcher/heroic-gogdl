class Depot:
    def __init__(self, target_lang, depot_data):
        self.target_lang = target_lang
        self.languages = depot_data["languages"]
        self.game_ids = depot_data["gameIDs"]
        self.size = depot_data["size"]
        self.manifest = depot_data["manifest"]

    def check_language(self):
        status = True
        for lang in self.languages:
            status = lang == "Neutral" or lang == self.target_lang
            if status:
                break
        return status


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
        self.path = data["path"]
        self.size = data["size"]
