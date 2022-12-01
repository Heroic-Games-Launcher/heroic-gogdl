class DepotFile:
    def __init__(self, item_data, product_id):
        self.path = item_data['path'].replace('\\', '/')
        self.chunks = item_data['chunks']
        self.flags = item_data.get('flags')
        self.md5 = item_data.get('md5')
        self.sha256 = item_data.get('sha256')
        self.product_id = product_id


# That exists in some depots, indicates directory to be created, it has only path in it
# Yes that's the thing
class DepotDirectory:
    def __init__(self, item_data):
        self.path = item_data['path']


class Depot:
    def __init__(self, target_lang, depot_data):
        self.target_lang = target_lang
        self.languages = depot_data['languages']
        self.bitness = depot_data.get('osBitness')
        self.product_id = depot_data['productId']
        self.compressed_size = depot_data.get('compressedSize')
        self.size = depot_data['size']
        self.manifest = depot_data['manifest']

    def check_language(self):
        status = False
        for lang in self.languages:
            status = lang == '*' or self.target_lang.lower() == lang.lower() or self.target_lang.split('-')[
                0].lower() == lang.lower()
            if status:
                break
        return status


class DepotV1:
    def __init__(self, target_lang, depot_data):
        self.target_lang = target_lang
        self.languages = depot_data['languages']
        self.game_ids = depot_data['gameIDs']
        self.size = depot_data['size']
        self.manifest = depot_data['manifest']

    def check_language(self):
        status = True
        for lang in self.languages:
            status = lang == "Neutral" or lang == self.target_lang
        return status


class DependencyV1:
    def __init__(self, data):
        self.id = data['redist']
        self.size = data.get('size')
        self.target_dir = data['targetDir']
