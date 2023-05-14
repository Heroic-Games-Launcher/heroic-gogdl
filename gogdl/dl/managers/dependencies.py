import logging
from multiprocessing import Pool

from gogdl.dl import dl_utils
import gogdl.constants as constants
from gogdl.dl.objects import v2
from gogdl.dl.workers import v2 as v2_worker


def get_depot_list(manifest, product_id=None):
    download_list = list()
    for item in manifest["depot"]["items"]:
        obj = None
        if item["type"] == "DepotFile":
            obj = v2.DepotFile(item, product_id)
        else:
            obj = v2.DepotDirectory(item)
        download_list.append(obj)
    return download_list


def run_process(worker):
    worker.work()


# Looks like we can use V2 dependencies for V1 games too WOAH
# We are doing that obviously 
class DependenciesManager:
    def __init__(
        self, ids, path, workers_count, api_handler, download_game_deps_only=False
    ):
        self.api = api_handler

        self.logger = logging.getLogger("REDIST")

        self.path = path
        self.workers_count = int(workers_count)
        self.repository = self.api.get_dependencies_list()

        self.ids = ids
        self.download_game_deps_only = download_game_deps_only  # Basically skip all redist with path starting with __redist

    def get(self, return_workers=False):
        depots = []
        if not self.ids:
            return []

        for depot in self.repository[0]["depots"]:
            if depot["dependencyId"] in self.ids:
                # By default we want to download all redist beginning with redist (game installation runs installation of the game's ones)
                # True if it's global redist
                # False if it's scoped to game dir
                should_download = depot["executable"]["path"].startswith("__redist")
                
                # If we want to download redist located in game dir we flip the boolean
                # False if it's global redist
                # True if it's scoped to game dir
                if self.download_game_deps_only:
                    should_download = not should_download

                if should_download:
                    depots.append(depot)

        files = []

        # Collect files for each redistributable
        for depot in depots:
            url = f'{constants.GOG_CDN}/content-system/v2/dependencies/meta/{dl_utils.galaxy_path(depot["manifest"])}'
            manifest = dl_utils.get_zlib_encoded(self.api, url)[0]

            files += get_depot_list(manifest)

        secure_link = dl_utils.get_dependency_link(self.api)

        workers = list()

        for file in files:
            worker = v2_worker.DLWorker(file, self.path, self.api, None, secure_link)
            worker.is_dependency = True
            workers.append(worker)

        if return_workers:
            return workers

        pool = Pool(self.workers_count)

        pool.map(run_process, workers)
