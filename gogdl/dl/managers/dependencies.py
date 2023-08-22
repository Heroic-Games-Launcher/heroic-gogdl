import logging
from multiprocessing import Pool

from gogdl.dl import dl_utils
import gogdl.constants as constants
from gogdl.dl.managers.task_executor import ExecutingManager
from gogdl.dl.objects import v2
from gogdl.dl.objects.generic import BaseDiff
from gogdl.dl.workers import task_executor


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


# Looks like we can use V2 dependencies for V1 games too WOAH
# We are doing that obviously 
class DependenciesManager:
    def __init__(
        self, ids, path, workers_count, api_handler, print_manifest=False, download_game_deps_only=False
    ):
        self.api = api_handler

        self.logger = logging.getLogger("REDIST")

        self.path = path
        self.workers_count = int(workers_count)
        self.repository = self.api.get_dependencies_list()

        self.ids = ids
        self.download_game_deps_only = download_game_deps_only  # Basically skip all redist with path starting with __redist
        if self.repository and print_manifest:
            print(self.repository)

    def get_files_for_depot_manifest(self, manifest):
        url = f'{constants.GOG_CDN}/content-system/v2/dependencies/meta/{dl_utils.galaxy_path(manifest)}'
        manifest = dl_utils.get_zlib_encoded(self.api, url)[0]

        return get_depot_list(manifest, 'redist')


    def get(self, return_files=False):
        depots = []
        if not self.ids:
            return []

        for depot in self.repository[0]["depots"]:
            if depot["dependencyId"] in self.ids:
                # By default we want to download all redist beginning with redist (game installation runs installation of the game's ones)
                should_download = depot["executable"]["path"].startswith("__redist")
                
                # If we want to download redist located in game dir we flip the boolean
                if self.download_game_deps_only:
                    should_download = not should_download

                if should_download:
                    depots.append(depot)

        files = []

        # Collect files for each redistributable
        for depot in depots:
            files += self.get_files_for_depot_manifest(depot["manifest"])

        if return_files:
            return files

        secure_link = dl_utils.get_dependency_link(self.api) # This should never expire

        diff = DependenciesDiff()
        diff.new = files

        executor = ExecutingManager(self.api, self.workers_count, self.path, diff, {'redist': secure_link})
        success = executor.setup()
        if not success:
            self.logger.error('Unable to proceed, not enough disk space')
            exit(2)
        executor.run()

class DependenciesDiff(BaseDiff):
    def __init__(self):
        super().__init__()
