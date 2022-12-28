import logging
from multiprocessing import Pool

from gogdl.dl import dl_utils
import gogdl.constants as constants
from gogdl.dl.objects import v2, v1
from gogdl.dl.workers import v2 as v2_worker


def get_depot_list(manifest, product_id=None):
    download_list = list()
    for item in manifest['depot']['items']:
        obj = None
        if item['type'] == 'DepotFile':
            obj = v2.DepotFile(item, product_id)
        else:
            obj = v2.DepotDirectory(item)
        download_list.append(obj)
    return download_list


def run_process(worker):
    worker.work()


class DependenciesManager:
    def __init__(self, arguments, unknown_arguments, api_handler):
        self.arguments = arguments
        self.unknown_arguments = unknown_arguments
        self.api = api_handler

        self.logger = logging.getLogger("REDIST")

        self.path = arguments.path
        self.version = int(arguments.version)
        self.workers_count = int(arguments.workers_count)
        self.repository = self.api.get_dependencies_list(depot_version=self.version)

        ids = arguments.ids
        self.ids = ids.split(",")

    def get(self):
        if self.version == 1:
            self.get_v1()
        elif self.version == 2:
            self.get_v2()

    def get_v1(self):
        pass

    def get_v2(self):
        depots = []

        for depot in self.repository[0]["depots"]:
            if depot["dependencyId"] in self.ids:
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

        pool = Pool(self.workers_count)

        pool.map(run_process, workers)
