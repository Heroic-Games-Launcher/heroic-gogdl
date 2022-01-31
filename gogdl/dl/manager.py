import os
import locale
import logging
import json
import requests
import threading
from time import sleep
from sys import platform
from multiprocessing import cpu_count
from gogdl.dl import dl_utils, objects, linux
from gogdl.dl.worker import DLWorker
from gogdl.dl.progressbar import ProgressBar
from concurrent.futures import ThreadPoolExecutor
import gogdl.constants as constants
from sys import exit

class DownloadManager():
    def __init__(self, api_handler):
        self.api_handler = api_handler
        self.logger = logging.getLogger('DOWNLOAD_MANAGER')
        self.logger.setLevel(logging.INFO)
        self.lang = locale.getdefaultlocale()[0].replace('_', '-')
        self.cancelled = False
        self.dlcs_should_be_downloaded = False
        self.threads = []
        self.platform = "windows" if platform == "win32" else "osx" if platform == "darwin" else "linux"

    def download(self, args, unknown_args):
        if self.get_download_metadata(args):
            if self.perform_download():
                exit(0)
            else:
                exit(2)
        else:
            exit(1)

    def calculate_download_size(self, args, unknown_args):
        if self.get_download_metadata(args):
            files = self.collect_depots()
            download_files = files[0]
            dependency_files = files[1]
            size_data = self.calculate_size(download_files, dependency_files)
            download_size = size_data[0]
            disk_size = size_data[1]

            dlcs = []
            if self.depot_version == 2:
                for product in self.meta['products']:
                    if product["productId"] != self.meta["baseProductId"]:
                        if self.api_handler.does_user_own("productId"):
                            dlcs.append({
                                "title": product['name'],
                                "app_name": product['productId']
                            })

            languages = []
            # Get possible languages
            depots_array = self.meta['depots'] if self.depot_version == 2 else self.meta['product']['depots']
            for depot in depots_array:
                if 'redist' in depot:
                    continue
                for lang in depot['languages']:
                    if (lang != "*" or lang != "Neutral") and not lang in languages:
                        languages.append(lang)

            print(json.dumps({"download_size": download_size, "disk_size": disk_size, "dlcs": dlcs, "version": self.builds['items'][0]["build_id"], "languages": languages, "folder_name": self.meta["installDirectory"] if self.depot_version == 2 else self.meta['product']['installDirectory']}))

    def get_download_metadata(self, args):

        if args.platform:
            self.platform = args.platform
        
        # Getting more and newer data
        self.dl_target = self.api_handler.get_item_data(args.id)
        self.dl_target['id'] = args.id

        if args.lang:
            self.lang = args.lang
        try:
            self.dlcs_should_be_downloaded = args.dlcs
        except AttributeError:
            pass
        is_compatible = self.check_compatibility()
        self.logger.info(f'Game is compatible') if is_compatible else self.logger.error(f'Game is incompatible')
        if not is_compatible:
            return False
        if self.platform == 'linux':
            linux.download(self.dl_target['id'], self.api_handler)
            return False
        self.logger.debug('Getting Build data')
        # Builds data
        self.builds = dl_utils.get_json(
            self.api_handler, f'{constants.GOG_CONTENT_SYSTEM}/products/{self.dl_target["id"]}/os/{self.platform}/builds?generation=2')
        # Just in case
        if self.builds['count'] == 0:
            self.logger.error('Nothing to download, exiting')
            return False

        target_build = self.builds['items'][0]
        if args.build:
            # Find build
            for build in self.builds['items']:
                if build['build_id'] == args.build:
                    target_build = build
                    break

        # Downloading most recent thing by default
        self.depot_version = target_build['generation']
        if self.depot_version == 1 or self.depot_version == 2:
            self.logger.info(f"Depot version: {self.depot_version}")
        else:
            self.logger.error("Unsupported depot version please report this")
            return False
        
        meta_url = target_build['link']
        self.logger.debug('Getting Meta data')
        self.meta = dl_utils.get_zlib_encoded(self.api_handler, meta_url)
        install_directory = self.meta['installDirectory'] if self.depot_version == 2 else self.meta['product']['installDirectory']
        try:
            self.path = args.path
            if args.command == 'download':
                self.dl_path = os.path.join(
                    self.path, install_directory)
            else:
                self.dl_path = self.path
        except AttributeError:
            pass
        
        # TODO: Handle Dependencies
        self.dependencies = self.handle_dependencies()

        return True



    def collect_depots(self):
        collected_depots = []
        download_files = []
        dependency_files = []

        owned_dlcs = []
        if self.depot_version == 2:
            if self.meta.get('products'):
                for dlc in self.meta['products']:
                    if dlc['productId'] != self.meta['baseProductId']:
                        if self.api_handler.does_user_own(dlc['productId']):
                            owned_dlcs.append(dlc['productId'])
            for depot in self.meta['depots']:
                if str(depot['productId']) == str(self.dl_target['id']) or (self.dlcs_should_be_downloaded and depot['productId'] in owned_dlcs):
                    # TODO: Respect user language
                    newObject = objects.Depot(self.lang, depot)
                    if newObject.check_language():
                        collected_depots.append(newObject)
        else:
            if self.meta['product'].get('gameIDs'):
                for dlc in self.meta['product']['gameIDs']:
                    if dlc['gameID'] != self.meta['product']['rootGameID']:
                        if self.api_handler.does_user_own(dlc['gameID']):
                            owned_dlcs.append(dlc['gameID'])
            for depot in self.meta['product']['depots']:
                if not 'redist' in depot:
                    depot_object = objects.DepotV1(self.lang, depot)
                    if depot_object.check_language():
                        collected_depots.append(depot_object)
                else:
                    dependency_object = objects.DependencyV1(depot)
                    dependency_files.append(dependency_object)

        
        self.logger.debug(
            f"Collected {len(collected_depots)} depots, proceeding to download, Dependencies Depots: {len(self.dependencies)}")
        if self.depot_version == 2:
            for depot in collected_depots:
                manifest = dl_utils.get_zlib_encoded(
                    self.api_handler, f'{constants.GOG_CDN}/content-system/v2/meta/{dl_utils.galaxy_path(depot.manifest)}')
                download_files += self.get_depot_list(manifest)
            for depot in self.dependencies:
                manifest = dl_utils.get_zlib_encoded(
                    self.api_handler, f'{constants.GOG_CDN}/content-system/v2/dependencies/meta/{dl_utils.galaxy_path(depot["manifest"])}')
                dependency_files += self.get_depot_list(manifest)
        return [download_files, dependency_files]
    # V2 downloading
    def perform_download(self):
        # print(self.meta)
        if self.depot_version == 1:
            return self.perform_download_V1()
        self.logger.debug("Collecting base game depots")

        files = self.collect_depots()

        download_files = files[0]
        dependency_files = files[1]

        self.logger.debug(
            f"Downloading {len(download_files)} game files, and {len(dependency_files)} dependency files proceeding")
        

        size_data = self.calculate_size(download_files, dependency_files)
        download_size = size_data[0]
        disk_size = size_data[1]

        readable_download_size = dl_utils.get_readable_size(download_size)
        readable_disk_size = dl_utils.get_readable_size(disk_size)
        self.logger.info(f"Download size: {round(readable_download_size[0], 2)}{readable_download_size[1]}")
        self.logger.info(f"Size on disk: {round(readable_disk_size[0], 2)}{readable_disk_size[1]}")
        self.logger.info("Checking free disk space")
        if not dl_utils.check_free_space(disk_size, self.path):
            self.logger.error("Not enough available disk space")
            return False
        allowed_threads = max(1, cpu_count())
        self.logger.debug("Spawning progress bar process")
        self.progress = ProgressBar(download_size, f"{round(readable_download_size[0], 2)}{readable_download_size[1]}", 50)
        self.progress.start()

        self.thpool = ThreadPoolExecutor(max_workers=allowed_threads)
        
        # Main game files
        for file in download_files:
            thread = DLWorker(file, self.dl_path, self.api_handler, self.dl_target['id'], self.progress.update_downloaded_size)
            self.threads.append(self.thpool.submit(thread.do_stuff))
        # Dependencies
        for file in dependency_files:
            thread = DLWorker(file, self.dl_path, self.api_handler, self.dl_target['id'], self.progress.update_downloaded_size)
            self.threads.append(self.thpool.submit(thread.do_stuff, (True)))

        # Wait until everything finishes
        while True:
            is_done = False
            for thread in self.threads:
                is_done = thread.done()
                if is_done == False:
                    break
            if is_done:
                break
            sleep(0.1)

        self.progress.completed = True
        return not self.cancelled

    def perform_download_V1(self):
        self.logger.debug("Redirecting download to V1 handler")
        self.logger.error("Currently V1 Depots are not supported yet.")
        exit(1)
        collected_depots = []
        download_files = []
        dependencies = []

        for depot in self.meta['product']['depots']:
            if not 'redist' in depot:
                depot_object = objects.DepotV1(self.lang, depot)
                if depot_object.check_language():
                    collected_depots.append(depot_object)
            else:
                dependency_object = objects.DependencyV1(depot)
                dependencies.append(dependency_object)

        self.logger.debug(f"Collected {len(collected_depots)} depots, proceeding to download, Dependencies Depots: {len(dependencies)}")
        self.logger.info("Getting data manifests of the depots")
        
        for depot in collected_depots:
            # url = f'{constants.GOG_CDN}/content-system/v2/meta/{depot.manifest}'
            url = f'{constants.GOG_CDN}/content-system/v1/manifests/{self.dl_target["id"]}/windows/{self.builds["items"][0]["legacy_build_id"]}/{depot.manifest}'
            manifest = dl_utils.get_json(self.api_handler, url)
            download_files += manifest['depot']['files']

        dl_utils.prepare_location(self.dl_path, self.logger)
        self.logger.info("Downloading main.bin file")
        if file_dl.get_file(f'{constants.GOG_CDN}/content-system/v1/depots/{self.dl_target["id"]}/main.bin', self.dl_path, self.api_handler, self.logger, False):
            self.unpack_v1(download_files)
        else:
            print("")
            self.logger.error("Error downloading a file")
        return False

    def handle_dependencies(self):
        dependencies_json = self.api_handler.get_dependenices_list()
        dependencies_array = []
        if self.depot_version == 2 and not 'dependencies' in self.meta:
            return []
        # TODO: Do more research on games with V1 depots
        iterator = self.meta['dependencies'] if self.depot_version == 2 else self.meta['product']['gameIDs'][0]['dependencies']

        for dependency in dependencies_json['depots']:
            for game_dep in iterator:
                if dependency['dependencyId'] == game_dep:
                    dependencies_array.append(dependency)
        return dependencies_array

    def get_depot_list(self, manifest):
        download_list = list()
        for item in manifest['depot']['items']:
            obj = None
            if item['type'] == 'DepotFile':
                obj = objects.DepotFile(item)
            else:
                obj = objects.DepotDirectory(item)
            download_list.append(obj)
        return download_list

    def check_compatibility(self):
        self.logger.info(f"Checking compatibility of {self.dl_target['title']} with {self.platform}")
        
        return self.dl_target['content_system_compatibility'][self.platform]

    def unpack_v1(self, download_files):
        self.logger.info("Unpacking main.bin (fs intense thing)")

    def calculate_size(self, files, dependencies):
        self.logger.info("Calculating download size")
        download_size = 0
        disk_size = 0
        for file in files:
            if type(file) == objects.DepotFile:
                for chunk in file.chunks:
                    download_size+=int(chunk.get('compressedSize'))
                    disk_size+=int(chunk['size'])

        for dependency in dependencies:
            if self.depot_version == 2:
                for chunk in dependency.chunks:
                    download_size+=int(chunk.get('compressedSize'))
                    disk_size+=int(chunk['size'])
            else:
                disk_size+=dependency.size

        if self.depot_version == 1:
            download_size = disk_size
        return (download_size, disk_size)