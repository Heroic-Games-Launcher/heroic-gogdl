# Manage downloading of linux native games using new zip method based on Range headers
import logging
import sys
import os.path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from gogdl.dl import dl_utils
from gogdl.dl.workers import linux as linux_worker
from gogdl.dl.objects import linux
from gogdl import constants
import signal


def get_folder_name_from_windows_manifest(api_handler, id):
    builds = dl_utils.get_json(
        api_handler,
        f"{constants.GOG_CONTENT_SYSTEM}/products/{id}/os/windows/builds?generation=2",
    )

    url = builds["items"][0]["link"]
    meta, headers = dl_utils.get_zlib_encoded(api_handler, url)
    install_dir = (
        meta["installDirectory"]
        if builds["items"][0]["generation"] == 2
        else meta["product"]["installDirectory"]
    )
    return install_dir


class Manager:
    def __init__(self, generic_manager):
        self.game_id = generic_manager.game_id
        self.arguments = generic_manager.arguments
        self.unknown_arguments = generic_manager.unknown_arguments

        self.api_handler = generic_manager.api_handler
        self.allowed_threads = generic_manager.allowed_threads
        self.folder_name = get_folder_name_from_windows_manifest(self.api_handler, self.game_id)

        if "path" in self.arguments:
            self.path = self.arguments.path
            if generic_manager.should_append_folder_name:
                self.path = os.path.join(self.path, self.folder_name)
        else:
            self.path = ""

        self.lang = self.arguments.lang
        self.dlcs_should_be_downloaded = self.arguments.dlcs
        if self.arguments.dlcs_list:
            self.dlcs_list = self.arguments.dlcs_list.split(",")
        else:
            self.dlcs_list = []
        self.dlc_only = self.arguments.dlc_only

        self.logger = logging.getLogger("LINUX")
        self.logger.info("Initialized Linux Download Manager")

        self.game_data = None

        self.languages_codes = list()
        self.downlink = None
        self.game_files = list()

        self.installer_handlers = list()

    @staticmethod
    def filter_linux_installers(installers):
        return [installer for installer in installers if installer["os"] == "linux"]

    def find_matching_installer(self, installers):
        if len(installers) == 1:
            return installers[0]
        for installer in installers:
            if installer["language"] == self.lang:
                return installer

        # English installers should be multilanguage ready
        for installer in installers:
            if installer["language"] == "en":
                return installer

        return None

    def setup(self):
        self.game_data = self.api_handler.get_item_data(self.game_id, expanded=['downloads', 'expanded_dlcs'])

        # Filter linux installers
        game_installers = self.filter_linux_installers(self.game_data["downloads"]["installers"])

        self.languages_codes = [installer["language"] for installer in game_installers]

        self.game_installer = self.find_matching_installer(game_installers)

        if not self.dlc_only:
            installer_data = dl_utils.get_json(self.api_handler, self.game_installer["files"][0]["downlink"])
            game_install_handler = linux.InstallerHandler(installer_data["downlink"],
                                                          self.game_installer["files"][0]["size"],
                                                          self.api_handler.session)
            self.installer_handlers.append(game_install_handler)

        # Create dlc installer handlers
        if self.dlcs_should_be_downloaded:
            for dlc in self.game_data["expanded_dlcs"]:
                if self.dlcs_should_be_downloaded and self.api_handler.does_user_own(dlc["id"]):
                    if self.dlcs_list and str(dlc["id"]) not in self.dlcs_list:
                        continue

                    linux_installers = self.filter_linux_installers(dlc["downloads"]["installers"])
                    installer = self.find_matching_installer(linux_installers)
                    installer_data = dl_utils.get_json(self.api_handler, installer["files"][0]["downlink"])

                    install_handler = linux.InstallerHandler(installer_data["downlink"],
                                                             installer["files"][0]["size"], self.api_handler.session)

                    self.installer_handlers.append(install_handler)

        pool = ThreadPoolExecutor(self.allowed_threads)
        futures = []
        for handler in self.installer_handlers:
            futures.append(pool.submit(handler.setup))

        for future in as_completed(futures):
            if future.cancelled():
                break

    def calculate_download_sizes(self):
        download_size = 0
        size = 0

        for handler in self.installer_handlers:
            for file in handler.central_directory.files:
                if not file.file_name.startswith("data/noarch") and file.file_name.endswith("/"):
                    continue
                size += file.uncompressed_size
                download_size += file.compressed_size
        return download_size, size

    def get_owned_dlcs(self):
        dlcs = list()
        for dlc in self.game_data["expanded_dlcs"]:
            if self.api_handler.does_user_own(dlc["id"]):
                dlc_languages = [installer["language"] for installer in
                                 self.filter_linux_installers(dlc["downloads"]["installers"])]
                dlcs.append({"title": dlc["title"], "id": dlc["id"], "languages": [dlc_languages]})
        return dlcs

    def get_download_size(self):
        self.setup()

        dlcs = self.get_owned_dlcs()

        download_size, disk_size = self.calculate_download_sizes()

        # TODO: get the folder name
        response = {
            "download_size": download_size,
            "disk_size": disk_size,
            "dlcs": dlcs,
            "languages": self.languages_codes,
            "folder_name": self.folder_name,
            "dependencies": [],
            "versionName": self.game_installer["version"],
        }

        return response

    def download(self):
        self.setup()

        download_size, disk_size = self.calculate_download_sizes()

        if not dl_utils.check_free_space(disk_size, self.path):
            raise Exception("Not enough space")

        processes = list()
        p = ProcessPoolExecutor(self.allowed_threads)
        for handler in self.installer_handlers:
            for file in handler.central_directory.files:
                if not file.file_name.startswith("data/noarch") or file.file_name.endswith("/"):
                    continue
                worker = linux_worker.DLWorker(file, self.path)
                processes.append(p.submit(worker.work, handler))

        def shut(sig, code):
            p.shutdown(wait=True, cancel_futures=True)
            sys.exit(-sig)

        signal.signal(signal.SIGINT, shut)
        signal.signal(signal.SIGTERM, shut)

        for process in as_completed(processes):
            if process.cancelled():
                break
