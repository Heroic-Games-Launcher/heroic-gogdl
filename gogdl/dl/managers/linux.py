# Manage downloading of linux native games using new zip method based on Range headers
import json
import logging
import hashlib
import os.path
import stat
from concurrent.futures import ThreadPoolExecutor, as_completed
from zlib import crc32
from gogdl.dl import dl_utils
from gogdl.dl.managers.task_executor import ExecutingManager
from gogdl.dl.objects.generic import BaseDiff
from gogdl.dl.objects.v2 import DepotLink
from gogdl.dl.workers import linux as linux_worker
from gogdl.dl.objects import linux
from gogdl.languages import Language
from gogdl import constants
from gogdl.dl.objects.generic import FileExclusion


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
        self.is_verifying = generic_manager.is_verifying

        self.api_handler = generic_manager.api_handler
        self.allowed_threads = generic_manager.allowed_threads
        self.folder_name = get_folder_name_from_windows_manifest(self.api_handler, self.game_id)

        if "path" in self.arguments:
            self.path = self.arguments.path
            if generic_manager.should_append_folder_name:
                self.path = os.path.join(self.path, self.folder_name)
        else:
            self.path = ""

        self.lang = Language.parse(self.arguments.lang)
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

        self.languages_codes = [Language.parse(installer["language"]) for installer in game_installers]

        self.game_installer = self.find_matching_installer(game_installers)

        if not self.dlc_only:
            installer_data = dl_utils.get_json(self.api_handler, self.game_installer["files"][0]["downlink"])
            game_install_handler = linux.InstallerHandler(installer_data["downlink"],self.game_id,self.api_handler.session)
            self.installer_handlers.append(game_install_handler)

        # Create dlc installer handlers
        if self.dlcs_should_be_downloaded:
            for dlc in self.game_data["expanded_dlcs"]:
                if self.dlcs_should_be_downloaded and self.api_handler.does_user_own(dlc["id"]):
                    if self.dlcs_list and str(dlc["id"]) not in self.dlcs_list:
                        continue

                    linux_installers = self.filter_linux_installers(dlc["downloads"]["installers"])
                    installer = self.find_matching_installer(linux_installers)

                    if installer is None:
                        self.logger.error(
                            dlc["title"] + " - Does not have a linux installer"
                        )
                        continue

                    installer_data = dl_utils.get_json(self.api_handler, installer["files"][0]["downlink"])

                    install_handler = linux.InstallerHandler(installer_data["downlink"],
                                                             str(dlc["id"]),
                                                             self.api_handler.session)

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
                if not file.file_name.startswith("data/noarch") or file.file_name.endswith("/"):
                    continue
                size += file.uncompressed_size
                download_size += file.compressed_size
        return download_size, size

    def get_owned_dlcs(self):
        dlcs = list()
        for dlc in self.game_data["expanded_dlcs"]:
            if self.api_handler.does_user_own(dlc["id"]):
                if not dlc["downloads"]["installers"]:
                    continue
                dlc_languages = [installer["language"] for installer in
                                 self.filter_linux_installers(dlc["downloads"]["installers"])]
                dlcs.append({"title": dlc["title"], "id": dlc["id"], "languages": [dlc_languages]})
        return dlcs

    def get_download_size(self):
        self.setup()

        dlcs = self.get_owned_dlcs()

        download_size, disk_size = self.calculate_download_sizes()

        response = {
            "download_size": download_size,
            "disk_size": disk_size,
            "dlcs": dlcs,
            "languages": [lang.code for lang in self.languages_codes],
            "folder_name": self.folder_name,
            "dependencies": [],
            "versionName": self.game_installer["version"],
        }

        return response

    def download(self):
        self.setup()
        manifest_path = os.path.join(self.path, '.gogdl-linux-manifest')

        cd_files = dict()
        for handler in self.installer_handlers:
            for file in handler.central_directory.files:
                if not file.file_name.startswith("data/noarch") or file.file_name.endswith("/"):
                    continue
                cd_files.update({file.file_name: file})

        manifest_data = None
        if os.path.exists(manifest_path):
            with open(manifest_path, 'r') as f:
                manifest_data = json.load(f)

        new: list[linux.CentralDirectoryFile] = list()
        deleted: list[str] = list()
        if manifest_data and not self.is_verifying:
            manifest_files = dict()
            for file in manifest_data['files']:
                manifest_files.update({file['file_name']: file['crc32']})

            for file_name in manifest_files:
                if file_name in cd_files:
                    if cd_files[file_name].crc32 != manifest_files[file_name]:
                        new.append(cd_files[file_name])
                else:
                    deleted.append(file_name)
            
            for file_name in cd_files:
                if file_name not in manifest_files:
                    new.append(cd_files[file_name])

        else:
            new = list(cd_files.values())

        sources = dict()
        for handler in self.installer_handlers:
            sources.update({handler.product: handler.url})

        print("New/changed files", len(new))
        print("Deleted", len(deleted))
        print("Total files", len(cd_files))

        if self.is_verifying:
            self.logger.info("Verifying files")
            invalid = list()
            for file in new:
                path = file.file_name.replace("data/noarch", self.path)

                if not os.path.exists(path):
                    invalid.append(file)
                else:
                    if file.is_symlink():
                        continue
                    with open(path, 'rb') as fh:
                        sum = 0
                        while data := fh.read(1024*1024):
                            sum = crc32(data, sum)

                        if sum != file.crc32:
                            invalid.append(file)
            if not len(invalid):
                self.logger.info("All files look good")
                return
            new = invalid


        diff = BaseDiff()

        with open(os.path.join(constants.CONFIG_DIR, "exclude", self.game_id), "r") as f:
            exclude_list = [line.strip().lower() for line in f if line.strip()]

        final_files = list()
        for i, file in enumerate(new):
            # Prepare file for download
            # Calculate data offsets
            handler = None
            for ins in self.installer_handlers:
                if ins.product == file.product:
                    handler = ins
                    break
                
            if not handler:
                print("Orphan file found")
                continue

            if FileExclusion.matches(file.file_name.lower().replace("data/noarch/", ""), exclude_list):
                continue

            data_start = handler.start_of_archive_index + file.file_data_offset
            c_size = file.compressed_size
            size = file.uncompressed_size
            method = file.compression_method
            checksum = file.crc32

            path = file.file_name.replace("data/noarch", self.path)
            if file.is_symlink():
                data = handler.get_bytes_from_file(from_b=data_start, size=c_size, add_archive_index=False)
                diff.links.append(DepotLink({"path": path, "target": os.path.normpath(os.path.join(dl_utils.parent_dir(path), data.decode()))}))
                continue
            file_permissions = int(bin(int.from_bytes(file.ext_file_attrs, "little"))[3:][:9])
            executable = (file_permissions & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)) != 0
            final_files.append(linux.LinuxFile(file.product, path, method, data_start, c_size, size, checksum, executable))

        diff.new = final_files

        manager = ExecutingManager(self.api_handler, self.allowed_threads, self.path, None, diff, sources)  
        
        manager.setup()
        for file in deleted:
            path = file.replace('data/noarch', self.path)
            if os.path.exists(path):
                os.remove(path)
        cancelled = manager.run()

        if cancelled:
            return

        new_manifest = dict()

        gameinfo_file = os.path.join(self.path, 'gameinfo')
        if os.path.exists(gameinfo_file):
            checksum = hashlib.md5()
            with open(gameinfo_file, 'rb') as f:
                checksum.update(f.read())
            new_manifest['info_checksum'] = checksum.hexdigest()

        new_manifest['files'] = [f.as_dict() for f in cd_files.values()]

        
        os.makedirs(self.path, exist_ok=True)
        with open(manifest_path, 'w') as f:
            manifest_data = json.dump(new_manifest,f)
