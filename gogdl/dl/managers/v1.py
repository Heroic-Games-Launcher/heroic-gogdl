# Handle old games downloading via V1 depot system
# V1 is there since GOG 1.0 days, it has no compression and relies on downloading chunks from big main.bin file
import os 
import logging
import json
from typing import Union
from gogdl import constants
from gogdl.dl import dl_utils
from gogdl.dl.managers.dependencies import DependenciesManager
from gogdl.dl.managers.task_executor import ExecutingManager
from gogdl.dl.workers.task_executor import DownloadTask1, DownloadTask2, TaskType, WriterTask
from gogdl.dl.objects import v1

manifests_dir = os.path.join(constants.CONFIG_DIR, "manifests")

class Manager:
    def __init__(self, generic_manager):
        self.game_id = generic_manager.game_id
        self.arguments = generic_manager.arguments
        self.unknown_arguments = generic_manager.unknown_arguments
        if "path" in self.arguments:
            self.path = self.arguments.path
        else:
            self.path = ""

        self.api_handler = generic_manager.api_handler
        self.should_append_folder_name = generic_manager.should_append_folder_name
        self.is_verifying = generic_manager.is_verifying
        self.allowed_threads = generic_manager.allowed_threads

        self.platform = generic_manager.platform

        self.builds = generic_manager.builds
        self.build = generic_manager.target_build
        self.version_name = self.build["version_name"]

        self.lang = self.arguments.lang or "English"
        self.dlcs_should_be_downloaded = self.arguments.dlcs
        if self.arguments.dlcs_list:
            self.dlcs_list = self.arguments.dlcs_list.split(",")

        else:
            self.dlcs_list = list()
        
        self.dlc_only = self.arguments.dlc_only

        self.manifest = None
        self.meta = None

        self.logger = logging.getLogger("V1")
        self.logger.info("Initialized V1 Download Manager")

    # Get manifest of selected build
    def get_meta(self):
        meta_url = self.build["link"]
        self.meta, headers = dl_utils.get_zlib_encoded(self.api_handler,meta_url)
        if not self.meta:
            raise Exception("There was an error obtaining meta")
        if headers:
            self.version_etag = headers.get("Etag")

        # Append folder name when downloading
        if self.should_append_folder_name:
            self.path = os.path.join(self.path, self.meta["product"]["installDirectory"]) 

    def get_download_size(self):
        self.get_meta()
        dlcs = self.get_dlcs_user_owns(True)
        self.manifest = v1.Manifest(self.platform, self.meta, self.lang, dlcs, self.api_handler, False)

        size_data = self.manifest.calculate_download_size()
        available_branches = set([build["branch"] for build in self.builds["items"] if build["branch"]])
        available_branches_list = [None] + list(available_branches)

        for dlc in dlcs:
            dlc.update({"size": size_data[dlc["id"]]})

        response = {
            "size": size_data[self.game_id],
            "dlcs": dlcs,
            "buildId": self.build["legacy_build_id"],
            "languages": self.manifest.list_languages(),
            "folder_name": self.meta["product"]["installDirectory"],
            "dependencies": [dep.id for dep in self.manifest.dependencies],
            "versionEtag": self.version_etag,
            "versionName": self.version_name,
            "available_branches": available_branches_list 
        }
        return response


    def get_dlcs_user_owns(self, info_command=False, requested_dlcs=None):
        if requested_dlcs is None:
            requested_dlcs = list()
        if not self.dlcs_should_be_downloaded and not info_command:
            return []
        self.logger.debug("Getting dlcs user owns")
        dlcs = []
        if len(requested_dlcs) > 0:
            for product in self.meta["product"]["gameIDs"]:
                if (
                        product["gameID"] != self.game_id # Check if not base game
                        and product["gameID"] in requested_dlcs # Check if requested by user
                        and self.api_handler.does_user_own(product["gameID"]) # Check if owned
                ):
                    dlcs.append({"title": product["name"]["en"], "id": product["gameID"]})
            return dlcs
        for product in self.meta["product"]["gameIDs"]:
            # Check if not base game and if owned
            if product["gameID"] != self.game_id and self.api_handler.does_user_own(
                    product["gameID"]
            ):
                dlcs.append({"title": product["name"]["en"], "id": product["gameID"]})
        return dlcs


    def download(self):
        self.get_meta()
        dlcs_user_owns = self.get_dlcs_user_owns(requested_dlcs=self.dlcs_list)

        if self.arguments.dlcs_list:
            self.logger.info(f"Requested dlcs {self.arguments.dlcs_list}")
            self.logger.info(f"Owned dlcs {dlcs_user_owns}")
        self.logger.debug("Parsing manifest")

        self.manifest = v1.Manifest(self.platform, self.meta, self.lang, dlcs_user_owns, self.api_handler, self.dlc_only)

        manifest_path = os.path.join(manifests_dir, self.game_id)
        old_manifest = None

        # Load old manifest
        if os.path.exists(manifest_path):
            with open(manifest_path, "r") as f_handle:
                try:
                    json_data = json.load(f_handle)
                    old_manifest = dl_utils.create_manifest_class(json_data, self.api_handler)
                except json.JSONDecodeError:
                    old_manifest = None
                    pass

        if self.is_verifying:
            if old_manifest:
                self.manifest = old_manifest
                old_manifest = None

        if self.manifest:
            self.manifest.get_files()

        if old_manifest:
            old_manifest.get_files()

        diff = v1.ManifestDiff.compare(self.manifest, old_manifest)

        self.logger.info(f"{diff}")

        download_tasks: list[Union[DownloadTask1, DownloadTask2]] = []
        writer_tasks: list[WriterTask] = []

        has_dependencies = len(self.manifest.dependencies) > 0
        
        secure_link_endpoints_ids = [product["id"] for product in dlcs_user_owns]
        if not self.dlc_only:
            secure_link_endpoints_ids.append(self.game_id)
        secure_links = dict()
        for product_id in secure_link_endpoints_ids:
            secure_links.update(
                {
                    product_id: dl_utils.get_secure_link(
                        self.api_handler, f"/{self.platform}/{self.build['legacy_build_id']}/", product_id, generation=1
                    )
                }
            )
        
        dependency_manager = DependenciesManager([dep.id for dep in self.manifest.dependencies], self.path, self.allowed_threads, self.api_handler, download_game_deps_only=True)
        
        # Find dependencies that are no longer used
        if old_manifest:
            removed_dependencies = [id for id in old_manifest.dependencies_ids if id not in self.manifest.dependencies_ids]
            
            for depot in dependency_manager.repository[0]["depots"]:
                if depot["dependencyId"] in removed_dependencies and not depot["executable"]["path"].startswith("__redist"):
                    diff.removed_redist += dependency_manager.get_files_for_depot_manifest(depot['manifest'])

        if has_dependencies:
            secure_links.update({'redist': dl_utils.get_dependency_link(self.api_handler)})
            
            diff.redist = dependency_manager.get(return_files=True) or []

            for f in diff.redist:
                if len(f.chunks) == 0:
                    writer_tasks.append(WriterTask(TaskType.CREATE, 'redist', f.flags, self.path, f.path, None))
                    continue
                for i, chunk in enumerate(f.chunks):
                    new_task = DownloadTask2(TaskType.DOWNLOAD_V2, 'redist', f.flags, i, chunk, self.path, f.path, True)
                    download_tasks.append(new_task)

        for f in diff.new:
            task = DownloadTask1(TaskType.DOWNLOAD_V1, f.product_id, f.flags, f.size, f.offset, f.hash, self.path, f.path)
            download_tasks.append(task)

        for f in diff.changed:
            task = DownloadTask1(TaskType.DOWNLOAD_V1, f.product_id, f.flags, f.size, f.offset, f.hash, self.path, f.path)
            download_tasks.append(task)

        executor = ExecutingManager(self.api_handler, self.allowed_threads, self.path, diff, secure_links)
        executor.setup(download_tasks, writer_tasks, [])
        dl_utils.prepare_location(self.path)
        # Remove all deleted files from diff
        [os.remove(os.path.join(self.path, f.path)) for f in diff.deleted if os.path.exists(os.path.join(self.path, f.path))]
        [os.remove(os.path.join(self.path, f.path)) for f in diff.removed_redist if os.path.exists(os.path.join(self.path, f.path))]
        for dir in self.manifest.dirs:
            manifest_dir_path = os.path.join(self.path, dir.path)
            dl_utils.prepare_location(dl_utils.get_case_insensitive_name(self.path, manifest_dir_path))
        if len(download_tasks) > 0 or len(writer_tasks) > 0:
            executor.run()

        dl_utils.prepare_location(manifests_dir)
        if self.manifest:
            with open(manifest_path, 'w') as f_handle:
                data = self.manifest.serialize_to_json()
                f_handle.write(data)

