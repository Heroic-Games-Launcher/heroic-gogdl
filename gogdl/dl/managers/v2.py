# Handle newer depots download
# This was introduced in GOG Galaxy 2.0, it features compression and files split by chunks
import json
from gogdl.dl import dl_utils
import gogdl.dl.objects.v2 as v2
import hashlib
from gogdl.dl.managers import dependencies
from gogdl.dl.managers.task_executor import ExecutingManager
from gogdl.dl.workers import task_executor
from gogdl import constants
import os
import logging

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

        self.allowed_threads = generic_manager.allowed_threads

        self.api_handler = generic_manager.api_handler
        self.should_append_folder_name = generic_manager.should_append_folder_name
        self.is_verifying = generic_manager.is_verifying

        self.builds = generic_manager.builds
        self.build = generic_manager.target_build
        self.version_name = self.build["version_name"]

        self.lang = self.arguments.lang or "en-US"
        self.dlcs_should_be_downloaded = self.arguments.dlcs
        if self.arguments.dlcs_list:
            self.dlcs_list = self.arguments.dlcs_list.split(",")
        else:
            self.dlcs_list = list()
        self.dlc_only = self.arguments.dlc_only

        self.manifest = None
        self.stop_all_threads = False

        self.logger = logging.getLogger("V2")
        self.logger.info("Initialized V2 Download Manager")

    def get_download_size(self):
        self.get_meta()
        dlcs = self.get_dlcs_user_owns(True)
        self.manifest = v2.Manifest(self.meta, self.lang, dlcs, self.api_handler, False)

        size_data = self.manifest.calculate_download_size()
        available_branches = set([build["branch"] for build in self.builds["items"] if build["branch"]])
        available_branches_list = [None] + list(available_branches)
        

        for dlc in dlcs:
            dlc.update({"size": size_data[dlc["id"]]})

        response = {
            "size": size_data[self.game_id],
            "dlcs": dlcs,
            "buildId": self.build["build_id"],
            "languages": self.manifest.list_languages(),
            "folder_name": self.meta["installDirectory"],
            "dependencies": self.manifest.dependencies_ids,
            "versionEtag": self.version_etag,
            "versionName": self.version_name,
            "available_branches": available_branches_list
        }
        return response

    def download(self):
        self.get_meta()
        dlcs_user_owns = self.get_dlcs_user_owns(
            requested_dlcs=self.dlcs_list
        )
    
        if self.arguments.dlcs_list:
            self.logger.info(f"Requested dlcs {self.arguments.dlcs_list}")
            self.logger.info(f"Owned dlcs {dlcs_user_owns}")
        self.logger.debug("Parsing manifest")

        self.manifest = v2.Manifest(
            self.meta, self.lang, dlcs_user_owns, self.api_handler, self.dlc_only
        )

        manifest_path = os.path.join(manifests_dir, self.game_id)
        old_manifest = None

        # Load old manifest
        if os.path.exists(manifest_path):
            self.logger.debug(f"Loading existing manifest for game {self.game_id}")
            with open(manifest_path, 'r') as f_handle:
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
        diff = v2.ManifestDiff.compare(self.manifest, old_manifest)
        self.logger.info(diff)


        dependencies_manager = dependencies.DependenciesManager(self.manifest.dependencies_ids, self.path,
                                                                self.arguments.workers_count, self.api_handler, download_game_deps_only=True)

        # Find dependencies that are no longer used
        if old_manifest:
            removed_dependencies = [id for id in old_manifest.dependencies_ids if id not in self.manifest.dependencies_ids]
            
            for depot in dependencies_manager.repository[0]["depots"]:
                if depot["dependencyId"] in removed_dependencies and not depot["executable"]["path"].startswith("__redist"):
                    diff.removed_redist += dependencies_manager.get_files_for_depot_manifest(depot['manifest'])


        if not len(diff.changed) and not len(diff.deleted) and not len(diff.new):
            self.logger.info("Nothing to do")
            return
        secure_link_endpoints_ids = [product["id"] for product in dlcs_user_owns]
        if not self.dlc_only:
            secure_link_endpoints_ids.append(self.game_id)
        secure_links = dict()
        for product_id in secure_link_endpoints_ids:
            secure_links.update(
                {
                    product_id: dl_utils.get_secure_link(
                        self.api_handler, "/", product_id
                    )
                }
            )

        
        diff.redist = dependencies_manager.get(True) or []


        if len(diff.redist) > 0:
            secure_links.update(
                {
                    'redist': dl_utils.get_dependency_link(self.api_handler)
                }
            )

        download_tasks = list()
        writer_tasks = list()
        writer_results = list()

        for f in diff.redist:
            if len(f.chunks) == 0:
                writer_tasks.append(task_executor.WriterTask(task_executor.TaskType.CREATE, 'redist', f.flags, self.path, f.path, None))
                continue
            for i, chunk in enumerate(f.chunks):
                new_task = task_executor.DownloadTask2(task_executor.TaskType.DOWNLOAD_V2, 'redist', f.flags, i, chunk, self.path, f.path, True)
                download_tasks.append(new_task)

        for f in diff.new:
            if len(f.chunks) == 0:
                writer_tasks.append(task_executor.WriterTask(task_executor.TaskType.CREATE, f.product_id, f.flags, self.path, f.path, None))
                continue
            joined_path = os.path.join(self.path, f.path)
            if os.path.exists(joined_path) and len(f.chunks) > 0:
                md5 = hashlib.md5()
                sha256 = hashlib.sha256()
                file_md5 = f.md5 or f.chunks[0]['md5']
                file_sha256 = f.sha256 
                with open(joined_path, 'rb') as fh:
                    while data := fh.read(1024 * 1024):
                        md5.update(data)
                        sha256.update(data)

                if file_md5 and md5.hexdigest() == file_md5 or file_sha256 and sha256.hexdigest() == file_sha256:
                    writer_results.append(task_executor.TaskResult(True, None, task_executor.WriterTask(task_executor.TaskType.ASSEMBLE, f.product_id, f.flags, self.path, f.path, len(f.chunks)), None))
                    continue
                
            for i, chunk in enumerate(f.chunks):
                new_task = task_executor.DownloadTask2(task_executor.TaskType.DOWNLOAD_V2, f.product_id, f.flags, i, chunk, self.path, f.path, False)
                download_tasks.append(new_task)

        for f in diff.changed:
            if type(f) == v2.DepotFile:
                for i, chunk in enumerate(f.chunks):
                    new_task = task_executor.DownloadTask2(task_executor.TaskType.DOWNLOAD_V2, f.product_id, f.flags, i, chunk, self.path,f.path, False)
                    download_tasks.append(new_task)
            else:
                for i, chunk in enumerate(f.file.chunks):
                    if not chunk.get("old_offset"):
                        new_task = task_executor.DownloadTask2(task_executor.TaskType.DOWNLOAD_V2, f.file.product_id, f.file.flags, i, chunk, self.path,f.file.path, False)
                        download_tasks.append(new_task)
                    else:
                        extract_task = task_executor.WriterTask(task_executor.TaskType.EXTRACT, f.file.product_id, f.file.flags, self.path, f.file.path, (i, chunk))
                        writer_tasks.append(extract_task)
                
        # TODO: Check available space before continuing 

        executor = ExecutingManager(self.api_handler, self.allowed_threads, self.path, diff, secure_links)
        executor.setup(download_tasks, writer_tasks, writer_results)
        # Remove all deleted files from diff
        [os.remove(os.path.join(self.path, f.path)) for f in diff.deleted if os.path.exists(os.path.join(self.path, f.path))]
        [os.remove(os.path.join(self.path, f.path)) for f in diff.removed_redist if os.path.exists(os.path.join(self.path, f.path))]
        if len(download_tasks) > 0 or len(writer_tasks) > 0 or len(writer_results) > 0:
            executor.run()
        
        dl_utils.prepare_location(manifests_dir)
        if self.manifest:
            with open(manifest_path, 'w') as f_handle:
                data = self.manifest.serialize_to_json()
                f_handle.write(data)

    def get_meta(self):
        meta_url = self.build["link"]
        self.meta, headers = dl_utils.get_zlib_encoded(self.api_handler, meta_url)
        self.version_etag = headers.get("Etag")

        # Append folder name when downloading
        if self.should_append_folder_name:
            self.path = os.path.join(self.path, self.meta["installDirectory"])

    def get_dlcs_user_owns(self, info_command=False, requested_dlcs=None):
        if requested_dlcs is None:
            requested_dlcs = list()
        if not self.dlcs_should_be_downloaded and not info_command:
            return []
        self.logger.debug("Getting dlcs user owns")
        dlcs = []
        if len(requested_dlcs) > 0:
            for product in self.meta["products"]:
                if (
                        product["productId"] != self.game_id
                        and product["productId"] in requested_dlcs
                        and self.api_handler.does_user_own(product["productId"])
                ):
                    dlcs.append({"title": product["name"], "id": product["productId"]})
            return dlcs
        for product in self.meta["products"]:
            if product["productId"] != self.game_id and self.api_handler.does_user_own(
                    product["productId"]
            ):
                dlcs.append({"title": product["name"], "id": product["productId"]})
        return dlcs
