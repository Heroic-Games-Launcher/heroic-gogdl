# Handle newer depots download
# This was introduced in GOG Galaxy 2.0, it features compression and files split by chunks
import json
import sys
import signal
import gogdl.dl.objects.v2 as v2
from gogdl.dl.workers.v2 import DLWorker
from gogdl.dl import dl_utils
from gogdl.dl.managers import dependencies
from gogdl import constants
from concurrent.futures import ProcessPoolExecutor, as_completed
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

        self.api_handler = generic_manager.api_handler
        self.should_append_folder_name = generic_manager.should_append_folder_name
        self.is_verifying = generic_manager.is_verifying

        self.builds = generic_manager.builds
        self.build = generic_manager.target_build
        self.version_name = self.build["version_name"]

        self.lang = self.arguments.lang
        self.dlcs_should_be_downloaded = self.arguments.dlcs
        if self.arguments.dlcs_list:
            self.dlcs_list = self.arguments.dlcs_list.split(",")
        else:
            self.dlcs_list = list()
        self.dlc_only = self.arguments.dlc_only

        self.manifest = None

        self.logger = logging.getLogger("V2")
        self.logger.info("Initialized V2 Download Manager")

    def get_download_size(self):
        self.get_meta()
        dlcs = self.get_dlcs_user_owns(True)
        self.manifest = v2.Manifest(self.meta, self.lang, dlcs, self.api_handler, False)

        download_size, disk_size = self.manifest.calculate_download_size()
        available_branches = set([build["branch"] for build in self.builds["items"]])

        response = {
            "download_size": download_size,
            "disk_size": disk_size,
            "dlcs": dlcs,
            "buildId": self.build["build_id"],
            "languages": self.manifest.list_languages(),
            "folder_name": self.meta["installDirectory"],
            "dependencies": self.manifest.dependencies_ids,
            "versionEtag": self.version_etag,
            "versionName": self.version_name,
            "available_branches": list(available_branches)
        }
        return response

    def download(self):
        self.get_meta()
        dlcs_user_owns = self.get_dlcs_user_owns(
            requested_dlcs=self.dlcs_list
        )
        if self.arguments.dlcs_list:
            self.logger.info(f"Requested dlcs {self.arguments.dlcs_list}")
        self.logger.debug("Parsing manifest")

        self.manifest = v2.Manifest(
            self.meta, self.lang, dlcs_user_owns, self.api_handler, self.dlc_only
        )

        manifest_path = os.path.join(manifests_dir, self.game_id)
        old_manifest = None

        # Load old manifest
        if os.path.exists(manifest_path):
            with open(manifest_path, 'r') as f_handle:
                try:
                    json_data = json.load(f_handle)
                    old_manifest = v2.Manifest.from_json(json_data, self.api_handler)
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

        dependencies_manager = dependencies.DependenciesManager(self.manifest.dependencies_ids, self.path, 2,
                                                                self.arguments.workers_count, self.api_handler, True)

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
        workers = list()
        dependency_workers = dependencies_manager.get(True)
        workers.extend(dependency_workers)
        threads = list()
        thread_pool = ProcessPoolExecutor(self.arguments.workers_count)

        for file in diff.deleted:
            file_path = os.path.join(self.path, file.path)
            if os.path.exists(file_path):
                os.remove(file_path)

        for directory in self.manifest.dirs:
            os.makedirs(os.path.join(self.path, directory.path), exist_ok=True)

        for file in diff.new:
            worker = DLWorker(
                file,
                self.path,
                self.api_handler,
                file.product_id,
                secure_links[file.product_id],
            )
            workers.append(worker)  # Register workers

        # TODO: Support diff.updated patching

        for worker in workers:
            threads.append(thread_pool.submit(worker.work))  # Begin execution

        def shut(sig, code):
            thread_pool.shutdown(wait=True, cancel_futures=True)
            sys.exit()

        signal.signal(signal.SIGINT, shut)
        signal.signal(signal.SIGTERM, shut)

        for thread in as_completed(threads):
            if thread.cancelled():
                self.cancelled = True
                break

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
