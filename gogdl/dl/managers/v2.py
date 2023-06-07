# Handle newer depots download
# This was introduced in GOG Galaxy 2.0, it features compression and files split by chunks
import json
import time
from multiprocessing import Queue, Manager as ProcessingManager
from queue import Empty
from threading import Thread
from gogdl.dl import dl_utils
import gogdl.dl.objects.v2 as v2
import gogdl.dl.workers.task_executor as task_executor
from gogdl.dl.managers import dependencies
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
        available_branches = set([build["branch"] for build in self.builds["items"]])

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
            self.logger.info(f"Owned dlcs {dlcs_user_owns}")
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
        # TODO: Check available space before continuing 
        self.logger.info(diff)

        dependencies_manager = dependencies.DependenciesManager(self.manifest.dependencies_ids, self.path,
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
        dependency_tasks = dependencies_manager.get(True)
        

        # Queues
        download_queue = Queue()
        download_res_queue = Queue()
        writer_queue = Queue()
        writer_res_queue = Queue()
        manager = ProcessingManager()
        shared_secure_links = manager.dict()
        shared_secure_links.update(secure_links)

        download_workers = list()
        writer_workers = list()

        # Remove all deleted files from diff
        [os.remove(os.path.join(self.path, f.path)) for f in diff.deleted if os.path.exists(os.path.join(self.path, f.path))]

        task_results_processor = Thread(target=self.process_task_results, args=(download_workers, diff, shared_secure_links, download_queue, writer_queue, download_res_queue))
        writer_results_processor = Thread(target=self.process_writer_task_results, args=(diff, download_queue, writer_queue, writer_res_queue))

        allowed_downloaders = max(self.allowed_threads - 2, 1)
        allowed_writers = min(allowed_downloaders, 2)

        # Spawn workers 
        for i in range(allowed_downloaders):
            worker = task_executor.Download(download_queue, download_res_queue, shared_secure_links)
            worker.start()
            download_workers.append(worker)
        
        for i in range(allowed_writers):
            worker = task_executor.Writer(writer_queue, writer_res_queue)
            worker.start()
            writer_workers.append(worker)

        for f in diff.new:
            if len(f.chunks) == 0:
                writer_queue.put(task_executor.WriterTask(task_executor.TaskType.CREATE, f.product_id, self.path, f.path, 0))
            for i, chunk in enumerate(f.chunks):
                new_task = task_executor.DownloadTask(task_executor.TaskType.DOWNLOAD, f.product_id, i, chunk, self.path,f.path, True)
                download_queue.put(new_task)

        for f in diff.changed:
            if type(f) == v2.DepotFile:
                for i, chunk in enumerate(f.chunks):
                    new_task = task_executor.DownloadTask(task_executor.TaskType.DOWNLOAD, f.product_id, i, chunk, self.path,f.path, True)
                    download_queue.put(new_task)
            else:
                print(f"Complex patching of {f.path}")

     
        task_results_processor.start()
        writer_results_processor.start()

        writer_results_processor.join()
        task_results_processor.join()
        
        [download_queue.put(task_executor.DownloadTask(task_executor.TaskType.EXIT, None, None, None, None, None, False)) for worker in download_workers]
        [writer_queue.put(task_executor.DownloadTask(task_executor.TaskType.EXIT, None, None, None, None, None, False)) for worker in writer_workers]

        [worker.join() for worker in download_workers]
        [worker.join() for worker in writer_workers]

        download_queue.close()
        download_res_queue.close()
        writer_queue.close()
        writer_res_queue.close()
        manager.shutdown()
        
        dl_utils.prepare_location(manifests_dir)

        if self.manifest:
            with open(manifest_path, 'w') as f_handle:
                data = self.manifest.serialize_to_json()
                f_handle.write(data)

    def get_meta(self):
        meta_url = self.build["link"]
        self.meta, headers = dl_utils.get_zlib_encoded(self.api_handler, meta_url)
        self.version_etag = headers.get("Etag")

        # Append folder name when downloadin>g
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


    def process_task_results(self, workers: list[task_executor.Download], diff: v2.ManifestDiff, secure_links:dict, download_queue: Queue, writer_queue: Queue, results_queue: Queue):
        duplicate_unauthorized_buffer_time = 10
        refreshed_secure_links_timestamps = {}
        # Dict storing the tuple (number_of_chunks, ready_chunks)
        state: dict[str, tuple[int, list[int]]] = dict()
        while True:
            try:
                res: task_executor.TaskResult = results_queue.get(timeout=1)
            except Empty:
                if self.stop_all_threads:
                    break
                continue
            if not res.success:
                if res.fail_reason in [task_executor.FailReason.CHECKSUM, task_executor.FailReason.CONNECTION]:
                    self.logger.info(f"Retrying {res.task.file_path}")
                    download_queue.put(res.task)
                
                if res.fail_reason == task_executor.FailReason.UNKNOWN:
                    self.logger.warning(f"Unknown fail reason, retrying the {res.task.file_path}")
                    download_queue.put(res.task)

                if res.fail_reason == task_executor.FailReason.UNAUTHORIZED:
                    last_refreshed_timestamp = refreshed_secure_links_timestamps.get(res.task.product_id)

                    if last_refreshed_timestamp + duplicate_unauthorized_buffer_time > time.time():
                        download_queue.put(res.task)
                        continue                    

                    self.logger.info(f"Secure link for {res.task.product_id} expired, refreshing")
                    new = self.api_handler.get_new_secure_link(res.task.product_id)
                    secure_links.update({res.task.product_id: new})
                    refreshed_secure_links_timestamps.update({res.task.product_id: time.time()})
                    download_queue.put(res.task)
                continue

                
            file_path = res.task.file_path
            if state.get(file_path): 
                state[file_path][1].append(res.task.chunk_index)
            else:
                found = None
                for f in diff.new:
                    if f.path == file_path:
                        found = f
                        break

                if not found:
                    for f in diff.changed:
                        if f.file.path == file_path:
                            found = f
                            break
                if not found:
                    self.logger.warning("Somehow we are downloading file that's not in the manifest")
                    continue
                state[file_path] = (len(found.chunks), [res.task.chunk_index])
            if state[file_path][0] <= len(state[file_path][1]):
                self.logger.info(f"File {file_path} is ready")
                writer_queue.put(task_executor.WriterTask(task_executor.TaskType.ASSEMBLE, res.task.product_id, self.path, file_path, state[file_path][0])) 

    def process_writer_task_results(self, diff: v2.ManifestDiff, download_queue: Queue, writer_queue: Queue, writer_res_queue: Queue):
        finished = 0
        while True:
            
            res: task_executor.TaskResult = writer_res_queue.get()

            if not res.success:
                if res.fail_reason.MISSING_CHUNK:
                    chunk_index = res.context
                    file_path = res.task.file_path 
                    found = None
                    for f in diff.new:
                        if f.path == file_path:
                            found = f
                            break

                    if not found:
                        for f in diff.changed:
                            if f.file.path == file_path:
                                found = f
                                break
                    if not found:
                        self.logger.info(f"MISSING CHUNK for file {file_path}, was not able to continue skipping the file")
                        continue
                    
                    chunk_data = found.chunks[chunk_index]
                    new_task = task_executor.DownloadTask(task_executor.TaskType.DOWNLOAD, res.task.product_id, chunk_index, chunk_data, self.path, file_path, True)
                    download_queue.put(new_task)
                continue
            
            finished += 1
            if finished == len(self.manifest.files):
                self.stop_all_threads = True
                break