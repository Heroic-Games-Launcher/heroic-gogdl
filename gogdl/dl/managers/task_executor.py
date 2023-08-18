import logging
import os
import time
import hashlib
from threading import Thread
from multiprocessing import Queue, Manager as ProcessingManager
from queue import Empty
from gogdl.dl import dl_utils

from gogdl.dl.dl_utils import get_readable_size
from gogdl.dl.progressbar import ProgressBar
from gogdl.dl.workers import task_executor
from gogdl.dl.objects import generic, v2, v1

class ExecutingManager:
    def __init__(self, api_handler, allowed_threads, path, diff, secure_links) -> None:
        self.api_handler = api_handler
        self.allowed_threads = allowed_threads
        self.path = path
        self.diff: generic.BaseDiff = diff
        self.secure_links = secure_links
        self.logger = logging.getLogger("TASK_EXEC")

        self.download_size = 0
        self.disk_size = 0

        self.downloaded_size = 0
        self.written_size = 0
        
        self.stop_all_threads = False

    def setup(self, download_tasks, writer_tasks, writer_results):
        self.finished = 0
        # Queues
        self.download_queue = Queue()
        self.download_res_queue = Queue()
        self.writer_queue = Queue()
        self.writer_res_queue = Queue()
        self.manager = ProcessingManager()
        self.shared_secure_links = self.manager.dict()
        self.shared_secure_links.update(self.secure_links)

        for f in self.diff.new + self.diff.changed + self.diff.redist:
            if type(f) == v1.File:
                self.download_size += f.size
                self.disk_size += f.size
            elif type(f) == v2.DepotFile:
                self.download_size += sum([ch["compressedSize"] for ch in f.chunks])
                self.disk_size += sum([ch["size"] for ch in f.chunks])
            elif type(f) == v2.FileDiff:
                self.download_size += sum([ch["compressedSize"] for ch in f.file.chunks if not ch.get("old_offset")])
                self.disk_size += sum([ch["size"] for ch in f.file.chunks])


        self.progress = ProgressBar(self.disk_size, get_readable_size(self.disk_size))

        self.download_workers = list()
        self.writer_workers = list()

        for task in download_tasks:
            self.download_queue.put(task)
        for task in writer_tasks:
            self.writer_queue.put(task)
        for task in writer_results:
            self.writer_res_queue.put(task)

    def run(self):

        # Dict storing the tuple (number_of_chunks, ready_chunks, complex_patching)
        state: dict[str, tuple[int, list[int], bool]] = dict()

        task_results_processor = Thread(target=self.process_task_results, args=(state, self.diff, self.shared_secure_links, self.download_queue, self.writer_queue, self.download_res_queue))
        writer_results_processor = Thread(target=self.process_writer_task_results, args=(state, self.diff, self.download_queue, self.writer_queue, self.writer_res_queue))

        allowed_downloaders = max(self.allowed_threads - 2, 1)
        allowed_writers = min(allowed_downloaders, 2)

        self.progress.start()
        task_results_processor.start()
        writer_results_processor.start()

        # Spawn workers 
        for _ in range(allowed_downloaders):
            worker = task_executor.Download(self.download_queue, self.download_res_queue, self.shared_secure_links)
            worker.start()
            self.download_workers.append(worker)
        
        for _ in range(allowed_writers):
            worker = task_executor.Writer(self.writer_queue, self.writer_res_queue)
            worker.start()
            self.writer_workers.append(worker)

        writer_results_processor.join()
        task_results_processor.join()
        self.shutdown()

    def shutdown(self, force=False):
        self.progress.completed = True
        if not force:
            [self.download_queue.put(task_executor.Task(task_executor.TaskType.EXIT, "", [])) for _ in self.download_workers]
            [self.writer_queue.put(task_executor.Task(task_executor.TaskType.EXIT, "", [])) for _ in self.writer_workers]

            [worker.join() for worker in self.download_workers]
            [worker.join() for worker in self.writer_workers]
        else:
            for worker in self.download_workers:
                worker.early_exit = True
            for worker in self.writer_workers:
                worker.early_exit = True

            [worker.join() for worker in self.download_workers]
            [worker.join() for worker in self.writer_workers]
        self.progress.join()

        self.download_queue.close()
        self.download_res_queue.close()
        self.writer_queue.close()
        self.writer_res_queue.close()
        self.manager.shutdown()

    def process_task_results(self, state: dict[str, tuple[int, list[int], bool]], diff: generic.BaseDiff, secure_links:dict, download_queue: Queue, writer_queue: Queue, results_queue: Queue):
        duplicate_unauthorized_buffer_time = 10
        refreshed_secure_links_timestamps = {}
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
                    last_refreshed_timestamp = refreshed_secure_links_timestamps.get(res.task.product_id) or 0

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
            chunk_index = 0 if type(res.task) == task_executor.DownloadTask1 else res.task.chunk_index
            result, compex_patch = self.update_download_state(diff, state, file_path, chunk_index)
            if not result:
                continue

            if type(res.task) == task_executor.DownloadTask2:
                self.progress.update_downloaded_size(res.task.chunk_data["compressedSize"])
                self.progress.update_bytes_written(res.task.chunk_data["size"])
            elif type(res.task) == task_executor.DownloadTask1:
                self.progress.update_downloaded_size(res.task.size)
                self.progress.update_bytes_written(res.task.size)

    

            if state[file_path][0] <= len(state[file_path][1]) and type(res.task) != task_executor.DownloadTask1:
                writer_queue.put(task_executor.WriterTask(task_executor.TaskType.ASSEMBLE, res.task.product_id, res.task.flags, self.path, file_path, (state[file_path][0], compex_patch))) 
            elif type(res.task) == task_executor.DownloadTask1:
                self.finished += 1

                if self.finished == len(diff.new) + len(diff.changed) + len(diff.redist):
                    self.stop_all_threads = True
                    break
                

    def process_writer_task_results(self, state: dict[str, tuple[int, list[int], bool]], diff: generic.BaseDiff, download_queue: Queue, writer_queue: Queue, writer_res_queue: Queue):
        while True:
            try:
                res: task_executor.TaskResult = writer_res_queue.get(timeout=1)
            except Empty:
                if self.stop_all_threads:
                    break
                continue
            if not res.success:
                if res.fail_reason == task_executor.FailReason.MISSING_CHUNK:
                    chunk_index = res.context[0]
                    file_path = res.task.file_path 
                    found = None
                    for f in diff.new:
                        if f.path == file_path:
                            found = f
                            break

                    if not found:
                        for f in diff.changed:
                            if type(f) != v2.FileDiff and f.path == file_path:
                                found = f
                                break
                            elif type(f) == v2.FileDiff and f.file.path == file_path:
                                found = f.file
                                break
                    if not found:
                        self.logger.info(f"MISSING CHUNK for file {file_path}, was not able to continue skipping the file")
                        continue
                    
                    chunk_data = found.chunks[chunk_index]
                    new_task = task_executor.DownloadTask2(task_executor.TaskType.DOWNLOAD_V2, res.task.product_id, res.task.flags, chunk_index, chunk_data, self.path, file_path, False)
                    download_queue.put(new_task)
                    continue
                continue
            if res.task.type == task_executor.TaskType.EXTRACT:
                self.update_download_state(diff, state, res.task.file_path, res.task.context)
                continue
            if res.task.type == task_executor.TaskType.ASSEMBLE:
                if type(res.task.context) == tuple and res.task.context[1]:
                    file = None
                    for f in diff.changed:
                        if type(f) == v2.DepotFile:
                            if f.path == res.task.file_path:
                                file = f
                                break
                            continue
                        if f.file.path == res.task.file_path:
                            file = f
                            break
                    destination = dl_utils.get_case_insensitive_name(res.task.destination, os.path.join(res.task.destination, res.task.file_path))
                    new_file = destination+".new"
                    with open(new_file, "rb") as new_f_handle:
                        md5_sum = hashlib.md5()
                        sha256_sum = hashlib.sha256()

                        f_md5 = file.file.md5 or file.file.chunks[0]['md5']
                        f_sha256 = file.file.sha256

                        while chunk:=new_f_handle.read(1024 * 1024):
                            md5_sum.update(chunk)
                            sha256_sum.update(chunk)

                        if not (f_md5 and md5_sum.hexdigest() == f_md5 or f_sha256 and sha256_sum.hexdigest() == f_sha256):
                            state[file.file.path] = (state[file.file.path][0], [], state[file.file.path][2])
                            os.remove(new_file)
                            for i, chunk in enumerate(file.file.chunks):
                                new_task = task_executor.DownloadTask2(task_executor.TaskType.DOWNLOAD_V2, file.file.product_id, file.file.flags, i, chunk, self.path,file.file.path, True, False)
                                download_queue.put(new_task)
                            continue
                        os.rename(new_file, destination)

            self.finished += 1
            if self.finished == len(diff.new) + len(diff.changed) + len(diff.redist):
                self.stop_all_threads = True
                break

    
    def update_download_state(self, diff: generic.BaseDiff, state: dict[str, tuple[int, list[int], bool]], file_path: str, index: int):
        if state.get(file_path): 
            state[file_path][1].append(index)
            return True, state[file_path][2]
        else:
            found = None
            for f in diff.new:
                if f.path == file_path:
                    found = f
                    break
            if not found:
                for f in diff.redist:
                    if f.path == file_path:
                        found = f
                        break
            if not found:
                for f in diff.changed:
                    if type(f) == v2.DepotFile:
                        if f.path == file_path:
                            found = f
                            break
                        continue
                    if f.file.path == file_path:
                        found = f
                        break
            if not found:
                self.logger.warning("Somehow we are downloading file that's not in the manifest")
                return False, False
            if type(found) == v1.File:
                state[file_path] = (1, [0], False)
                return True, False
            if type(found) == v2.FileDiff:
                state[file_path] = (len(found.file.chunks), [index], True)
                return True, True
            else:
                state[file_path] = (len(found.chunks), [index], False)
                return True, False
