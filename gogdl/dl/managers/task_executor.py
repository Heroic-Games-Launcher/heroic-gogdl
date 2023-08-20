import logging
import os
import time
import hashlib
from threading import Thread
from collections import deque
from multiprocessing import Queue, Manager as ProcessingManager
from threading import Condition
from multiprocessing.shared_memory import SharedMemory
from queue import Empty
from typing import Counter
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
        self.cache = os.path.join(path, '.gogdl-download-cache')
        self.diff: generic.BaseDiff = diff
        self.secure_links = secure_links
        self.logger = logging.getLogger("TASK_EXEC")

        self.download_size = 0
        self.disk_size = 0

        self.downloaded_size = 0
        self.written_size = 0

        self.shared_memory = None
        self.shm_segments = deque()
        self.v2_chunks_to_download = deque()
        self.v1_files_to_download = deque()
        self.tasks = deque()
        self.active_tasks = 0

        self.processed_items = 0
        self.items_to_complete = 0

        self.download_workers = list()
        self.writer_worker = None
        self.threads = list()

        self.shm_cond = Condition()
        self.task_cond = Condition()
        
        self.running = True

    def setup(self):
        self.logger.debug("Beginning executor manager setup")
        self.logger.debug("Initializing queues")
        # Queues
        self.download_queue = Queue()
        self.download_res_queue = Queue()
        self.writer_queue = Queue()
        self.writer_res_queue = Queue()
        self.manager = ProcessingManager()
        self.shared_secure_links = self.manager.dict()
        self.shared_secure_links.update(self.secure_links)

        # This can be either v1 File or v2 DepotFile
        for f in self.diff.deleted + self.diff.removed_redist:
            self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.DELETE_FILE))

        for f in self.diff.new + self.diff.changed + self.diff.redist:
            if isinstance(f, v1.File):
                self.download_size += f.size
                self.disk_size += f.size
            elif isinstance(f, v2.DepotFile):
                self.download_size += sum([ch["compressedSize"] for ch in f.chunks])
                self.disk_size += sum([ch["size"] for ch in f.chunks])
            elif isinstance(f, v2.FileDiff):
                self.download_size += sum([ch["compressedSize"] for ch in f.file.chunks if not ch.get("old_offset")])
                self.disk_size += sum([ch["size"] for ch in f.file.chunks])


        shared_chunks_counter = Counter()
        cached = set()

        self.biggest_chunk = 0
        for f in self.diff.new + self.diff.changed + self.diff.redist:
            if isinstance(f, v2.DepotFile):
                for i, chunk in enumerate(f.chunks):
                    shared_chunks_counter[chunk["compressedMd5"]] += 1
                    if self.biggest_chunk < chunk["size"]:
                        self.biggest_chunk = chunk["size"]
            elif isinstance(f, v2.FileDiff):
                for i, chunk in enumerate(f.file.chunks):
                    if not chunk.get("old_offset"):
                        shared_chunks_counter[chunk["compressedMd5"]] += 1
                        if self.biggest_chunk < chunk["size"]:
                            self.biggest_chunk = chunk["size"]
        if not self.biggest_chunk:
            self.biggest_chunk = 20 * 1024 * 1024

        # Create tasks for each chunk
        for f in self.diff.new + self.diff.changed + self.diff.redist:
            if isinstance(f, v1.File):
                if f.size == 0:
                    self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.CREATE_FILE))
                    continue
                self.v1_files_to_download.append(f)
                self.tasks.append(f)
                if 'executable' in f.flags:
                    self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.MAKE_EXE))
            elif isinstance(f, v2.DepotFile):
                if not len(f.chunks):
                    self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.CREATE_FILE))
                    continue
                self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.OPEN_FILE))
                for i, chunk in enumerate(f.chunks):
                    new_task = generic.ChunkTask(f.product_id, i, chunk["compressedMd5"], chunk["md5"], chunk["size"], chunk["compressedSize"])
                    is_cached = chunk["compressedMd5"] in cached
                    if shared_chunks_counter[chunk["compressedMd5"]] > 1 and not is_cached:
                        self.v2_chunks_to_download.append((f.product_id, chunk["compressedMd5"]))
                        new_task.offload_to_cache = True
                        cached.add(chunk["compressedMd5"])
                    elif is_cached:
                        new_task.old_offset = 0
                        new_task.old_file = os.path.join(self.cache, chunk["compressedMd5"])
                    else:
                        self.v2_chunks_to_download.append((f.product_id, chunk["compressedMd5"]))
                    shared_chunks_counter[chunk["compressedMd5"]] -= 1
                    new_task.cleanup = shared_chunks_counter[chunk["compressedMd5"]] == 0
                    self.tasks.append(new_task)
                self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.CLOSE_FILE))
                if 'executable' in f.flags:
                    self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.MAKE_EXE))

            elif isinstance(f, v2.FileDiff):
                chunk_tasks = []
                reused = 0
                for i, chunk in enumerate(f.file.chunks):
                    chunk_task = generic.ChunkTask(f.file.product_id, i, chunk["compressedMd5"], chunk["md5"], chunk["size"], chunk["compressedSize"])
                    if chunk.get("old_offset"):
                        chunk_task.old_offset = chunk["old_offset"]
                        chunk_task.old_file = f.file.path
                        reused += 1
                    else:
                        is_cached = chunk["compressedMd5"] in cached
                        if shared_chunks_counter[chunk["compressedMd5"]] > 1 and not is_cached:
                            self.v2_chunks_to_download.append((f.file.product_id, chunk["compressedMd5"]))
                            chunk_task.offload_to_cache = True
                            cached.add(chunk["compressedMd5"])
                        elif is_cached:
                            chunk_task.old_offset = 0
                            chunk_task.old_file = os.path.join(self.cache, chunk["compressedMd5"])
                        else:
                            self.v2_chunks_to_download.append((f.file.product_id, chunk["compressedMd5"]))

                        shared_chunks_counter[chunk["compressedMd5"]] -= 1
                        chunk_task.cleanup = shared_chunks_counter[chunk["compressedMd5"]] == 0
                    chunk_tasks.append(chunk_task)
                if reused:
                    self.tasks.append(generic.FileTask(f.file.path + ".tmp", flags=generic.TaskFlag.OPEN_FILE))
                    self.tasks.extend(chunk_tasks)
                    self.tasks.append(generic.FileTask(f.file.path + ".tmp", flags=generic.TaskFlag.CLOSE_FILE))
                    self.tasks.append(generic.FileTask(f.file.path, flags=generic.TaskFlag.RENAME_FILE | generic.TaskFlag.DELETE_FILE, old_file=f.file.path + ".tmp"))
                else:
                    self.tasks.append(generic.FileTask(f.file.path, flags=generic.TaskFlag.OPEN_FILE))
                    self.tasks.extend(chunk_tasks)
                    self.tasks.append(generic.FileTask(f.file.path, flags=generic.TaskFlag.CLOSE_FILE))
                if 'executable' in f.file.flags:
                    self.tasks.append(generic.FileTask(f.file.path, flags=generic.TaskFlag.MAKE_EXE))

        self.progress = ProgressBar(self.disk_size, get_readable_size(self.disk_size))
        self.items_to_complete = len(self.tasks)

    def run(self):
        self.shared_memory = SharedMemory(create=True, size=1024*1024*1024)
        self.logger.debug(f"Created shared memory {self.shared_memory.size / 1024 / 1024:.02f} MiB")

        chunk_size = self.biggest_chunk 
        for i in range(int(self.shared_memory.size / chunk_size)):
            segment = generic.MemorySegment(offset=i*chunk_size, end=i*chunk_size+chunk_size)
            self.shm_segments.append(segment)
        self.logger.debug(f"Created shm segments {len(self.shm_segments)}, chunk size = {self.biggest_chunk / 1024 / 1024} MiB")

        try:
            self.threads.append(Thread(target=self.download_manager, args=(self.task_cond, self.shm_cond)))
            self.threads.append(Thread(target=self.process_task_results, args=(self.task_cond,)))
            self.threads.append(Thread(target=self.process_writer_task_results, args=(self.shm_cond,)))

            self.progress.start()
            [th.start() for th in self.threads]

            # Spawn workers 
            for _ in range(self.allowed_threads):
                worker = task_executor.Download(self.shared_memory.name, self.download_queue, self.download_res_queue, self.shared_secure_links)
                worker.start()
                self.download_workers.append(worker)
        
            self.writer_worker = task_executor.Writer(self.shared_memory.name, self.writer_queue, self.writer_res_queue, self.cache)
            self.writer_worker.start()

            while self.processed_items < self.items_to_complete:
                time.sleep(1)
        except KeyboardInterrupt:
            self.progress.completed = True
            self.running = False
            
            with self.task_cond:
                self.task_cond.notify()

            with self.shm_cond:
                self.shm_cond.notify()

            for t in self.threads:
                t.join(timeout=5.0)
                if t.is_alive():
                    self.logger.warning(f'Thread did not terminate! {repr(t)}')

            for child in self.download_workers:
                child.join(timeout=5.0)
                if child.exitcode is None:
                    child.terminate()
            
            # Clean queues
            for queue in [self.writer_res_queue, self.writer_queue, self.download_queue, self.download_res_queue]:
                try:
                    while True:
                        _ = queue.get_nowait()
                except Empty:
                    queue.close()
                    queue.join_thread()

            self.shared_memory.close()
            self.shared_memory.unlink()
            self.shared_memory = None
            return
        
        self.shutdown()

    def shutdown(self):
        self.logger.debug("Stopping progressbar")
        self.progress.completed = True
        

        self.logger.debug("Sending terminate instruction to workers")
        for _ in range(self.allowed_threads):
            self.download_queue.put(generic.TerminateWorker())
        
        self.writer_queue.put(generic.TerminateWorker())
        with self.task_cond:
            self.task_cond.notify()

        with self.shm_cond:
            self.shm_cond.notify()
        for worker in self.download_workers:
            worker.join(timeout=2)
            if worker.is_alive():
                self.logger.warning("Forcefully terminating download workers")
                worker.terminate()
        self.writer_worker.join(timeout=10)
        
        self.writer_queue.close()
        self.writer_res_queue.close()
        self.download_queue.close()
        self.download_res_queue.close()

        self.logger.debug("Unlinking shared memory")
        if self.shared_memory:
            self.shared_memory.close()
            self.shared_memory.unlink()
            self.shared_memory = None
        self.running = False

    def download_manager(self, task_cond: Condition, shm_cond: Condition):
        self.logger.debug("Starting download scheduler")
        no_shm = False
        while (self.v2_chunks_to_download or self.v1_files_to_download) and self.running:
            while self.active_tasks < self.allowed_threads and (self.v2_chunks_to_download or self.v1_files_to_download):
                try:
                    file = self.v1_files_to_download.popleft()
                    # V1 files stream directly to drive
                    self.download_queue.put(task_executor.DownloadTask1(file.product_id, file.flags, file.size, file.offset, file.hash, self.path, file.path))
                    self.logger.debug("Pushed v1 download to queue")
                    self.active_tasks += 1
                    continue
                except IndexError:
                    pass

                try:
                    memory_segment = self.shm_segments.popleft()
                    no_shm = False
                except IndexError:
                    no_shm = True
                    break 

                product_id, chunk_hash = self.v2_chunks_to_download.popleft()
                try:
                    self.download_queue.put(task_executor.DownloadTask2(product_id, chunk_hash, memory_segment), timeout=1)
                    self.logger.debug(f"Pushed DownloadTask2 for {chunk_hash}")
                    self.active_tasks += 1
                except Exception as e:
                    self.logger.warning(f"Failed to push task to download {e}")
                    self.v2_chunks_to_download.appendleft((product_id, chunk_hash))
                    break

            else:
                with task_cond:
                    self.logger.debug("Waiting for more tasks")
                    task_cond.wait(timeout=1.0)
                    continue

            if no_shm:
                with shm_cond:
                    self.logger.debug(f"Waiting for more memory")
                    shm_cond.wait(timeout=1.0)

        self.logger.debug("Download scheduler out..")


    def process_task_results(self, task_cond: Condition):
        self.logger.debug("Download results collector starting")
        ready_chunks = dict()

        try:
            task = self.tasks.popleft()
        except IndexError:
            task = None
            
        current_file = ''

        while task and self.running:
            if isinstance(task, generic.FileTask):
                try:
                    writer_task = task_executor.WriterTask(self.path, task.path, task.flags, old_file=task.old_file)
                    self.writer_queue.put(writer_task, timeout=1)
                    if task.flags & generic.TaskFlag.OPEN_FILE:
                        current_file = task.path
                except Exception as e:
                    self.tasks.appendleft(task)
                    self.logger.warning(f"Failed to add queue element {e}")
                    continue

                try:
                    task: generic.Union[ChunkTask, v1.File] = self.tasks.popleft()
                except IndexError:
                    break
                continue
            
            while (not isinstance(task, v1.File)) and ((task.compressed_md5 in ready_chunks) or task.old_file):
                shm = None
                if not task.old_file:
                    shm = ready_chunks[task.compressed_md5].task.memory_segment

                try:
                    self.logger.debug(f"Adding {task.compressed_md5} to writer")
                    flags =  generic.TaskFlag.NONE
                    if task.cleanup:
                        flags |= generic.TaskFlag.RELEASE_MEM
                    if task.offload_to_cache:
                        flags |= generic.TaskFlag.OFFLOAD_TO_CACHE
                    self.writer_queue.put(task_executor.WriterTask(self.path, current_file, flags=flags, shared_memory=shm, old_file=task.old_file, old_offset=task.old_offset, size=task.size, hash=task.compressed_md5), timeout=1)
                except Exception as e:
                    self.logger.error(f"Adding to writer queue failed {e}")
                    break

                if task.cleanup and not task.old_file:
                    del ready_chunks[task.compressed_md5]

                try:
                    task = self.tasks.popleft()
                    if isinstance(task, generic.FileTask):
                        break
                except IndexError:
                    task = None
                    break

            else:
                try:
                    res: task_executor.DownloadTaskResult = self.download_res_queue.get(timeout=1)
                    if isinstance(res.task, task_executor.DownloadTask1):
                        if not res.success:
                            self.download_queue.put(res.task)
                            self.active_tasks += 1
                        else:
                            self.processed_items += 1

                    elif res.success:
                        self.logger.debug(f"Chunk {res.task.compressed_sum} ready")
                        ready_chunks[res.task.compressed_sum] = res
                        self.progress.update_downloaded_size(res.download_size)
                        self.progress.update_bytes_written(res.decompressed_size)
                    else:
                        self.logger.warning(f"Chunk download failed, reason {res.fail_reason}")
                        try:
                            self.download_queue.put(res.task, timeout=1)
                            self.active_tasks += 1
                        except Exception as e:
                            self.logger.warning("Failed to resubmit download task, pushing to chunks queue")
                            self.v2_chunks_to_download.appendleft((res.task.product_id, res.task.compressed_sum))

                    self.active_tasks -= 1
                    with task_cond:
                        task_cond.notify()
                except Empty:
                    pass
                except Exception as e:
                    self.logger.warning(f"Unhandled exception {e}")

        self.logger.debug("Download results collector exiting...")

    def process_writer_task_results(self, shm_cond: Condition):
        self.logger.debug("Starting writer results collector")
        while self.running:
            try:
                res: task_executor.WriterTaskResult = self.writer_res_queue.get(timeout=1)

                if isinstance(res.task, generic.TerminateWorker):
                    break
                
                if not res.success:
                    self.logger.fatal("Task writer failed")

                if res.task.flags & generic.TaskFlag.RELEASE_MEM and res.task.shared_memory:
                    self.logger.debug(f"Releasing memory {res.task.shared_memory}")
                    self.shm_segments.appendleft(res.task.shared_memory)
                    with shm_cond:
                        shm_cond.notify()
                self.processed_items += 1

            except Empty:
                continue

        self.logger.debug("Writer results collector exiting...")
    
