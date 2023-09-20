import logging
import os
import signal
import time
from sys import exit
from threading import Thread
from collections import deque
from multiprocessing import Queue, Manager as ProcessingManager
from threading import Condition
from multiprocessing.shared_memory import SharedMemory
from queue import Empty
from typing import Counter, Union
from gogdl.dl import dl_utils

from gogdl.dl.dl_utils import get_readable_size
from gogdl.dl.progressbar import ProgressBar
from gogdl.dl.workers import task_executor
from gogdl.dl.objects import generic, v2, v1

class ExecutingManager:
    def __init__(self, api_handler, allowed_threads, path, support, diff, secure_links) -> None:
        self.api_handler = api_handler
        self.allowed_threads = allowed_threads
        self.path = path
        self.resume_file = os.path.join(path, '.gogdl-resume')
        self.support = support or os.path.join(path, 'gog-support')
        self.cache = os.path.join(path, '.gogdl-download-cache')
        self.diff: generic.BaseDiff = diff
        self.secure_links = secure_links
        self.logger = logging.getLogger("TASK_EXEC")

        self.download_size = 0
        self.disk_size = 0

        self.shared_memory = None
        self.shm_segments = deque()
        self.hash_map = dict()
        self.v2_chunks_to_download = deque()
        self.v1_chunks_to_download = deque()
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

        # Required space for download to succeed
        required_disk_size_delta = 0
        current_tmp_size = 0
        # This can be either v1 File or v2 DepotFile
        for f in self.diff.deleted + self.diff.removed_redist:
            support_flag = generic.TaskFlag.SUPPORT if 'support' in f.flags else generic.TaskFlag.NONE
            self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.DELETE_FILE | support_flag))
            if isinstance(f, v1.File):
                required_disk_size_delta -= f.size
            elif isinstance(f, v2.DepotFile):
                required_disk_size_delta -= sum([ch['size'] for ch in f.chunks])

        shared_chunks_counter = Counter()
        completed_files = set()

        missing_files = set()
        mismatched_files = set()

        downloaded_v1 = dict()
        cached = set()
        
        # Re-use cachces
        if os.path.exists(self.cache):
            for cache_file in os.listdir(self.cache):
                cached.add(cache_file)

        self.biggest_chunk = 0
        # Find biggest chunk to optimize how much memory is 'wasted' per chunk
        # Also create hashmap for those files
        for f in self.diff.new + self.diff.changed + self.diff.redist:
            if isinstance(f, v1.File):
                self.hash_map.update({f.path.lower(): f.hash})

            elif isinstance(f, v2.DepotFile):
                first_chunk_checksum = f.chunks[0]['md5'] if len(f.chunks) else None
                checksum = f.md5 or f.sha256 or first_chunk_checksum
                self.hash_map.update({f.path.lower(): checksum})
                for i, chunk in enumerate(f.chunks):
                    shared_chunks_counter[chunk["compressedMd5"]] += 1
                    if self.biggest_chunk < chunk["size"]:
                        self.biggest_chunk = chunk["size"]

            elif isinstance(f, v2.FileDiff):
                first_chunk_checksum = f.file.chunks[0]['md5'] if len(f.file.chunks) else None
                checksum = f.file.md5 or f.file.sha256 or first_chunk_checksum
                self.hash_map.update({f.file.path.lower(): checksum})
                for i, chunk in enumerate(f.file.chunks):
                    if not chunk.get("old_offset"):
                        shared_chunks_counter[chunk["compressedMd5"]] += 1
                        if self.biggest_chunk < chunk["size"]:
                            self.biggest_chunk = chunk["size"]
        if not self.biggest_chunk:
            self.biggest_chunk = 20 * 1024 * 1024
        else:
            # Have at least 8 MiB chunk size for V1 downloads
            self.biggest_chunk = max(self.biggest_chunk, 8 * 1024 * 1024)

        if os.path.exists(self.resume_file):
            self.logger.info("Attempting to continue the download")
            try:
                missing = 0
                mismatch = 0

                with open(self.resume_file, 'r') as f:
                    for line in f.readlines():
                        hash, support, file_path = line.strip().split(':')
                        
                        if support == 'support':
                            abs_path = os.path.join(self.support, file_path)
                        else:
                            abs_path = os.path.join(self.path, file_path)

                        if not os.path.exists(dl_utils.get_case_insensitive_name(abs_path)):
                            missing_files.add(file_path.lower())
                            missing += 1
                            continue
                        
                        current_hash = self.hash_map.get(file_path.lower())
                        if current_hash != hash:
                            mismatched_files.add(file_path.lower())
                            mismatch += 1
                            continue

                        completed_files.add(file_path.lower())
                if missing:
                    self.logger.warning(f'There are {missing} missing files, and will be re-downloaded')
                if mismatch:
                    self.logger.warning(f'There are {mismatch} changed files since last download, and will be re-downloaded')

            except Exception as e:
                self.logger.error(f"Unable to resume download, continuing as normal {e}")


        # Create tasks for each chunk
        for f in self.diff.new + self.diff.changed + self.diff.redist:
            if isinstance(f, v1.File):
                support_flag = generic.TaskFlag.SUPPORT if 'support' in f.flags else generic.TaskFlag.NONE
                if f.size == 0:
                    self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.CREATE_FILE | support_flag))
                    continue

                if f.path.lower() in completed_files:
                    downloaded_v1[f.hash] = f
                    continue

                required_disk_size_delta += f.size
                # In case of same file we can copy it over
                if f.hash in downloaded_v1:
                    self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.COPY_FILE | support_flag, old_flags=generic.TaskFlag.SUPPORT if 'support' in downloaded_v1[f.hash].flags else generic.TaskFlag.NONE, old_file=downloaded_v1[f.hash].path))
                    if 'executable' in f.flags:
                        self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.MAKE_EXE | support_flag))
                    continue
                self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.OPEN_FILE | support_flag))
                self.download_size += f.size
                self.disk_size += f.size
                size_left = f.size
                chunk_offset = 0
                i = 0
                # Split V1 file by chunks, so we can store it in shared memory
                # This makes checksum useless during the download, but well...
                while size_left:
                    chunk_size = min(self.biggest_chunk, size_left)
                    offset = f.offset + chunk_offset
                    
                    task = generic.V1Task(f.product_id, i, offset, chunk_size, f.hash)
                    self.tasks.append(task)
                    self.v1_chunks_to_download.append((f.product_id, task.compressed_md5, offset, chunk_size))

                    chunk_offset += chunk_size
                    size_left -= chunk_size
                    i += 1

                self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.CLOSE_FILE | support_flag))
                if 'executable' in f.flags:
                    self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.MAKE_EXE | support_flag))
                downloaded_v1[f.hash] = f

            elif isinstance(f, v2.DepotFile):
                support_flag = generic.TaskFlag.SUPPORT if 'support' in f.flags else generic.TaskFlag.NONE
                if not len(f.chunks):
                    self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.CREATE_FILE | support_flag))
                    continue
                if f.path.lower() in completed_files:
                    continue
                self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.OPEN_FILE | support_flag))
                for i, chunk in enumerate(f.chunks):
                    new_task = generic.ChunkTask(f.product_id, i, chunk["compressedMd5"], chunk["md5"], chunk["size"], chunk["compressedSize"])
                    is_cached = chunk["md5"] in cached
                    if shared_chunks_counter[chunk["compressedMd5"]] > 1 and not is_cached:
                        self.v2_chunks_to_download.append((f.product_id, chunk["compressedMd5"]))
                        self.download_size += chunk['compressedSize']
                        new_task.offload_to_cache = True
                        new_task.cleanup = True
                        cached.add(chunk["md5"])
                        current_tmp_size += chunk['size']
                    elif is_cached:
                        new_task.old_offset = 0
                        # This can safely be absolute path, due to
                        # how os.path.join works in Writer
                        new_task.old_file = os.path.join(self.cache, chunk["md5"])
                    else:
                        self.v2_chunks_to_download.append((f.product_id, chunk["compressedMd5"]))
                        self.download_size += chunk['compressedSize']
                    self.disk_size += chunk['size']
                    current_tmp_size += chunk['size']
                    shared_chunks_counter[chunk["compressedMd5"]] -= 1
                    new_task.cleanup = True
                    self.tasks.append(new_task)
                    if is_cached and shared_chunks_counter[chunk["compressedMd5"]] == 0:
                        cached.remove(chunk["md5"])
                        self.tasks.append(generic.FileTask(os.path.join(self.cache, chunk["md5"]), flags=generic.TaskFlag.DELETE_FILE))
                        current_tmp_size -= chunk['size']
                self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.CLOSE_FILE | support_flag))
                if 'executable' in f.flags:
                    self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.MAKE_EXE | support_flag))

            elif isinstance(f, v2.FileDiff):
                chunk_tasks = []
                reused = 0
                file_size = 0
                support_flag = generic.TaskFlag.SUPPORT if 'support' in f.file.flags else generic.TaskFlag.NONE
                old_support_flag = generic.TaskFlag.SUPPORT if 'support' in f.old_file_flags else generic.TaskFlag.NONE
                if f.file.path.lower() in completed_files:
                    continue
                for i, chunk in enumerate(f.file.chunks):
                    chunk_task = generic.ChunkTask(f.file.product_id, i, chunk["compressedMd5"], chunk["md5"], chunk["size"], chunk["compressedSize"])
                    file_size += chunk['size']
                    if chunk.get("old_offset") and f.file.path.lower() not in mismatched_files and f.file.path.lower() not in missing_files:
                        chunk_task.old_offset = chunk["old_offset"]
                        chunk_task.old_flags = old_support_flag
                        chunk_task.old_file = f.file.path
                        reused += 1

                        chunk_tasks.append(chunk_task)
                    else:
                        is_cached = chunk["md5"] in cached
                        if shared_chunks_counter[chunk["compressedMd5"]] > 1 and not is_cached:
                            self.v2_chunks_to_download.append((f.file.product_id, chunk["compressedMd5"]))
                            self.download_size += chunk['compressedSize']
                            chunk_task.offload_to_cache = True
                            cached.add(chunk["md5"])
                            current_tmp_size += chunk['size']
                        elif is_cached:
                            chunk_task.old_offset = 0
                            chunk_task.old_file = os.path.join(self.cache, chunk["md5"])
                        else:
                            self.v2_chunks_to_download.append((f.file.product_id, chunk["compressedMd5"]))
                            self.download_size += chunk['compressedSize']

                        shared_chunks_counter[chunk["compressedMd5"]] -= 1
                        chunk_task.cleanup = True
                        chunk_tasks.append(chunk_task)
                        if is_cached and shared_chunks_counter[chunk["compressedMd5"]] == 0:
                            cached.remove(chunk["md5"])
                            self.tasks.append(generic.FileTask(os.path.join(self.cache, chunk["md5"]), flags=generic.TaskFlag.DELETE_FILE))
                            current_tmp_size -= chunk['size']
                current_tmp_size += file_size
                required_disk_size_delta = max(current_tmp_size, required_disk_size_delta)
                if reused:
                    self.tasks.append(generic.FileTask(f.file.path + ".tmp", flags=generic.TaskFlag.OPEN_FILE | support_flag))
                    self.tasks.extend(chunk_tasks)
                    self.tasks.append(generic.FileTask(f.file.path + ".tmp", flags=generic.TaskFlag.CLOSE_FILE | support_flag))
                    self.tasks.append(generic.FileTask(f.file.path, flags=generic.TaskFlag.RENAME_FILE | generic.TaskFlag.DELETE_FILE | support_flag, old_file=f.file.path + ".tmp"))
                    current_tmp_size -= file_size
                else:
                    self.tasks.append(generic.FileTask(f.file.path, flags=generic.TaskFlag.OPEN_FILE | support_flag))
                    self.tasks.extend(chunk_tasks)
                    self.tasks.append(generic.FileTask(f.file.path, flags=generic.TaskFlag.CLOSE_FILE | support_flag))
                if 'executable' in f.file.flags:
                    self.tasks.append(generic.FileTask(f.file.path, flags=generic.TaskFlag.MAKE_EXE | support_flag))
                self.disk_size += file_size
            required_disk_size_delta = max(current_tmp_size, required_disk_size_delta)
            
        for f in self.diff.links:
            self.tasks.append(generic.FileTask(f.path, flags=generic.TaskFlag.CREATE_SYMLINK, old_file=f.target))

        self.progress = ProgressBar(self.disk_size, get_readable_size(self.disk_size))
        self.items_to_complete = len(self.tasks)
                
        return dl_utils.check_free_space(required_disk_size_delta, self.path)

        
    def run(self):
        self.shared_memory = SharedMemory(create=True, size=1024*1024*1024)
        self.logger.debug(f"Created shared memory {self.shared_memory.size / 1024 / 1024:.02f} MiB")

        chunk_size = self.biggest_chunk 
        for i in range(int(self.shared_memory.size / chunk_size)):
            segment = generic.MemorySegment(offset=i*chunk_size, end=i*chunk_size+chunk_size)
            self.shm_segments.append(segment)
        self.logger.debug(f"Created shm segments {len(self.shm_segments)}, chunk size = {self.biggest_chunk / 1024 / 1024:.02f} MiB")
        interrupted = False
        def handle_sig(num, frame):
            nonlocal interrupted
            self.interrupt_shutdown()
            interrupted = True
            exit(-num)

        try:
            self.threads.append(Thread(target=self.download_manager, args=(self.task_cond, self.shm_cond)))
            self.threads.append(Thread(target=self.process_task_results, args=(self.task_cond,)))
            self.threads.append(Thread(target=self.process_writer_task_results, args=(self.shm_cond,)))

            # Spawn workers 
            for _ in range(self.allowed_threads):
                worker = task_executor.Download(self.shared_memory.name, self.download_queue, self.download_res_queue, self.shared_secure_links)
                worker.start()
                self.download_workers.append(worker)
        
            self.writer_worker = task_executor.Writer(self.shared_memory.name, self.writer_queue, self.writer_res_queue, self.cache)
            self.writer_worker.start()

            [th.start() for th in self.threads]

            signal.signal(signal.SIGTERM, handle_sig)
            signal.signal(signal.SIGINT, handle_sig)

            if self.disk_size:
                self.progress.start()

            while self.processed_items < self.items_to_complete and not interrupted:
                time.sleep(1)
            if interrupted:
                return True
        except KeyboardInterrupt:
            return True
        
        self.shutdown()
        return

    def interrupt_shutdown(self):
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


    def shutdown(self):
        self.logger.debug("Stopping progressbar")
        self.progress.completed = True
        

        self.logger.debug("Sending terminate instruction to workers")
        for _ in range(self.allowed_threads):
            self.download_queue.put(generic.TerminateWorker())
        
        self.writer_queue.put(generic.TerminateWorker())
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
        with self.task_cond:
            self.task_cond.notify()

        with self.shm_cond:
            self.shm_cond.notify()

        try:
            os.remove(self.resume_file)
        except:
            self.logger.error("Failed to remove resume file")

    def download_manager(self, task_cond: Condition, shm_cond: Condition):
        self.logger.debug("Starting download scheduler")
        no_shm = False
        while self.running:
            while self.active_tasks <= self.allowed_threads * 2 and (self.v2_chunks_to_download or self.v1_chunks_to_download):

                try:
                    memory_segment = self.shm_segments.popleft()
                    no_shm = False
                except IndexError:
                    no_shm = True
                    break 

                if self.v1_chunks_to_download:
                    product_id, chunk_id, offset, chunk_size = self.v1_chunks_to_download.popleft()

                    try:
                        self.download_queue.put(task_executor.DownloadTask1(product_id, offset, chunk_size, chunk_id, memory_segment))
                        self.logger.debug(f"Pushed v1 download to queue {chunk_id} {product_id} {offset} {chunk_size}")
                        self.active_tasks += 1
                        continue
                    except Exception as e:
                        self.logger.warning(f"Failed to push v1 task to download {e}")
                        self.v1_chunks_to_download.appendleft((product_id, chunk_id, offset, chunk_size))
                        self.shm_segments.appendleft(memory_segment)
                        break
                elif self.v2_chunks_to_download:
                    product_id, chunk_hash = self.v2_chunks_to_download.popleft()
                    try:
                        self.download_queue.put(task_executor.DownloadTask2(product_id, chunk_hash, memory_segment), timeout=1)
                        self.logger.debug(f"Pushed DownloadTask2 for {chunk_hash}")
                        self.active_tasks += 1
                    except Exception as e:
                        self.logger.warning(f"Failed to push task to download {e}")
                        self.v2_chunks_to_download.appendleft((product_id, chunk_hash))
                        self.shm_segments.appendleft(memory_segment)
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
            
        current_dest = self.path
        current_file = ''

        while task and self.running:
            if isinstance(task, generic.FileTask):
                try:
                    task_dest = self.path
                    old_destination = None
                    if task.flags & generic.TaskFlag.SUPPORT:
                        task_dest = self.support
                    if task.old_flags & generic.TaskFlag.SUPPORT:
                        old_destination = self.support

                    writer_task = task_executor.WriterTask(task_dest, task.path, task.flags, old_destination=old_destination, old_file=task.old_file)
                    self.writer_queue.put(writer_task, timeout=1)
                    if task.flags & generic.TaskFlag.OPEN_FILE:
                        current_file = task.path
                        current_dest = task_dest 
                except Exception as e:
                    self.tasks.appendleft(task)
                    self.logger.warning(f"Failed to add queue element {e}")
                    continue

                try:
                    task: Union[generic.ChunkTask, generic.V1Task] = self.tasks.popleft()
                except IndexError:
                    break
                continue
            
            while ((task.compressed_md5 in ready_chunks) or task.old_file):
                shm = None
                if not task.old_file:
                    shm = ready_chunks[task.compressed_md5].task.memory_segment

                try:
                    self.logger.debug(f"Adding {task.compressed_md5} to writer")
                    flags =  generic.TaskFlag.NONE
                    old_destination = None
                    if task.cleanup:
                        flags |= generic.TaskFlag.RELEASE_MEM
                    if task.offload_to_cache:
                        flags |= generic.TaskFlag.OFFLOAD_TO_CACHE
                    if task.old_flags & generic.TaskFlag.SUPPORT:
                        old_destination = self.support
                    self.writer_queue.put(task_executor.WriterTask(current_dest, current_file, flags=flags, shared_memory=shm, old_destination=old_destination, old_file=task.old_file, old_offset=task.old_offset, size=task.size, hash=task.md5), timeout=1)
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
                    if res.success:
                        self.logger.debug(f"Chunk {res.task.compressed_sum} ready")
                        ready_chunks[res.task.compressed_sum] = res
                        self.progress.update_downloaded_size(res.download_size)
                        self.active_tasks -= 1
                    else:
                        self.logger.warning(f"Chunk download failed, reason {res.fail_reason}")
                        try:
                            self.download_queue.put(res.task, timeout=1)
                        except Exception as e:
                            self.logger.warning("Failed to resubmit download task, pushing to chunks queue")

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
                
                if res.success and res.task.flags & generic.TaskFlag.CLOSE_FILE:
                    if res.task.file_path.endswith('.tmp'):
                        res.task.file_path = res.task.file_path[:-4]
                    checksum = self.hash_map.get(res.task.file_path.lower())
                    if not checksum:
                        self.logger.warning(f"No checksum for closed file, unable to push to resume file {res.task.file_path}")
                    else:
                        if res.task.flags & generic.TaskFlag.SUPPORT:
                            support = "support"
                        else:
                            support = ""

                        with open(self.resume_file, 'a') as f:
                            f.write(f"{checksum}:{support}:{res.task.file_path}\n")

                if not res.success:
                    self.logger.fatal("Task writer failed")

                self.progress.update_bytes_written(res.written)
                if res.task.flags & generic.TaskFlag.RELEASE_MEM and res.task.shared_memory:
                    self.logger.debug(f"Releasing memory {res.task.shared_memory}")
                    self.shm_segments.appendleft(res.task.shared_memory)
                with shm_cond:
                    shm_cond.notify()
                self.processed_items += 1

            except Empty:
                continue

        self.logger.debug("Writer results collector exiting...")
    
