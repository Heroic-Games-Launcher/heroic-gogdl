from multiprocessing.shared_memory import SharedMemory
import os
from queue import Empty
import shutil
import sys
import stat
import requests
import zlib
import hashlib
from typing import Any, Optional, Union
from copy import copy
from gogdl.dl import dl_utils
from dataclasses import dataclass
from enum import Enum, auto
from multiprocessing import Process, Queue
from gogdl.dl.objects.generic import MemorySegment, TaskFlag, TerminateWorker


class FailReason(Enum):
    UNKNOWN = 0
    CHECKSUM = auto()
    CONNECTION = auto()
    UNAUTHORIZED = auto()

    MISSING_CHUNK = auto()


@dataclass
class DownloadTask:
    product_id: str

@dataclass
class DownloadTask1(DownloadTask):
    flags: list 
    size: int
    offset: int
    checksum: str
    destination: str
    file_path: str

@dataclass
class DownloadTask2(DownloadTask):
    compressed_sum: str
    memory_segment: MemorySegment


@dataclass
class WriterTask:
    # Root directory of game files
    destination: str
    # File path from manifest
    file_path: str
    flags: TaskFlag

    hash: Optional[str] = None
    size: Optional[int] = None
    shared_memory: Optional[MemorySegment] = None
    old_file: Optional[str] = None
    old_offset: Optional[int] = None


@dataclass
class DownloadTaskResult:
    success: bool
    fail_reason: Optional[FailReason]
    task: Union[DownloadTask2, DownloadTask1]
    download_size: Optional[int] = None
    decompressed_size: Optional[int] = None

@dataclass
class WriterTaskResult:
    success: bool
    task: Union[WriterTask, TerminateWorker]


class Download(Process):
    def __init__(self, shared_memory, download_queue, results_queue, shared_secure_links):
        self.shared_memory = SharedMemory(name=shared_memory)
        self.download_queue: Queue = download_queue
        self.results_queue: Queue = results_queue
        self.secure_links: dict = shared_secure_links
        self.session = requests.session()
        self.early_exit = False
        super().__init__()

    def run(self):
        while not self.early_exit:
            try:
                task: Union[DownloadTask1, DownloadTask2, TerminateWorker] = self.download_queue.get(timeout=2)
            except Empty:
               continue 

            if isinstance(task, TerminateWorker):
                break

            if type(task) == DownloadTask2:
                self.v2(task)
            elif type(task) == DownloadTask1:
                self.v1(task)

        self.shared_memory.close()

    def v2(self, task: DownloadTask2):
        
        urls = self.secure_links[task.product_id]

        compressed_md5 = task.compressed_sum

        endpoint = copy(urls[0])
        if task.product_id != 'redist':
            endpoint["parameters"]["path"] += f"/{dl_utils.galaxy_path(compressed_md5)}"
            url = dl_utils.merge_url_with_params(
                endpoint["url_format"], endpoint["parameters"]
            )
        else:
            endpoint["url"] += "/" + dl_utils.galaxy_path(compressed_md5)
            url = endpoint["url"]

        response = None
        try:
            response = self.session.get(url, timeout=5)
            response.raise_for_status()
        except Exception as e:
            print("Download failed", e)
            # Handle exception
            if response and response.status_code == 401:
                self.results_queue.put(DownloadTaskResult(False, FailReason.UNAUTHORIZED, task))
                return
            self.results_queue.put(DownloadTaskResult(False, FailReason.CHECKSUM, task))
            return 
        compressed_sum = hashlib.md5()
        download_size = 0
        decompressed_size = 0
        try:
            chunk = response.content
            download_size = len(chunk)
            compressed_sum.update(chunk)
            data = zlib.decompress(chunk)
            decompressed_size = len(data)
            del chunk
            self.shared_memory.buf[task.memory_segment.offset:decompressed_size+task.memory_segment.offset] = data

        except Exception as e:
            print("ERROR", e)
            self.results_queue.put(DownloadTaskResult(False, FailReason.UNKNOWN, task))
            return 

        if compressed_sum.hexdigest() != compressed_md5:
            self.results_queue.put(DownloadTaskResult(False, FailReason.CHECKSUM, task))
            return 

        self.results_queue.put(DownloadTaskResult(True, None, task, download_size=download_size, decompressed_size=decompressed_size))
    
    def v1(self, task: DownloadTask1):
        destination = os.path.join(
            task.destination, task.file_path
        )

        destination = dl_utils.get_case_insensitive_name(task.destination, destination)
        dl_utils.prepare_location(os.path.split(destination)[0])

        if task.size == 0:
            self.results_queue.put(DownloadTaskResult(True, None, task, None))
            return 

        urls = self.secure_links[task.product_id]
        md5 = task.checksum

        file_handle = open(destination, "wb")

        final_sum = hashlib.md5()

        endpoint = copy(urls[0])
        response = None
        endpoint["parameters"]["path"] += "/main.bin"
        url = dl_utils.merge_url_with_params(
            endpoint["url_format"], endpoint["parameters"]
        )
        range_header = dl_utils.get_range_header(task.offset, task.size)
        try:
            response = self.session.get(url, stream=True, timeout=5, headers={'Range': range_header})
            response.raise_for_status()
        except Exception as e:
            # Handle exception
            if response and response.status_code == 401:
                self.results_queue.put(DownloadTaskResult(False, FailReason.UNAUTHORIZED, task, None))
                return
            self.results_queue.put(DownloadTaskResult(False, FailReason.CHECKSUM, task, None))
            return 
         
        try:
            for chunk in response.iter_content(1024 * 1024):
                final_sum.update(chunk)
                file_handle.write(chunk)
        except Exception as e:
            print("ERROR", e)
            self.results_queue.put(DownloadTaskResult(False, FailReason.UNKNOWN, task, None))
            return 
        file_handle.close()

        if final_sum.hexdigest() != md5:
            self.results_queue.put(DownloadTaskResult(False, FailReason.CHECKSUM, task, None))
            return 

        self.results_queue.put(DownloadTaskResult(True, None, task, None))

class Writer(Process):
    def __init__(self, shared_memory, writer_queue, results_queue, cache):
        self.shared_memory = SharedMemory(name=shared_memory)
        self.cache = cache
        self.writer_queue: Queue = writer_queue
        self.results_queue: Queue = results_queue
        self.early_exit = False
        super().__init__()

    def run(self):
        file_handle = None
        current_file = ''

        while not self.early_exit:
            try:
                task: Union[WriterTask, TerminateWorker] = self.writer_queue.get(timeout=2)
            except Empty:
                continue

            if isinstance(task, TerminateWorker):
                self.results_queue.put(WriterTaskResult(True, task))
                break
            
            task_path = dl_utils.get_case_insensitive_name(task.destination, os.path.join(task.destination, task.file_path))
            split_path = os.path.split(task_path)
            if split_path[0] and not os.path.exists(split_path[0]):
                dl_utils.prepare_location(split_path[0])

            if task.flags & TaskFlag.CREATE_FILE:
                open(task_path, 'a').close()
                self.results_queue.put(WriterTaskResult(True, task))
                continue

            elif task.flags & TaskFlag.OPEN_FILE:
                if file_handle:
                    print("Opening on unclosed file")
                    file_handle.close()
                file_handle = open(task_path, 'wb')
                current_file = task_path

                self.results_queue.put(WriterTaskResult(True, task))
                continue
            elif task.flags & TaskFlag.CLOSE_FILE:
                if file_handle:
                    file_handle.close()
                    file_handle = None
                self.results_queue.put(WriterTaskResult(True, task))
                continue

            elif task.flags & TaskFlag.RENAME_FILE:
                if file_handle:
                    print("Renaming on unclosed file")
                    file_handle.close()
                    file_handle = None

                if not task.old_file:
                    # if this ever happens....
                    self.results_queue.put(WriterTaskResult(False, task))
                    continue
                
                if task.flags & TaskFlag.DELETE_FILE:
                    try:
                        os.remove(task_path)
                    except OSError as e:
                        self.results_queue.put(WriterTaskResult(False, task))
                        continue
                try:
                    os.rename(dl_utils.get_case_insensitive_name(task.destination, os.path.join(task.destination, task.old_file)),task_path)
                except OSError as e:
                    self.results_queue.put(WriterTaskResult(False, task))
                    continue

                self.results_queue.put(WriterTaskResult(True, task))
                continue
            
            elif task.flags & TaskFlag.DELETE_FILE:
                if file_handle:
                    print("Deleting on unclosed file")
                    file_handle.close()
                    file_handle = None
                try:
                    os.remove(task_path)
                except OSError as e:
                    self.results_queue.put(WriterTaskResult(False, task))
                    continue

            elif task.flags & TaskFlag.MAKE_EXE:
                if file_handle and task.file_path == current_file:
                    print("Making exe on unclosed file")
                    file_handle.close()
                    file_handle = None
                if sys.platform != 'win32':
                    try:
                        st = os.stat(task_path)
                        os.chmod(task_path, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                    except Exception as e:
                        self.results_queue.put(WriterTaskResult(False, task))
                        continue
                self.results_queue.put(WriterTaskResult(True, task))
                continue

            
            try:
                if task.shared_memory:
                    if not task.size:
                        print("No size")
                        self.results_queue.put(WriterTaskResult(False, task))
                        continue
                    offset = task.shared_memory.offset
                    end = offset + task.size
                    file_handle.write(self.shared_memory.buf[offset:end].tobytes())
                    if task.flags & TaskFlag.OFFLOAD_TO_CACHE and task.hash:
                        cache_file_path = os.path.join(self.cache, task.hash)
                        dl_utils.prepare_location(self.cache)
                        cache_file = open(cache_file_path, 'wb')
                        cache_file.write(self.shared_memory.buf[offset:end].tobytes())
                        cache_file.close()
                elif task.old_file:
                    if not task.size:
                        print("No size")
                        self.results_queue.put(WriterTaskResult(False, task))
                        continue
                    old_file_path = dl_utils.get_case_insensitive_name(task.destination, os.path.join(task.destination, task.old_file))
                    old_file_handle = open(old_file_path, "rb")
                    if task.old_offset:
                        old_file_handle.seek(task.old_offset)
                    file_handle.write(old_file_handle.read(task.size))
                    old_file_handle.close()



            except Exception as e:
                print("Writer exception", e)
                self.results_queue.put(WriterTaskResult(False, task))
            else:
                self.results_queue.put(WriterTaskResult(True, task))

        
        self.shared_memory.close()
        shutil.rmtree(self.cache, ignore_errors=True)

