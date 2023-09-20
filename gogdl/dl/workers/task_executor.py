from multiprocessing.shared_memory import SharedMemory
import os
from queue import Empty
import shutil
import sys
import stat
import time
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
    offset: int
    size: int
    # This sum is not valid MD5 as it contains chunk id too
    # V1 doesn't support chunks, this is sort of forceful way to use them
    # in this algorithm
    compressed_sum: str
    memory_segment: MemorySegment

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
    old_destination: Optional[str] = None
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
    written: int = 0


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
                task: Union[DownloadTask1, DownloadTask2, TerminateWorker] = self.download_queue.get(timeout=1)
            except Empty:
               continue 

            if isinstance(task, TerminateWorker):
                break

            if type(task) == DownloadTask2:
                self.v2(task)
            elif type(task) == DownloadTask1:
                self.v1(task)

        self.session.close()
        self.shared_memory.close()

    def v2(self, task: DownloadTask2):
        retries = 5 
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
        while retries > 0:
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
            except Exception as e:
                print("Connection failed", e)
                # Handle exception
                if response and response.status_code == 401:
                    self.results_queue.put(DownloadTaskResult(False, FailReason.UNAUTHORIZED, task))
                    return
                retries -= 1
                time.sleep(2)
                continue
            break
        else:
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
        retries = 5
        urls = self.secure_links[task.product_id]

        endpoint = copy(urls[0])
        response = None
        endpoint["parameters"]["path"] += "/main.bin"
        url = dl_utils.merge_url_with_params(
            endpoint["url_format"], endpoint["parameters"]
        )
        range_header = dl_utils.get_range_header(task.offset, task.size)

        while retries > 0:
            try:
                response = self.session.get(url, timeout=10, headers={'Range': range_header})
                response.raise_for_status()
            except Exception as e:
                print("Connection failed", e)
                #Handle exception
                if response and response.status_code == 401:
                    self.results_queue.put(DownloadTaskResult(False, FailReason.UNAUTHORIZED, task))
                    return
                retries -= 1
                time.sleep(2)
                continue
            break
        else:
            self.results_queue.put(DownloadTaskResult(False, FailReason.CHECKSUM, task))
            return
         
        download_size = 0
        try:
            chunk = response.content
            download_size = len(chunk)
            self.shared_memory.buf[task.memory_segment.offset:download_size + task.memory_segment.offset] = chunk

        except Exception as e:
            print("ERROR", e)
            self.results_queue.put(DownloadTaskResult(False, FailReason.UNKNOWN, task))
            return 

        self.results_queue.put(DownloadTaskResult(True, None, task, download_size=download_size, decompressed_size=download_size))

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

            written = 0
            
            task_path = dl_utils.get_case_insensitive_name(os.path.join(task.destination, task.file_path))
            split_path = os.path.split(task_path)
            if split_path[0] and not os.path.exists(split_path[0]):
                dl_utils.prepare_location(split_path[0])

            if task.flags & TaskFlag.CREATE_FILE:
                open(task_path, 'a').close()
                self.results_queue.put(WriterTaskResult(True, task))
                continue

            elif task.flags & TaskFlag.CREATE_SYMLINK:
                dest = task.old_destination or task.destination
                # Windows will likely not have this ran ever
                if os.path.exists(task_path):
                    shutil.rmtree(task_path)
                os.symlink(dl_utils.get_case_insensitive_name(os.path.join(dest, task.old_file)), task_path)
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
            
            elif task.flags & TaskFlag.COPY_FILE:
                if file_handle and task.file_path == current_file:
                    print("Copy on unclosed file")
                    file_handle.close()
                    file_handle = None

                if not task.old_file:
                    # if this ever happens....
                    self.results_queue.put(WriterTaskResult(False, task))
                    continue

                dest = task.old_destination or task.destination
                try:
                    shutil.copy(dl_utils.get_case_insensitive_name(os.path.join(dest, task.old_file)), task_path)
                except shutil.SameFileError:
                    pass
                except Exception:
                    self.results_queue.put(WriterTaskResult(False, task))
                    continue
                self.results_queue.put(WriterTaskResult(True, task))
                continue

            elif task.flags & TaskFlag.RENAME_FILE:
                if file_handle and task.file_path == current_file:
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
                dest = task.old_destination or task.destination
                try:
                    os.rename(dl_utils.get_case_insensitive_name(os.path.join(dest, task.old_file)), task_path)
                except OSError as e:
                    self.results_queue.put(WriterTaskResult(False, task))
                    continue

                self.results_queue.put(WriterTaskResult(True, task))
                continue
            
            elif task.flags & TaskFlag.DELETE_FILE:
                if file_handle and task.file_path == current_file:
                    print("Deleting on unclosed file")
                    file_handle.close()
                    file_handle = None
                try:
                    if os.path.exists(task_path):
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
                    written += file_handle.write(self.shared_memory.buf[offset:end].tobytes())
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
                    dest = task.old_destination or task.destination
                    old_file_path = dl_utils.get_case_insensitive_name(os.path.join(dest, task.old_file))
                    old_file_handle = open(old_file_path, "rb")
                    if task.old_offset:
                        old_file_handle.seek(task.old_offset)
                    written += file_handle.write(old_file_handle.read(task.size))
                    old_file_handle.close()



            except Exception as e:
                print("Writer exception", e)
                self.results_queue.put(WriterTaskResult(False, task))
            else:
                self.results_queue.put(WriterTaskResult(True, task, written=written))

        
        self.shared_memory.close()
        shutil.rmtree(self.cache, ignore_errors=True)

