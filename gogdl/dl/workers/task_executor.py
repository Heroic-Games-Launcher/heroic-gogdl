import os
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


class TaskType(Enum):
    EXIT = 0
    DOWNLOAD_V1 = auto()
    DOWNLOAD_V2 = auto()
    ASSEMBLE = auto()
    EXTRACT = auto()
    CREATE = auto()


class FailReason(Enum):
    UNKNOWN = 0
    CHECKSUM = auto()
    CONNECTION = auto()
    UNAUTHORIZED = auto()

    MISSING_CHUNK = auto()


@dataclass
class Task:
    type: TaskType
    product_id: str
    flags: list 

@dataclass
class DownloadTask1(Task):
    size: int
    offset: int
    checksum: str
    destination: str
    file_path: str

@dataclass
class DownloadTask2(Task):
    chunk_index: int
    chunk_data: dict
    destination: str
    file_path: str
    dependency: bool


@dataclass
class WriterTask(Task):
    destination: str
    file_path: str
    context: Any


@dataclass
class TaskResult:
    success: bool
    fail_reason: Optional[FailReason]
    task: Union[DownloadTask2, DownloadTask1, WriterTask]
    context: Any


class Download(Process):
    def __init__(self, download_queue, results_queue, shared_secure_links):
        self.download_queue: Queue = download_queue
        self.results_queue: Queue = results_queue
        self.secure_links: dict = shared_secure_links
        self.session = requests.session()
        self.early_exit = False
        super().__init__()

    def run(self):
        while not self.early_exit:
            task: Union[DownloadTask1, DownloadTask2] = self.download_queue.get()

            if task.type == TaskType.EXIT:
                break

            if type(task) == DownloadTask2:
                self.v2(task)
            elif type(task) == DownloadTask1:
                self.v1(task)

    def v2(self, task: DownloadTask2):
        destination = os.path.join(
            task.destination, task.file_path + f".tmp{task.chunk_index}"
        )

        destination = dl_utils.get_case_insensitive_name(task.destination, destination)
        dl_utils.prepare_location(os.path.split(destination)[0])

        urls = self.secure_links[task.product_id]

        compressed_md5 = task.chunk_data["compressedMd5"]
        md5 = task.chunk_data["md5"]

        if os.path.exists(destination):
            file_handle = open(destination, "rb")
            test_md5 = hashlib.md5(file_handle.read())
            file_handle.close()

            if test_md5.hexdigest() == md5:
                self.results_queue.put(TaskResult(True, None, task, None))
                return
                

        file_handle = open(destination, "wb")

        endpoint = copy(urls[0])
        if not task.dependency:
            endpoint["parameters"]["path"] += f"/{dl_utils.galaxy_path(compressed_md5)}"
            url = dl_utils.merge_url_with_params(
                endpoint["url_format"], endpoint["parameters"]
            )
        else:
            endpoint["url"] += "/" + dl_utils.galaxy_path(compressed_md5)
            url = endpoint["url"]

        response = None
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
        except Exception as e:
            # Handle exception
            if response and response.status_code == 401:
                self.results_queue.put(TaskResult(False, FailReason.UNAUTHORIZED, task, None))
                return
            self.results_queue.put(TaskResult(False, FailReason.CHECKSUM, task, None))
            return 
        compressed_sum = hashlib.md5()
        final_sum = hashlib.md5()
        decompressor = zlib.decompressobj(15)
        try:
            chunk = response.content
            compressed_sum.update(chunk)
            data = decompressor.decompress(chunk)
            final_sum.update(data)
            file_handle.write(data)

        except Exception as e:
            print("ERROR", e)
            self.results_queue.put(TaskResult(False, FailReason.UNKNOWN, task, None))
            return 

        file_handle.close()

        if compressed_sum.hexdigest() != compressed_md5 or final_sum.hexdigest() != md5:
            self.results_queue.put(TaskResult(False, FailReason.CHECKSUM, task, None))
            return 

        self.results_queue.put(TaskResult(True, None, task, None))
    
    def v1(self, task: DownloadTask1):
        destination = os.path.join(
            task.destination, task.file_path
        )

        destination = dl_utils.get_case_insensitive_name(task.destination, destination)
        dl_utils.prepare_location(os.path.split(destination)[0])

        if task.size == 0:
            self.results_queue.put(TaskResult(True, None, task, None))
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
            response = self.session.get(url, stream=True, timeout=30, headers={'Range': range_header})
            response.raise_for_status()
        except Exception as e:
            # Handle exception
            if response and response.status_code == 401:
                self.results_queue.put(TaskResult(False, FailReason.UNAUTHORIZED, task, None))
                return
            self.results_queue.put(TaskResult(False, FailReason.CHECKSUM, task, None))
            return 
         
        try:
            for chunk in response.iter_content(1024 * 1024):
                final_sum.update(chunk)
                file_handle.write(chunk)
        except Exception as e:
            print("ERROR", e)
            self.results_queue.put(TaskResult(False, FailReason.UNKNOWN, task, None))
            return 
        file_handle.close()

        if final_sum.hexdigest() != md5:
            self.results_queue.put(TaskResult(False, FailReason.CHECKSUM, task, None))
            return 

        self.results_queue.put(TaskResult(True, None, task, None))

class Writer(Process):
    def __init__(self, writer_queue, results_queue):
        self.writer_queue: Queue = writer_queue
        self.results_queue: Queue = results_queue
        self.early_exit = False
        super().__init__()

    def run(self):
        while not self.early_exit:
            task: WriterTask = self.writer_queue.get()

            if task.type == TaskType.EXIT:
                break

            if task.type == TaskType.EXTRACT:
                index, chunk = task.context
                source = os.path.join(task.destination, task.file_path)
                if not os.path.exists(source):
                    source = dl_utils.get_case_insensitive_name(task.destination, source)
                target = source + f".tmp{index}" 

                file_handle = open(source, "rb")
                destination_handle = open(target, "wb")
                file_handle.seek(chunk['old_offset'])
                data = file_handle.read(chunk['size'])
                md5 = chunk['md5']
                calculated = hashlib.md5(data).hexdigest()
                valid = md5 == calculated
                
                if not valid:
                    # Handle invalid chunk
                    print("Failed to read chunk", md5, calculated)
                    self.results_queue.put(TaskResult(False, FailReason.CHECKSUM, task, None))
                    continue
                
                destination_handle.write(data)

                destination_handle.close()
                file_handle.close()
                self.results_queue.put(TaskResult(True, None, task, None))
                continue


            destination = os.path.join(task.destination, task.file_path)
            destination = dl_utils.get_case_insensitive_name(task.destination, destination)
            dl_utils.prepare_location(os.path.split(destination)[0])

            if task.type == TaskType.CREATE:
                if not os.path.exists(destination):
                    open(destination, "x").close()
                self.results_queue.put(TaskResult(True, None, task, None))
                continue
            
            failed = False
            for i in range(task.context[0]):
                tmp_destination = dl_utils.get_case_insensitive_name(task.destination, destination+f".tmp{i}")
                if not os.path.exists(tmp_destination):
                    # This shouldn't happen
                    self.results_queue.put(TaskResult(False, FailReason.MISSING_CHUNK, task, (i, task.context[1])))
                    failed = True
                    continue
            if failed:
                continue

            handle_path = destination
            # Complex patching boolean
            if task.context[1]:
                handle_path = destination+".new"
            handle_path = dl_utils.get_case_insensitive_name(task.destination, handle_path)
            # Number of chunks
            if task.context[0] == 1:
                os.rename(dl_utils.get_case_insensitive_name(task.destination, destination+f".tmp0"), handle_path)

                if "executable" in task.flags and sys.platform != 'win32':
                    mode = os.stat(handle_path).st_mode
                    os.chmod(handle_path, mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                self.results_queue.put(TaskResult(True, None, task, None))
                continue
            
            # Whether it's patching
            if not task.context[1] and os.path.exists(destination):
                os.remove(destination)
            
            file_handle = open(handle_path, "wb")

            for i in range(task.context[0]):
                tmp_destination = dl_utils.get_case_insensitive_name(task.destination ,destination+f".tmp{i}")
                reader = open(tmp_destination, "rb")
                data = reader.read()
                reader.close()
                os.remove(tmp_destination)
                file_handle.write(data)

            file_handle.close()

            if "executable" in task.flags and sys.platform != 'win32':
                mode = os.stat(handle_path).st_mode
                os.chmod(handle_path, mode | stat.S_IEXEC | stat.S_IXUSR | stat.S_IXGRP)
                
            self.results_queue.put(TaskResult(True, None, task, None))
