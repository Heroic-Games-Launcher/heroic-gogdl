import os
import sys
import stat
import requests
import zlib
import hashlib
from copy import copy
from gogdl.dl import dl_utils
from dataclasses import dataclass
from enum import Enum, auto
from multiprocessing import Process, Queue


class TaskType(Enum):
    EXIT = 0
    DOWNLOAD = auto()
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
class DownloadTask(Task):
    chunk_index: int
    chunk_data: dict
    destination: str
    file_path: str
    decompress_while_downloading: bool
    dependency: bool


@dataclass
class WriterTask(Task):
    destination: str
    file_path: str
    context: any


@dataclass
class TaskResult:
    success: bool
    fail_reason: FailReason
    task: Task
    context = None


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
            task: DownloadTask = self.download_queue.get()

            if task.type == TaskType.EXIT:
                break

            destination = os.path.join(
                task.destination, task.file_path + f".tmp{task.chunk_index}"
            )

            dl_utils.prepare_location(os.path.split(destination)[0])

            urls = self.secure_links[task.product_id]

            compressed_md5 = task.chunk_data["compressedMd5"]
            md5 = task.chunk_data["md5"]

            if os.path.exists(destination):
                file_handle = open(destination, "rb")
                test_md5 = hashlib.md5(file_handle.read())
                file_handle.close()

                if test_md5.hexdigest() == md5:
                    self.results_queue.put(TaskResult(True, None, task))
                    continue
                

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

            try:
                response = self.session.get(url, stream=True, timeout=10)
                response.raise_for_status()
            except Exception as e:
                # Handle exception
                if response.status_code == 401:
                    self.results_queue.put(TaskResult(False, FailReason.UNAUTHORIZED, task))
                    continue
                self.results_queue.put(TaskResult(False, FailReason.CHECKSUM, task))
                continue
            decompressor = None
            compressed_sum = hashlib.md5()
            final_sum = hashlib.md5()
            if task.decompress_while_downloading:
                decompressor = zlib.decompressobj(15)
            try:
                for chunk in response.iter_content(1024 * 1024):
                    compressed_sum.update(chunk)
                    if decompressor:
                        data = decompressor.decompress(chunk)
                        final_sum.update(data)
                        file_handle.write(data)
                    else:
                        file_handle.write(chunk)

            except Exception as e:
                print("ERROR", e)
                self.results_queue.put(TaskResult(False, FailReason.UNKNOWN, task))
                continue 

            file_handle.close()

            if compressed_sum.hexdigest() != compressed_md5 or (
                task.decompress_while_downloading and final_sum.hexdigest() != md5
            ):
                self.results_queue.put(TaskResult(False, FailReason.CHECKSUM, task))
                continue

            self.results_queue.put(TaskResult(True, None, task))


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
                    self.results_queue.put(TaskResult(False, FailReason.CHECKSUM, task))
                    continue
                
                destination_handle.write(data)

                destination_handle.close()
                file_handle.close()
                self.results_queue.put(TaskResult(True, None, task))
                continue


            destination = os.path.join(task.destination, task.file_path)
            dl_utils.prepare_location(os.path.split(destination)[0])

            if task.type == TaskType.CREATE:
                if not os.path.exists(destination):
                    open(destination, "x").close()
                self.results_queue.put(TaskResult(True, None, task))
                continue
            
            for i in range(task.context[0]):
                if not os.path.exists(destination+f".tmp{i}"):
                    # This shouldn't happen
                    print(f"Unable to put chunks together, file {i} is missing")
                    self.results_queue.put(TaskResult(False, FailReason.MISSING_CHUNK, task, i))
                    continue

            handle_path = destination
            # Complex patching boolean
            if task.context[1]:
                handle_path = destination+".new"

            # Number of chunks
            if task.context[0] == 1:
                os.rename(destination+f".tmp0",handle_path)

                if "executable" in task.flags and sys.platform != 'win32':
                    mode = os.stat(handle_path).st_mode
                    os.chmod(handle_path, mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                self.results_queue.put(TaskResult(True, None, task))
                continue
            
            # Whether it's patching
            if not task.context[1] and os.path.exists(destination):
                os.remove(destination)
            
            file_handle = open(handle_path, "wb")

            for i in range(task.context[0]):
                reader = open(destination+f".tmp{i}", "rb")
                data = reader.read()
                reader.close()
                os.remove(destination+f".tmp{i}")
                file_handle.write(data)

            file_handle.close()

            if "executable" in task.flags and sys.platform != 'win32':
                mode = os.stat(handle_path).st_mode
                os.chmod(handle_path, mode & stat.S_IEXEC & stat.S_IXUSR & stat.S_IXGRP)
                
            self.results_queue.put(TaskResult(True, None, task))
