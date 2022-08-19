from threading import Thread
from gogdl.dl import dl_utils
from copy import copy
from sys import platform as os_platform
import shutil
import hashlib
import zlib
import time
import logging
import os
import stat


class DLWorker:
    def __init__(self, data, path, api_handler, gameId, endpoint, progress=None):
        self.data = data
        self.path = path
        self.api_handler = api_handler
        self.progress = progress
        self.gameId = gameId
        self.completed = False
        self.endpoint = endpoint
        self.logger = logging.getLogger("DOWNLOAD_WORKER")

        self.retries = 0

    def work(self):
        item_path = os.path.join(self.path, self.data.path)

        if self.verify_file(item_path):
            return
        elif os.path.exists(item_path):
            os.remove(item_path)
        dl_utils.prepare_location(dl_utils.parent_dir(item_path))
        for index, chunk in enumerate(self.data.chunks):
            compressed_md5 = chunk["compressedMd5"]
            md5 = chunk["md5"]

            parameters = copy(self.endpoint["parameters"])
            parameters["path"] += "/" + dl_utils.galaxy_path(compressed_md5)
            url = dl_utils.merge_url_with_params(
                self.endpoint["url_format"], parameters
            )
            self.get_file(url, item_path + f".part{index}", compressed_md5, md5, index)

        target_file = open(item_path, "ab")
        # Decompress chunks and write append them to the file (This operation is safe since chunks are can have maximum 100MB)
        for index, chunk in enumerate(self.data.chunks):
            compressed_md5 = chunk["compressedMd5"]
            md5 = chunk["md5"]
            chunk_file = open(item_path + f".part{index}", "rb")
            data = chunk_file.read()
            target_file.write(zlib.decompress(data))
            chunk_file.close()
            os.remove(item_path + f".part{index}")

        target_file.close()
        self.retries = 0

        if not self.verify_file(item_path):
            if self.retries < 3:
                self.retries += 1
                self.work()
            else:
                self.logger.error(f"Failed to download file properly {item_path}")
                return

    def get_file(self, url, path, compressed_sum, decompressed_sum, index=0):
        isExisting = os.path.exists(path)
        if isExisting:
            if dl_utils.calculate_sum(path, hashlib.md5) != compressed_sum:
                os.remove(path)
            else:
                return
        with open(path, "ab") as f:
            response = self.api_handler.session.get(
                url, stream=True, allow_redirects=True
            )
            total = response.headers.get("Content-Length")
            if total is None:
                f.write(response.content)
            else:
                total = int(total)
                for data in response.iter_content(
                    chunk_size=max(int(total / 1000), 1024 * 1024)
                ):
                    f.write(data)
            f.close()
            isExisting = os.path.exists(path)
            if isExisting and (
                dl_utils.calculate_sum(path, hashlib.md5) != compressed_sum
            ):
                self.logger.warning(
                    f"Checksums missmatch for compressed chunk of {path}"
                )
                if isExisting:
                    os.remove(path)
                if self.retries < 5:
                    self.retries += 1
                    self.get_file(url, path, compressed_sum, decompressed_sum, index)
                else:
                    self.logger.error(
                        "Worker exceeded max retries and file is still corrupted "
                        + path
                    )
                    raise Exception("Panic")

    def verify_file(self, item_path):
        if os.path.exists(item_path):
            calculated = None
            should_be = None
            if len(self.data.chunks) > 1:
                if self.data.md5:
                    should_be = self.data.md5
                    calculated = dl_utils.calculate_sum(item_path, hashlib.md5)
                elif self.data.sha256:
                    should_be = self.data.sha256
                    calculated = dl_utils.calculate_sum(item_path, hashlib.sha256)
            else:
                # In case if there are sha256 sums in chunks
                if "sha256" in self.data.chunks[0]:
                    calculated = dl_utils.calculate_sum(item_path, hashlib.sha256)
                    should_be = self.data.chunks[0]["sha256"]
                elif "md5" in self.data.chunks[0]:
                    calculated = dl_utils.calculate_sum(item_path, hashlib.md5)
                    should_be = self.data.chunks[0]["md5"]
            return calculated == should_be
        else:
            return False
