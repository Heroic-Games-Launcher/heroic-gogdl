from threading import Thread
from gogdl.dl import dl_utils
from gogdl.dl.objects.v2 import DepotDirectory
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
    def __init__(self, data, path, api_handler, gameId, progress, endpoints):
        self.data = data
        self.path = path
        self.api_handler = api_handler
        self.progress = progress
        self.gameId = gameId
        self.completed = False
        self.endpoints = endpoints
        self.logger = logging.getLogger("DOWNLOAD_WORKER")
        self.downloaded_size = 0

    def work(self):
        pass

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
                    self.progress.update_download_speed(len(data))
                    f.write(data)
            f.close()
            isExisting = os.path.exists(path)
            if isExisting and (
                dl_utils.calculate_sum(path, hashlib.md5) != compressed_sum
            ):
                self.logger.warning(
                    f"Checksums dismatch for compressed chunk of {path}"
                )
                if isExisting:
                    os.remove(path)
                self.get_file(url, path, compressed_sum, decompressed_sum, index)

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
