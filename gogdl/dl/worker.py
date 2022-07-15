from threading import Thread
from gogdl.dl import objects, dl_utils
from gogdl.dl.objects import DepotDirectory
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

    def do_stuff(self, is_dependency=False):
        self.is_dependency = is_dependency
        if os_platform == "win32":
            self.data.path = self.data.path.replace("/", "\\")
        else:
            self.data.path = self.data.path.replace("\\", os.sep)
        item_path = os.path.join(self.path, self.data.path)
        if type(self.data) == DepotDirectory:
            dl_utils.prepare_location(item_path)
            return
        if self.data.flags and "support" in self.data.flags:
            item_path = os.path.join(self.path, "support", self.gameId, self.data.path)
        if type(self.data) == DepotDirectory:
            dl_utils.prepare_location(item_path)
            return
        # Fix for https://github.com/Heroic-Games-Launcher/heroic-gogdl/issues/3
        if len(self.data.chunks) == 0:
            directory, file_name = os.path.split(item_path)
            dl_utils.prepare_location(directory)
            open(item_path, "w").close()
            return

        if self.verify_file(item_path):
            size = 0
            for chunk in self.data.chunks:
                size += chunk["compressedSize"]
            self.progress.update_downloaded_size(size)
            self.completed = True
            return

        if os.path.exists(item_path):
            os.remove(item_path)

        for index in range(len(self.data.chunks)):
            chunk = self.data.chunks[index]
            compressed_md5 = chunk["compressedMd5"]
            md5 = chunk["md5"]
            self.downloaded_size = chunk["compressedSize"]
            if is_dependency:
                url = dl_utils.get_dependency_link(
                    self.api_handler, dl_utils.galaxy_path(compressed_md5)
                )
            else:
                endpoint = self.endpoints[self.data.product_id]
                parameters = copy(endpoint["parameters"])
                parameters["path"] += "/" + dl_utils.galaxy_path(compressed_md5)
                url = dl_utils.merge_url_with_params(endpoint["url_format"], parameters)
            download_path = os.path.join(item_path + f".tmp{index}")
            dl_utils.prepare_location(dl_utils.parent_dir(download_path), self.logger)
            self.get_file(url, download_path, compressed_md5, md5, index)
            self.progress.update_downloaded_size(self.downloaded_size)

        for index in range(len(self.data.chunks)):
            self.decompress_file(item_path + f".tmp{index}", item_path)

        if (
            self.data.flags
            and ("executable" in self.data.flags)
            and os_platform != "win32"
        ):
            file_stats = os.stat(item_path)
            permissions = file_stats.st_mode | stat.S_IEXEC
            os.chmod(item_path, permissions)

        if self.data.path.startswith("app"):
            file_path = os.path.join(self.path, "support", self.gameId, self.data.path)

            dest_path = self.data.path.replace("app", self.path)

            try:
                shutil.copy(file_path, dest_path)
            except:
                pass

        self.completed = True

    def decompress_file(self, compressed, decompressed):
        if os.path.exists(compressed):
            file = open(compressed, "rb")
            dc = zlib.decompress(file.read(), 15)
            f = open(decompressed, "ab")
            f.write(dc)
            f.close()
            file.close()
            os.remove(compressed)
        else:
            if self.is_dependency:
                url = dl_utils.get_dependency_link(
                    self.api_handler, dl_utils.galaxy_path(compressed_md5)
                )
            else:
                url = dl_utils.get_secure_link(
                    self.api_handler, dl_utils.galaxy_path(compressed_md5), self.gameId
                )
            dl_utils.prepare_location(dl_utils.parent_dir(compressed), self.logger)
            self.get_file(url, compressed, compressed_md5, md5, index)
            return self.decompress_file(compressed, decompressed)

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


class DLWorkerV1:
    def __init__(self, data, path, download_link, api_handler, gameId, progressbar):
        self.data = data
        self.path = path
        self.api_handler = api_handler
        self.progress = progressbar
        self.gameId = gameId
        self.completed = False
        self.logger = logging.getLogger("DOWNLOAD_WORKER_V1")
        self.download_link = download_link
        self.downloaded_size = 0

    def do_stuff(self, is_dependency=False):
        if self.data["path"].startswith("/"):
            self.data["path"] = self.data["path"][1:]
        item_path = os.path.join(self.path, self.data["path"])
        if os_platform == "win32":
            item_path = item_path.replace("/", os.sep)
        else:
            item_path = item_path.replace("\\", os.sep)

        if self.data.get("support") == True:
            item_path = os.path.join(self.path, "support", self.data["path"])
        if self.verify_file(item_path):
            self.completed = True
            if not is_dependency:
                self.progress.update_downloaded_size(int(self.data["size"]))
            return
        else:
            if os.path.exists(item_path):
                os.remove(item_path)
        dl_utils.prepare_location(dl_utils.parent_dir(item_path), self.logger)
        self.get_file(item_path)
        if not is_dependency:
            self.progress.update_downloaded_size(int(self.data["size"]))

    def get_file(self, item_path):
        headers = {
            "Range": dl_utils.get_range_header(self.data["offset"], self.data["size"])
        }
        with open(item_path, "ab") as f:
            response = self.api_handler.session.get(
                self.download_link, headers=headers, stream=True, allow_redirects=True
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
            if os.path.exists(item_path):
                if not self.verify_file(item_path):
                    self.logger.warning(f"Checksums dismatch for file {item_path}")
                    os.remove(item_path)
                    self.get_file(item_path)

    def verify_file(self, item_path):
        if os.path.exists(item_path):
            calculated = dl_utils.calculate_sum(item_path, hashlib.md5)
            should_be = self.data["hash"]
            return calculated == should_be
        return False
