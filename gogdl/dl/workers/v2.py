from gogdl.dl import dl_utils
from copy import copy
import hashlib
import zlib
import logging
import os
from gogdl.dl.objects import v2


class DLWorker:
    def __init__(self, data, path, api_handler, game_id, endpoint, progress=None):
        self.data = data
        self.path = path
        self.api_handler = api_handler
        self.progress = progress
        self.gameId = game_id
        self.completed = False
        self.endpoint = endpoint
        self.logger = logging.getLogger("DOWNLOAD_WORKER")

        self.retries = 0
        self.is_dependency = False

    def work(self):
        item_path = os.path.join(self.path, self.data.path.lstrip("/\\"))

        if type(self.data) == v2.DepotDirectory:
            os.makedirs(item_path, exist_ok=True)
            return

        if self.verify_file(item_path):
            return
        elif os.path.exists(item_path):
            os.remove(item_path)
        dl_utils.prepare_location(dl_utils.parent_dir(item_path))
        for index, chunk in enumerate(self.data.chunks):
            compressed_md5 = chunk["compressedMd5"]
            md5 = chunk["md5"]

            if not self.is_dependency:
                parameters = copy(self.endpoint["parameters"])
                parameters["path"] += "/" + dl_utils.galaxy_path(compressed_md5)
                url = dl_utils.merge_url_with_params(
                    self.endpoint["url_format"], parameters
                )
            else:
                copied_endpoint = copy(self.endpoint)
                copied_endpoint["url"] += "/" + dl_utils.galaxy_path(compressed_md5)
                url = copied_endpoint["url"]

            self.get_file(url, item_path + f".part{index}", compressed_md5, md5, index)

        target_file = open(item_path, "ab")
        for index, chunk in enumerate(self.data.chunks):
            compressed_md5 = chunk["compressedMd5"]
            md5 = chunk["md5"]
            chunk_file = open(item_path + f".part{index}", "rb")

            decompression = zlib.decompressobj(15)

            while data := chunk_file.read(10 * 1024 * 1024):
                target_file.write(decompression.decompress(data))

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
        is_existing = os.path.exists(path)
        if is_existing:
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
            is_existing = os.path.exists(path)
            if is_existing and (
                    dl_utils.calculate_sum(path, hashlib.md5) != compressed_sum
            ):
                self.logger.warning(
                    f"Checksums missmatch for compressed chunk of {path}"
                )
                if is_existing:
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
            elif len(self.data.chunks) == 1:
                # In case if there are sha256 sums in chunks
                if "sha256" in self.data.chunks[0]:
                    calculated = dl_utils.calculate_sum(item_path, hashlib.sha256)
                    should_be = self.data.chunks[0]["sha256"]
                elif "md5" in self.data.chunks[0]:
                    calculated = dl_utils.calculate_sum(item_path, hashlib.md5)
                    should_be = self.data.chunks[0]["md5"]
            elif len(self.data.chunks) == 0:
                return True
            return calculated == should_be
        else:
            return False
