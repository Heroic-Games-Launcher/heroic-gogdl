from threading import Thread
from gogdl.dl import objects, dl_utils
import hashlib
import zlib
import time
import logging
import os


class DLWorker():
    def __init__(self, data, path, api_handler, gameId, submit_downloaded_size):
        self.data = data
        self.path = path
        self.api_handler = api_handler
        self.submit_downloaded_size = submit_downloaded_size
        self.gameId = gameId
        self.completed = False
        self.logger = logging.getLogger("DOWNLOAD_WORKER")
        self.downloaded_size = 0
        
    def do_stuff(self, is_dependency=False):
        item_path = os.path.join(self.path, self.data.path)
        if self.verify_file(item_path):
            size = 0
            for chunk in self.data.chunks:
                size += chunk['compressedSize']
            self.submit_downloaded_size(size)
            self.completed = True
            return
        if os.path.exists(item_path):
            os.remove(item_path)
        for index in range(len(self.data.chunks)):
            chunk = self.data.chunks[index]
            compressed_md5 = chunk['compressedMd5']
            md5 = chunk['md5']
            self.downloaded_size = chunk['compressedSize']
            if is_dependency:
                url = dl_utils.get_dependency_link(self.api_handler, dl_utils.galaxy_path(compressed_md5))
            else:
                url = dl_utils.get_secure_link(self.api_handler, dl_utils.galaxy_path(compressed_md5), self.gameId)
            download_path = os.path.join(
                self.path, self.data.path+f'.tmp{index}')
            dl_utils.prepare_location(
                dl_utils.parent_dir(download_path), self.logger)
            self.get_file(url, download_path, compressed_md5, md5, index)
            self.submit_downloaded_size(self.downloaded_size)
        for index in range(len(self.data.chunks)):
            path = os.path.join(self.path, self.data.path)
            self.decompress_file(path+f'.tmp{index}', path)
        self.completed = True


    def decompress_file(self, compressed, decompressed):
        if os.path.exists(compressed):
            file = open(compressed, 'rb')
            dc = zlib.decompress(file.read(), 15)
            f = open(decompressed, 'ab')
            f.write(dc)
            f.close()
            file.close()
            os.remove(compressed)

    def get_file(self, url, path, compressed_sum='', decompressed_sum='', index=0):
        isExisting = os.path.exists(path)
        if isExisting:
            os.remove(path)
        with open(path, 'ab') as f:
            response = self.api_handler.session.get(
                url, stream=True, allow_redirects=True)
            total = response.headers.get('Content-Length')
            if total is None:
                f.write(response.content)
            else:
                total = int(total)
                for data in response.iter_content(chunk_size=max(int(total/1000), 1024*1024)):
                    f.write(data)
            f.close()
            isExisting = os.path.exists(path)
            if isExisting and (dl_utils.calculate_sum(path, hashlib.md5) != compressed_sum):
                self.logger.warning(
                    f'Checksums dismatch for compressed chunk of {path}')
                if isExisting:
                    os.remove(path)
                self.get_file(url, path, compressed_sum,
                              decompressed_sum, index)

    def verify_file(self, item_path):
        if os.path.exists(item_path):
            calculated = None
            should_be = None
            if len(self.data.chunks) > 1:
                if data.md5:
                    should_be = data.md5
                    calculated = dl_utils.calculate_sum(item_path, hashlib.md5)
                elif data.sha256:
                    should_be = data.sha256
                    calculated = dl_utils.calculate_sum(item_path, hashlib.sha256)
            else:
                calculated = dl_utils.calculate_sum(item_path, hashlib.md5)
                should_be = self.data.chunks[0]['md5']
            return calculated == should_be
        else:
            return False