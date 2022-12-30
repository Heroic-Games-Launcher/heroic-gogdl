import os
import zlib

from gogdl.dl.objects.linux import LocalFile


class DLWorker:
    def __init__(self, file_data, path):
        self.data = file_data
        self.install_path = path
        self.file_path = self.data.file_name.replace("data/noarch", self.install_path)

        self.retries = 0

    def verify(self):
        file_handle = open(self.file_path, 'rb')
        crc = 0
        while data := file_handle.read(1024 * 1024):
            crc = zlib.crc32(data, crc)

        return crc == self.data.crc32

    def work(self, installer_handler):
        if os.path.exists(self.file_path):
            if self.verify():
                return

        file_permissions = bin(int.from_bytes(self.data.ext_file_attrs, "little"))[9:][:9]

        # Load local file header
        file_data = installer_handler.get_bytes_from_file(
            from_b=self.data.relative_local_file_offset,
            size=30,
        )
        local_file = LocalFile.from_bytes(
            file_data,
            self.data.relative_local_file_offset,
            installer_handler,  # Passsing in handler to be able to pull more data
        )
        local_file.relative_local_file_offset = self.data.relative_local_file_offset

        directory, name = os.path.split(self.file_path)
        os.makedirs(directory, exist_ok=True)

        response = local_file.load_data(installer_handler)
        total = response.headers.get("Content-Length")

        with open(self.file_path + ".tmp", "wb") as f:
            if total is None:
                f.write(response.content)
            else:
                total = int(total)
                for data in response.iter_content(
                        chunk_size=max(int(total / 1000), 1024 * 1024)
                ):
                    f.write(data)

            f.close()

        with open(self.file_path, "wb") as f:
            tmp_handle = open(self.file_path + ".tmp", 'rb')
            decompressor = zlib.decompressobj(-15)

            if local_file.compression_method == 8:
                while stream := tmp_handle.read(1024 * 1024):
                    decompressed = decompressor.decompress(stream)
                    f.write(decompressed)
                f.flush()
                f.close()
            elif local_file.compression_method == 0:
                tmp_handle.close()
                f.close()
                os.rename(self.file_path + ".tmp", self.file_path)
            else:
                print("Unsupported compression method", local_file.compression_method)

        if os.path.exists(self.file_path + ".tmp"):
            os.remove(self.file_path + ".tmp")

        if not self.verify():
            if self.retries < 3:
                self.retries += 1
                os.remove(self.file_path)
                self.work(installer_handler)
                return

        os.chmod(self.file_path, int(f"0b{file_permissions}", base=0))
