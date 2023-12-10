END_OF_CENTRAL_DIRECTORY = b"\x50\x4b\x05\x06"
CENTRAL_DIRECTORY = b"\x50\x4b\x01\x02"
LOCAL_FILE_HEADER = b"\x50\x4b\x03\x04"

# ZIP64
ZIP_64_END_OF_CD_LOCATOR = b"\x50\x4b\x06\x07"
ZIP_64_END_OF_CD = b"\x50\x4b\x06\x06"

class LocalFile:
    def __init__(self) -> None:
        self.relative_local_file_offset: int
        self.version_needed: bytes 
        self.general_purpose_bit_flag: bytes
        self.compression_method: int
        self.last_modification_time: bytes
        self.last_modification_date: bytes
        self.crc32: bytes
        self.compressed_size: int
        self.uncompressed_size: int
        self.file_name_length: int
        self.extra_field_length: int
        self.file_name: str
        self.extra_field: bytes
        self.last_byte: int

    def load_data(self, handler):
        return handler.get_bytes_from_file(
            from_b=self.last_byte + self.relative_local_file_offset,
            size=self.compressed_size,
            raw_response=True
        )

    @classmethod
    def from_bytes(cls, data, offset, handler):
        local_file = cls()
        local_file.relative_local_file_offset = 0
        local_file.version_needed = data[4:6]
        local_file.general_purpose_bit_flag = data[6:8]
        local_file.compression_method = int.from_bytes(data[8:10], "little")
        local_file.last_modification_time = data[10:12]
        local_file.last_modification_date = data[12:14]
        local_file.crc32 = data[14:18]
        local_file.compressed_size = int.from_bytes(data[18:22], "little")
        local_file.uncompressed_size = int.from_bytes(data[22:26], "little")
        local_file.file_name_length = int.from_bytes(data[26:28], "little")
        local_file.extra_field_length = int.from_bytes(data[28:30], "little")

        extra_data = handler.get_bytes_from_file(
            from_b=30 + offset,
            size=local_file.file_name_length + local_file.extra_field_length,
        )

        local_file.file_name = bytes(
            extra_data[0: local_file.file_name_length]
        ).decode()

        local_file.extra_field = data[
                                 local_file.file_name_length: local_file.file_name_length
                                                              + local_file.extra_field_length
                                 ]
        local_file.last_byte = (
                local_file.file_name_length + local_file.extra_field_length + 30
        )
        return local_file

    def __str__(self):
        return f"\nCompressionMethod: {self.compression_method} \nFileNameLen: {self.file_name_length} \nFileName: {self.file_name} \nCompressedSize: {self.compressed_size} \nUncompressedSize: {self.uncompressed_size}"


class CentralDirectoryFile:
    def __init__(self, product):
        self.product = product
        self.version_made_by: bytes
        self.version_needed_to_extract: bytes
        self.general_purpose_bit_flag: bytes
        self.compression_method: int 
        self.last_modification_time: bytes
        self.last_modification_date: bytes
        self.crc32: int
        self.compressed_size: int
        self.uncompressed_size: int
        self.file_name_length: int
        self.extra_field_length: int
        self.file_comment_length: int
        self.disk_number_start: bytes
        self.int_file_attrs: bytes
        self.ext_file_attrs: bytes
        self.relative_local_file_offset: int
        self.file_name: str
        self.extra_field: bytes
        self.comment: bytes
        self.last_byte: int

    @classmethod
    def from_bytes(cls, data, product):
        cd_file = cls(product)

        cd_file.version_made_by = data[4:6]
        cd_file.version_needed_to_extract = data[6:8]
        cd_file.general_purpose_bit_flag = data[8:10]
        cd_file.compression_method = int.from_bytes(data[10:12], "little")
        cd_file.last_modification_time = data[12:14]
        cd_file.last_modification_date = data[14:16]
        cd_file.crc32 = int.from_bytes(data[16:20], "little")
        cd_file.compressed_size = int.from_bytes(data[20:24], "little")
        cd_file.uncompressed_size = int.from_bytes(data[24:28], "little")
        cd_file.file_name_length = int.from_bytes(data[28:30], "little")
        cd_file.extra_field_length = int.from_bytes(data[30:32], "little")
        cd_file.file_comment_length = int.from_bytes(data[32:34], "little")
        cd_file.disk_number_start = data[34:36]
        cd_file.int_file_attrs = data[36:38]
        cd_file.ext_file_attrs = data[38:42]
        cd_file.relative_local_file_offset = int.from_bytes(data[42:46], "little")

        extra_field_start = 46 + cd_file.file_name_length
        cd_file.file_name = bytes(data[46:extra_field_start]).decode()

        cd_file.extra_field = data[
                              extra_field_start: extra_field_start + cd_file.extra_field_length
                              ]
        comment_start = extra_field_start + cd_file.extra_field_length
        cd_file.comment = data[
                          comment_start: comment_start + cd_file.file_comment_length
                          ]

        cd_file.last_byte = comment_start + cd_file.file_comment_length

        return cd_file, comment_start + cd_file.file_comment_length
    
    def as_dict(self):
        return {'file_name': self.file_name, 'crc32': self.crc32, 'compressed_size': self.compressed_size, 'size': self.uncompressed_size}

    def __str__(self):
        return f"\nCompressionMethod: {self.compression_method} \nFileNameLen: {self.file_name_length} \nFileName: {self.file_name} \nStartDisk: {self.disk_number_start} \nCompressedSize: {self.compressed_size} \nUncompressedSize: {self.uncompressed_size}"

    def __repr__(self):
        return self.file_name


class CentralDirectory:
    def __init__(self, product):
        self.files = []
        self.product = product

    @staticmethod
    def create_central_dir_file(data, product):
        return CentralDirectoryFile.from_bytes(data, product)

    @classmethod
    def from_bytes(cls, data, n, product):
        central_dir = cls(product)
        for record in range(n):
            cd_file, next_offset = central_dir.create_central_dir_file(data, product)
            central_dir.files.append(cd_file)
            data = data[next_offset:]
        return central_dir

class Zip64EndOfCentralDirLocator:
    def __init__(self):
        self.number_of_disk: int
        self.zip64_end_of_cd_offset: int
        self.total_number_of_disks: int

    @classmethod
    def from_bytes(cls, data):
        zip64_end_of_cd = cls()
        zip64_end_of_cd.number_of_disk = int.from_bytes(data[4:8], "little")
        zip64_end_of_cd.zip64_end_of_cd_offset = int.from_bytes(data[8:16], "little")
        zip64_end_of_cd.total_number_of_disks = int.from_bytes(data[16:20], "little")
        return zip64_end_of_cd
    
    def __str__(self):
        return f"\nZIP64EOCDLocator\nDisk Number: {self.number_of_disk}\nZ64_EOCD Offset: {self.zip64_end_of_cd_offset}\nNumber of disks: {self.total_number_of_disks}"

class Zip64EndOfCentralDir:
    def __init__(self):
        self.size: int
        self.version_made_by: bytes
        self.version_needed: bytes
        self.number_of_disk: bytes
        self.central_directory_start_disk: bytes
        self.number_of_entries_on_this_disk: int
        self.number_of_entries_total: int
        self.size_of_central_directory: int
        self.central_directory_offset: int
        self.extensible_data = None

    @classmethod
    def from_bytes(cls, data):
        end_of_cd = cls()

        end_of_cd.size = int.from_bytes(data[4:12], "little")
        end_of_cd.version_made_by = data[12:14]
        end_of_cd.version_needed = data[14:16]
        end_of_cd.number_of_disk = data[16:20]
        end_of_cd.central_directory_start_disk = data[20:24]
        end_of_cd.number_of_entries_on_this_disk = int.from_bytes(data[24:32], "little")
        end_of_cd.number_of_entries_total = int.from_bytes(data[32:40], "little")
        end_of_cd.size_of_central_directory = int.from_bytes(data[40:48], "little")
        end_of_cd.central_directory_offset = int.from_bytes(data[48:56], "little")

        return end_of_cd

    def __str__(self) -> str:
        return f"\nZ64 EndOfCD\nSize: {self.size}\nNumber of disk: {self.number_of_disk}\nEntries on this disk: {self.number_of_entries_on_this_disk}\nEntries total: {self.number_of_entries_total}\nCD offset: {self.central_directory_offset}"


class EndOfCentralDir:
    def __init__(self):
        self.number_of_disk: bytes
        self.central_directory_disk: bytes
        self.central_directory_records: int
        self.size_of_central_directory: int
        self.central_directory_offset: int
        self.comment_length: bytes
        self.comment: bytes

    @classmethod
    def from_bytes(cls, data):
        central_dir = cls()
        central_dir.number_of_disk = data[4:6]
        central_dir.central_directory_disk = data[6:8]
        central_dir.central_directory_records = int.from_bytes(data[8:10], "little")
        central_dir.size_of_central_directory = int.from_bytes(data[12:16], "little")
        central_dir.central_directory_offset = int.from_bytes(data[16:20], "little")
        central_dir.comment_length = data[20:22]
        central_dir.comment = data[
                              22: 22 + int.from_bytes(central_dir.comment_length, "little")
                              ]

        return central_dir

    def __str__(self):
        return f"\nDiskNumber: {self.number_of_disk} \nCentralDirRecords: {self.central_directory_records} \nCentralDirSize: {self.size_of_central_directory} \nCentralDirOffset: {self.central_directory_offset}"


class InstallerHandler:
    def __init__(self, url, product_id, session):
        self.url = url
        self.product = product_id
        self.session = session
        self.file_size = 0
        beginning_of_file = self.get_bytes_from_file(
            from_b=1024*512, size=1024*512, add_archive_index=False
        )
        
        self.start_of_archive_index = beginning_of_file.find(LOCAL_FILE_HEADER) + 1024*512

        # ZIP contents
        self.central_directory_offset: int
        self.central_directory_records: int
        self.size_of_central_directory: int
        self.central_directory: CentralDirectory

    def get_bytes_from_file(self, from_b=-1, size=None, add_archive_index=True, raw_response=False):
        if add_archive_index:
            from_b += self.start_of_archive_index

        from_b_repr = str(from_b) if from_b > -1 else ""
        if size:
            end_b = from_b + size - 1
        else:
            end_b = ""
        range_header = self.get_range_header(from_b_repr, end_b)

        response = self.session.get(self.url, headers={'Range': range_header},
                                    allow_redirects=False, stream=raw_response)
        if response.status_code == 302:
            # Skip content-system API
            self.url = response.headers.get('Location') or self.url
            return self.get_bytes_from_file(from_b, size, add_archive_index, raw_response)
        if not self.file_size:
            self.file_size = int(response.headers.get("Content-Range").split("/")[-1])
        if raw_response:
            return response
        else:
            data = response.content
            return data

    @staticmethod
    def get_range_header(from_b="", to_b=""):
        return f"bytes={from_b}-{to_b}"

    def setup(self):
        self.__find_end_of_cd()
        self.__find_central_directory()

    def __find_end_of_cd(self):
        end_of_cd_data = self.get_bytes_from_file(
            from_b=self.file_size - 100, add_archive_index=False
        )

        end_of_cd_header_data_index = end_of_cd_data.find(END_OF_CENTRAL_DIRECTORY)
        zip64_end_of_cd_locator_index = end_of_cd_data.find(ZIP_64_END_OF_CD_LOCATOR)
        end_of_cd = EndOfCentralDir.from_bytes(end_of_cd_data[end_of_cd_header_data_index:])
        if end_of_cd.central_directory_offset == 0xFFFFFFFF:
            # We need to find zip64 headers

            zip64_end_of_cd_locator = Zip64EndOfCentralDirLocator.from_bytes(end_of_cd_data[zip64_end_of_cd_locator_index:])
            zip64_end_of_cd_data = self.get_bytes_from_file(from_b=zip64_end_of_cd_locator.zip64_end_of_cd_offset, size=200)
            zip64_end_of_cd = Zip64EndOfCentralDir.from_bytes(zip64_end_of_cd_data)

            self.central_directory_offset = zip64_end_of_cd.central_directory_offset
            self.size_of_central_directory = zip64_end_of_cd.size_of_central_directory
            self.central_directory_records = zip64_end_of_cd.number_of_entries_total
        else:
            self.central_directory_offset = end_of_cd.central_directory_offset
            self.size_of_central_directory = end_of_cd.size_of_central_directory
            self.central_directory_records = end_of_cd.central_directory_records 

    def __find_central_directory(self):
        central_directory_data = self.get_bytes_from_file(
            from_b=self.central_directory_offset,
            size=self.size_of_central_directory,
        )

        self.central_directory = CentralDirectory.from_bytes(
            central_directory_data, self.central_directory_records, self.product
        )


class LinuxFile:
    def __init__(self, product, path, compression, start, compressed_size, size, checksum, executable):
        self.product = product
        self.path = path
        self.compression = compression == 8
        self.offset = start
        self.compressed_size = compressed_size
        self.size = size
        self.hash = str(checksum)
        self.flags = []
        if executable:
            self.flags.append("executable")

