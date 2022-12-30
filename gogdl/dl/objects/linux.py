END_OF_CENTRAL_DIRECTORY = b"\x50\x4b\x05\x06"
CENTRAL_DIRECTORY = b"\x50\x4b\x01\x02"
LOCAL_FILE_HEADER = b"\x50\x4b\x03\x04"


class LocalFile:
    def load_data(self, handler):
        return handler.get_bytes_from_file(
            from_b=self.last_byte + self.relative_local_file_offset,
            size=self.compressed_size,
            raw_response=True
        )

    @classmethod
    def from_bytes(cls, data, offset, handler):
        local_file = cls()
        local_file.relative_local_file_offset = None
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
            size=local_file.file_name_length + local_file.file_name_length,
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
    @classmethod
    def from_bytes(cls, data):
        cd_file = cls()

        cd_file.version_made_by = data[4:6]
        cd_file.version_needed_to_extract = data[6:8]
        cd_file.general_purpose_bit_flag = data[8:10]
        cd_file.compression_method = data[10:12]
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

    def __str__(self):
        return f"\nCompressionMethod: {self.compression_method} \nFileNameLen: {self.file_name_length} \nFileName: {bytes(self.file_name).decode('iso-8859-15')} \nStartDisk: {self.disk_number_start} \nCompressedSize: {self.compressed_size} \nUncompressedSize: {self.uncompressed_size}"

    def __repr__(self):
        return self.file_name


class CentralDirectory:
    def __init__(self):
        self.files = []

    @staticmethod
    def create_central_dir_file(data):
        return CentralDirectoryFile.from_bytes(data)

    @classmethod
    def from_bytes(cls, data, n):
        central_dir = cls()
        for record in range(n):
            cd_file, next_offset = central_dir.create_central_dir_file(data)
            central_dir.files.append(cd_file)
            data = data[next_offset:]
        return central_dir


class EndOfCentralDir:
    def __init__(self):
        self.number_of_disk = None
        self.central_directory_disk = None
        self.central_directory_records = None
        self.size_of_central_directory = None
        self.central_directory_offset = None
        self.comment_length = None
        self.comment = None

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
    def __init__(self, url, file_size, session):
        self.url = url
        self.session = session
        self.file_size = file_size
        beginning_of_file = self.get_bytes_from_file(
            from_b=760000, size=300000, add_archive_index=False
        )
        self.start_of_archive_index = beginning_of_file.find(LOCAL_FILE_HEADER) + 760000

        # ZIP contents
        self.end_of_cd = None
        self.central_directory = None

    def get_bytes_from_file(self, from_b=0, size=None, add_archive_index=True, raw_response=False):
        if add_archive_index:
            from_b += self.start_of_archive_index

        from_b_repr = str(from_b) if from_b > 0 else ""
        if size:
            end_b = from_b + size
        else:
            end_b = ""
        range_header = self.get_range_header(from_b_repr, end_b)

        response = self.session.get(self.url, headers={'Range': range_header},
                                    allow_redirects=True, stream=raw_response)
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
            from_b=self.file_size - 32, add_archive_index=False
        )

        end_of_cd_header_data_index = end_of_cd_data.find(END_OF_CENTRAL_DIRECTORY)
        self.end_of_cd = EndOfCentralDir.from_bytes(end_of_cd_data[end_of_cd_header_data_index:])

    def __find_central_directory(self):
        central_directory_data = self.get_bytes_from_file(
            from_b=self.end_of_cd.central_directory_offset,
            size=self.end_of_cd.size_of_central_directory,
        )

        self.central_directory = CentralDirectory.from_bytes(
            central_directory_data, self.end_of_cd.central_directory_records
        )
