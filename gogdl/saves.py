import os
import logging
import requests
import hashlib
import datetime
import gzip
from enum import Enum

import gogdl.dl.dl_utils as dl_utils
import gogdl.constants as constants


class SyncAction(Enum):
    DOWNLOAD = 0
    UPLOAD = 1
    CONFLICT = 2


class SyncFile:
    def __init__(self, path, abs_path, md5=None, update_time=None):
        self.relative_path = path
        self.absolute_path = abs_path
        self.md5 = md5
        self.update_time = update_time

    @staticmethod
    def get_file_metadata(path: str):

        ts = os.stat(path).st_mtime
        print(ts)
        date_time_obj = datetime.datetime.fromtimestamp(ts)
        md5_sum = hashlib.md5(
            gzip.compress(open(path, "rb").read(), 6, mtime=0)
        ).hexdigest()

        return md5_sum, date_time_obj.isoformat(timespec="microseconds")

    def __repr__(self):
        return f"{self.md5} {self.relative_path}"


class CloudStorageManager:
    def __init__(self, api_handler):
        self.api = api_handler
        self.session = requests.Session()
        self.logger = logging.getLogger("SAVES")

        self.session.headers.update(
            {"User-Agent": "GOGGalaxyCommunicationService/2.0.4.164 (Windows_32bit)"}
        )

        self.credentials = dict()
        self.client_id = str()
        self.client_secret = str()

    def create_directory_map(self, path: str) -> list:
        """
        Creates list of every file in directory to be synced
        """
        files = list()
        directory_contents = os.listdir(path)

        for content in directory_contents:
            abs_path = os.path.join(path, content)
            if os.path.isdir(abs_path):
                files.extend(self.create_directory_map(abs_path))
            else:
                files.append(abs_path)
        return files

    def get_relative_path(self, root: str, path: str) -> str:
        if not root.endswith("/") and not root.endswith("\\"):
            root = root + os.sep
        return path.replace(root, "")

    def sync(self, arguments, unknown_args):
        self.sync_path = os.path.normpath(arguments.path)
        self.sync_path = self.sync_path.replace("\\", os.sep)

        self.arguments = arguments
        self.unknown_args = unknown_args

        if not os.path.exists(self.sync_path):
            self.logger.error("Provided path doesn't exist")
            exit(1)
        dir_list = self.create_directory_map(self.sync_path)
        if len(dir_list) == 0:
            self.logger.info("No files in directory")

        local_files = [
            SyncFile(self.get_relative_path(self.sync_path, f), f) for f in dir_list
        ]

        for f in local_files:
            f.md5, f.update_time = SyncFile.get_file_metadata(f.absolute_path)

        self.logger.info(f"Local files: {len(dir_list)}")
        self.client_id, self.client_secret = self.get_auth_ids()
        self.get_auth_token()

        cloud_files = self.get_cloud_files_list()

        # comparison = FilesComparison.compare(local_files, cloud_files)

        # self.logger.info(f"Files to download {len(comparison.download)}")
        # self.logger.info(f"Files to upload {len(comparison.upload)}")

        # # print(local_files, cloud_files)
        # return
        # if len(comparison.download) == 0 and len(comparison.upload) == 0:
        #     self.logger.info("Nothing to do")
        #     return
        newest_file = cloud_files[0]
        for f in cloud_files:
            if newest_file.update_time > f.update_time:
                newest_file = f

        action = None
        if len(local_files) > 0 and len(cloud_files) == 0:
            action = SyncAction.UPLOAD
        elif len(local_files) == 0 and len(cloud_files) > 0:
            action = SyncAction.DOWNLOAD
        print(newest_file)
        print(action)
        # for f in cloud_files:
            # self.download_file(f)
        return


        self.logger.info("Done")

    def store_success_info(self):

    def get_auth_token(self):
        url = self._get_token_gen_url(self.client_id, self.client_secret)

        self.credentials = dl_utils.get_json(self, url)
        self.session.headers.update(
            {"Authorization": f"Bearer {self.credentials['access_token']}"}
        )

    def get_cloud_files_list(self):
        response = self.session.get(
            f"{constants.GOG_CLOUDSTORAGE}/v1/{self.credentials['user_id']}/{self.client_id}",
            headers={"Accept": "application/json"},
        )

        if not response.ok:
            return []

        json_res = response.json()
        print(json_res)
        self.logger.info(f"Files in cloud: {len(json_res)}")

        files = [
            SyncFile(
                sync_f["name"].replace("saves/", "", 1),
                os.path.join(self.sync_path, sync_f["name"].replace("saves/", "", 1)),
                md5=sync_f["hash"],
                update_time=sync_f["last_modified"].split("+")[0],
            )
            for sync_f in json_res
        ]

        return files

    def get_auth_ids(self):
        builds = dl_utils.get_json(
            self.api,
            f"{constants.GOG_CONTENT_SYSTEM}/products/{self.arguments.id}/os/{self.arguments.platform}/builds?generation=2",
        )
        meta_url = builds["items"][0]["link"]

        meta, headers = dl_utils.get_zlib_encoded(self.api, meta_url)
        return meta["clientId"], meta["clientSecret"]

    def delete_file(self, file: SyncFile):
        self.logger.info(f"Deleting {file.relative_path}")
        response = self.session.delete(
            f"{constants.GOG_CLOUDSTORAGE}/v1/{self.credentials['user_id']}/{self.client_id}/saves/{file.relative_path}",
        )

    def upload_file(self, file: SyncFile):
        self.logger.info(f"Uploading {file.relative_path}")

        compressed_data = gzip.compress(
            open(file.absolute_path, "rb").read(), 6, mtime=0
        )
        headers = {
            "X-Object-Meta-LocalLastModified": f"{file.update_time}+00:00",
            "Etag": hashlib.md5(compressed_data).hexdigest(),
            "Content-Encoding": "gzip",
        }

        response = self.session.put(
            f"{constants.GOG_CLOUDSTORAGE}/v1/{self.credentials['user_id']}/{self.client_id}/saves/{file.relative_path}",
            data=compressed_data,
            headers=headers,
        )

        if not response.ok:
            self.logger.error("There was an error uploading a file")
            return
        self.logger.info(f"Successfully uploaded file {file.relative_path}")

    def download_file(self, file: SyncFile):
        self.logger.info(f"Downloading {file.relative_path}")

        response = self.session.get(
            f"{constants.GOG_CLOUDSTORAGE}/v1/{self.credentials['user_id']}/{self.client_id}/saves/{file.relative_path}",
            stream=True,
        )

        if not response.ok:
            self.logger.error("Downloading file failed")

        total = response.headers.get("Content-Length")
        os.makedirs(os.path.split(file.absolute_path)[0], exist_ok=True)
        with open(file.absolute_path, "wb") as f:
            # if not total:
            #     f.write(response.content)
            total = int(total)
            for data in response.iter_content(
                chunk_size=max(int(total / 1000), 1024 * 1024)
            ):
                f.write(data)

        f_timestamp = datetime.datetime.fromisoformat(
            response.headers.get("X-Object-Meta-LocalLastModified")
        ).timestamp()

        os.utime(file.absolute_path, (f_timestamp, f_timestamp))

    def _get_token_gen_url(self, client_id: str, client_secret: str) -> str:
        return f"https://auth.gog.com/token?client_id={client_id}&client_secret={client_secret}&grant_type=refresh_token&refresh_token={self.arguments.token}&without_new_session=1"


class FilesComparison:
    def __init__(self):
        self.download = []
        self.upload = []

    @classmethod
    def compare(cls, local, cloud):
        comparison = cls()

        if len(cloud) > 0:
            local_files = dict()
            for f in local:
                local_files[f.relative_path] = f

            for f in cloud:
                if f.relative_path not in local_files:
                    comparison.download.append(f)
                    continue

                c_timestamp = datetime.datetime.fromisoformat(f.update_time).timestamp()
                l_timestamp = datetime.datetime.fromisoformat(
                    local_files[f.relative_path].update_time
                ).timestamp()
                if c_timestamp > l_timestamp:
                    comparison.download.append(f)
                elif c_timestamp < l_timestamp:
                    comparison.upload.append(local_files[f.relative_path])

                del local_files[f.relative_path]

            for f in local_files:
                comparison.upload.append(local_files[f])
        else:
            # In this case there are just new files
            comparison.upload = local

        return comparison


class SyncClassifier:
    def __init__(self):
        self.action = None

    def classify(cls, local, cloud):
        classifier = cls()
