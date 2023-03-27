import os
import sys
import logging
import requests
import hashlib
import datetime
import gzip
from enum import Enum

import gogdl.dl.dl_utils as dl_utils
import gogdl.constants as constants

LOCAL_TIMEZONE = datetime.datetime.utcnow().astimezone().tzinfo


class SyncAction(Enum):
    DOWNLOAD = 0
    UPLOAD = 1
    CONFLICT = 2
    NONE = 3


class SyncFile:
    def __init__(self, path, abs_path, md5=None, update_time=None):
        self.relative_path = path
        self.absolute_path = abs_path
        self.md5 = md5
        self.update_time = update_time
        self.update_ts = (
            datetime.datetime.fromisoformat(update_time).astimezone().timestamp()
            if update_time
            else None
        )

    def get_file_metadata(self):
        ts = os.stat(self.absolute_path).st_mtime
        date_time_obj = datetime.datetime.fromtimestamp(
            ts, tz=LOCAL_TIMEZONE
        ).astimezone(datetime.timezone.utc)
        self.md5 = hashlib.md5(
            gzip.compress(open(self.absolute_path, "rb").read(), 6, mtime=0)
        ).hexdigest()

        self.update_time = date_time_obj.isoformat(timespec="seconds")
        self.update_ts = date_time_obj.timestamp()

    def __repr__(self):
        return f"{self.md5} {self.relative_path}"


class CloudStorageManager:
    def __init__(self, api_handler, authorization_manager):
        self.api = api_handler
        self.auth_manager = authorization_manager
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
        prefered_action = arguments.prefered_action
        self.sync_path = os.path.normpath(arguments.path.strip('"'))
        self.sync_path = self.sync_path.replace("\\", os.sep)
        self.cloud_save_dir_name = arguments.dirname
        self.arguments = arguments
        self.unknown_args = unknown_args

        if not os.path.exists(self.sync_path):
            self.logger.warning("Provided path doesn't exist, creating")
            os.makedirs(self.sync_path, exist_ok=True)
        dir_list = self.create_directory_map(self.sync_path)
        if len(dir_list) == 0:
            self.logger.info("No files in directory")

        local_files = [
            SyncFile(self.get_relative_path(self.sync_path, f), f) for f in dir_list
        ]

        for f in local_files:
            f.get_file_metadata()

        self.logger.info(f"Local files: {len(dir_list)}")
        self.client_id, self.client_secret = self.get_auth_ids()
        self.get_auth_token()

        cloud_files = self.get_cloud_files_list()

        if len(local_files) > 0 and len(cloud_files) == 0:
            action = SyncAction.UPLOAD
            self.logger.info("No files in cloud, uploading")
            for f in local_files:
                self.upload_file(f)
            self.logger.info("Done")
            return
        elif len(local_files) == 0 and len(cloud_files) > 0:
            self.logger.info("No files locally, downloading")
            action = SyncAction.DOWNLOAD
            for f in cloud_files:
                self.download_file(f)
            self.logger.info("Done")
            return

        timestamp = float(arguments.timestamp)
        classifier = SyncClassifier.classify(local_files, cloud_files, timestamp)

        action = classifier.get_action()
        # print(action)

        if prefered_action:
            if prefered_action == "forceupload":
                self.logger.warning("Forcing upload")
                classifier.updated_local = local_files
                action = SyncAction.UPLOAD
            elif prefered_action == "forcedownload":
                self.logger.warning("Forcing download")
                classifier.updated_cloud = cloud_files
                action = SyncAction.DOWNLOAD
            if prefered_action == "upload" and action == SyncAction.DOWNLOAD:
                self.logger.warning("Refused to upload files, newer files in the cloud")
                print(self.arguments.timestamp)
                return
            elif prefered_action == "download" and action == SyncAction.UPLOAD:
                self.logger.warning("Refused to download files, newer files locally")
                print(self.arguments.timestamp)
                return

        # return
        if action == SyncAction.UPLOAD:
            self.logger.info("Uploading files")
            for f in classifier.updated_local:
                self.upload_file(f)
        elif action == SyncAction.DOWNLOAD:
            self.logger.info("Downloading files")
            for f in classifier.updated_cloud:
                self.download_file(f)
        elif action == SyncAction.CONFLICT:
            self.logger.warning(
                "Files in conflict force downloading or uploading of files"
            )
        elif action == SyncAction.NONE:
            self.logger.info("Nothing to do")

        sys.stdout.write(str(datetime.datetime.now().timestamp()))
        sys.stdout.flush()
        self.logger.info("Done")

    def clear(self, arguments, unknown_args):
        self.sync_path = os.path.normpath(arguments.path.strip('"'))
        self.sync_path = self.sync_path.replace("\\", os.sep)
        self.cloud_save_dir_name = arguments.dirname
        self.arguments = arguments
        self.unknown_args = unknown_args

        self.client_id, self.client_secret = self.get_auth_ids()
        self.get_auth_token()

        cloud_files = self.get_cloud_files_list()
        for f in cloud_files:
            self.delete_file(f)
        self.logger.info("Done")

    def get_auth_token(self):
        self.credentials = self.auth_manager.get_credentials(self.client_id, self.client_secret)
        self.session.headers.update(
            {"Authorization": f"Bearer {self.credentials['access_token']}"}
        )

    def is_in_our_dir(self, value):
        return value["name"].startswith(self.cloud_save_dir_name)

    def get_cloud_files_list(self):
        response = self.session.get(
            f"{constants.GOG_CLOUDSTORAGE}/v1/{self.credentials['user_id']}/{self.client_id}",
            headers={"Accept": "application/json"},
        )

        if not response.ok:
            return []

        json_res = response.json()
        # print(json_res)
        self.logger.info(f"Files in cloud: {len(json_res)}")

        filtered = filter(self.is_in_our_dir, json_res)

        files = [
            SyncFile(
                sync_f["name"].replace(f"{self.cloud_save_dir_name}/", "", 1),
                os.path.join(
                    self.sync_path,
                    sync_f["name"].replace(f"{self.cloud_save_dir_name}/", "", 1),
                ),
                md5=sync_f["hash"],
                update_time=sync_f["last_modified"],
            )
            for sync_f in filtered
        ]

        return files

    def get_auth_ids(self):
        builds = dl_utils.get_json(
            self.api,
            f"{constants.GOG_CONTENT_SYSTEM}/products/{self.arguments.id}/os/{self.arguments.platform}/builds?generation=2",
        )
        meta_url = builds["items"][0]["link"]

        meta, headers = dl_utils.get_zlib_encoded(self.api, meta_url, self.logger)
        return meta["clientId"], meta["clientSecret"]

    def delete_file(self, file: SyncFile):
        self.logger.info(f"Deleting {file.relative_path}")
        response = self.session.delete(
            f"{constants.GOG_CLOUDSTORAGE}/v1/{self.credentials['user_id']}/{self.client_id}/{self.cloud_save_dir_name}/{file.relative_path}",
        )

    def upload_file(self, file: SyncFile):
        compressed_data = gzip.compress(
            open(file.absolute_path, "rb").read(), 6, mtime=0
        )
        headers = {
            "X-Object-Meta-LocalLastModified": f"{file.update_time}",
            "Etag": hashlib.md5(compressed_data).hexdigest(),
            "Content-Encoding": "gzip",
        }

        response = self.session.put(
            f"{constants.GOG_CLOUDSTORAGE}/v1/{self.credentials['user_id']}/{self.client_id}/{self.cloud_save_dir_name}/{file.relative_path}",
            data=compressed_data,
            headers=headers,
        )

        if not response.ok:
            self.logger.error(
                f"There was an error uploading a file \n{response.status_code}\n{response.content}"
            )
            return

    def download_file(self, file: SyncFile, retries=3):
        try:
            response = self.session.get(
                f"{constants.GOG_CLOUDSTORAGE}/v1/{self.credentials['user_id']}/{self.client_id}/{self.cloud_save_dir_name}/{file.relative_path}",
                stream=True,
            )
        except:
            if (retries > 1):
                self.logger.debug(f"Failed sync of {file}, retrying (retries left {retries - 1})")
                self.download_file(file, retries - 1)
                return
            else:
                response = {}

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

        try:
            f_timestamp = (
                datetime.datetime.fromisoformat(
                    response.headers.get("X-Object-Meta-LocalLastModified")
                )
                .astimezone()
                .timestamp()
            )
            os.utime(file.absolute_path, (f_timestamp, f_timestamp))
        except ValueError:
            self.logger.warning(f"Incorrect LastModified header for file {file.relative_path} {response.headers.get('X-Object-Meta-LocalLastModified')} ; Ignoring...")
            pass 


class SyncClassifier:
    def __init__(self):
        self.action = None
        self.updated_local = list()
        self.updated_cloud = list()

    def get_action(self):
        if len(self.updated_local) == 0 and len(self.updated_cloud) > 0:
            self.action = SyncAction.DOWNLOAD

        elif len(self.updated_local) > 0 and len(self.updated_cloud) == 0:
            self.action = SyncAction.UPLOAD

        elif len(self.updated_local) == 0 and len(self.updated_cloud) == 0:
            self.action = SyncAction.NONE

        else:
            self.action = SyncAction.CONFLICT

        return self.action

    @classmethod
    def classify(cls, local, cloud, timestamp):
        classifier = cls()

        for f in local:

            if f.update_ts > timestamp:
                classifier.updated_local.append(f)

        for f in cloud:

            if f.update_ts > timestamp:
                classifier.updated_cloud.append(f)

        return classifier
