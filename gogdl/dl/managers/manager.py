from multiprocessing import cpu_count
import logging
import json

from gogdl import constants
from gogdl.dl.managers import linux, v1, v2


class Manager:
    def __init__(self, arguments, unknown_arguments, api_handler):
        self.arguments = arguments
        self.unknown_arguments = unknown_arguments
        self.api_handler = api_handler

        self.platform = arguments.platform
        self.should_append_folder_name = self.arguments.command != "install"
        self.game_id = arguments.id
        if "workers_count" in arguments:
            self.allowed_threads = arguments.workers_count
        else:
            self.allowed_threads = cpu_count()

        self.logger = logging.getLogger("GENERIC DOWNLOAD_MANAGER")

        self.download_manager = None
        self.builds = None
        self.target_build = None

    def get_builds(self):
        build_platform = self.platform
        if self.platform == 'linux':
            build_platform = 'windows'
        response = self.api_handler.session.get(
            f"{constants.GOG_CONTENT_SYSTEM}/products/{self.game_id}/os/{build_platform}/builds?generation=2"
        )

        if not response.ok:
            raise Exception("Platform unsupported")

        return response.json()

    def calculate_download_size(self, arguments, unknown_arguments):
        self.setup_download_manager()

        download_size_response = self.download_manager.get_download_size()
        print(json.dumps(download_size_response))
        return

    def download(self, arguments, unknown_arguments):
        self.setup_download_manager()

        self.download_manager.download()

    def setup_download_manager(self):

        if self.platform == "linux":
            self.logger.info(
                "Platform is Linux, redirecting download to Linux Native manager"
            )

            self.download_manager = linux.Manager(self)

            return

        self.builds = self.get_builds()

        self.target_build = self.builds["items"][0]

        if self.arguments.build:
            # Find build
            for build in self.builds["items"]:
                if build["build_id"] == self.arguments.build:
                    self.target_build = build
                    break

        generation = self.target_build["generation"]

        if generation == 1 or generation == 2:
            self.logger.info(f"Depot version: {generation}")
        else:
            raise Exception("Unsupported depot version please report this")

        if generation == 1:
            self.download_manager = v1.Manager(self)
        elif generation == 2:
            self.download_manager = v2.Manager(self)
