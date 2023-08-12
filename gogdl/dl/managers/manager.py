from multiprocessing import cpu_count
from sys import exit
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
        self.should_append_folder_name = self.arguments.command == "download"
        self.is_verifying = self.arguments.command == "repair"
        self.game_id = arguments.id
        self.branch = arguments.branch or None
        if "workers_count" in arguments:
            self.allowed_threads = int(arguments.workers_count)
        else:
            self.allowed_threads = cpu_count()

        self.logger = logging.getLogger("GENERIC DOWNLOAD_MANAGER")

        self.galaxy_api_data = None

        self.download_manager = None
        self.builds = None
        self.target_build = None

    def get_builds(self):
        build_platform = self.platform
        password = '' if not self.arguments.password else '&' + self.arguments.password
        generation = self.arguments.force_generation or "2"
        response = self.api_handler.session.get(
            f"{constants.GOG_CONTENT_SYSTEM}/products/{self.game_id}/os/{build_platform}/builds?&generation={generation}{password}"
        )

        if not response.ok:
            raise Exception("Platform unsupported")

        return response.json()

    def calculate_download_size(self, arguments, unknown_arguments):
        self.setup_download_manager()

        download_size_response = self.download_manager.get_download_size()


        print(json.dumps(download_size_response))

    def download(self, arguments, unknown_arguments):
        self.setup_download_manager()

        self.download_manager.download()

    def setup_download_manager(self):
        self.galaxy_api_data = self.api_handler.get_item_data(self.game_id, ["downloads", "expanded_dlcs"])

        if self.platform == "linux" and not self.galaxy_api_data["content_system_compatibility"]["linux"]:
            self.logger.info(
                "Platform is Linux, redirecting download to Linux Native installer manager"
            )

            self.download_manager = linux.Manager(self)

            return
        

        if not self.galaxy_api_data["content_system_compatibility"].get(self.platform):
            self.logger.error(f"Game doesn't support content system api, unable to proceed using platfrom {self.platform}")
            exit(1)

        # If Linux download ever progresses to this point, then it's time for some good party
        self.builds = self.get_builds()

        if len(self.builds["items"]) == 0:
            self.logger.error("No builds found") 
            exit(1)
        self.target_build = self.builds["items"][0]

        for build in self.builds["items"]:
            if build["branch"] == self.branch:
                self.target_build = build
                break

        if self.arguments.build:
            # Find build
            for build in self.builds["items"]:
                if build["build_id"] == self.arguments.build:
                    self.target_build = build
                    break

        generation = self.target_build["generation"]

        # This code shouldn't run at all but it's here just in case GOG decides they will return different generation than requested one
        # Of course assuming they will ever change their content system generation (I highly doubt they will)
        if generation not in [1, 2]:
            raise Exception("Unsupported depot version please report this")

        self.logger.info(f"Depot version: {generation}")

        if generation == 1:
            self.download_manager = v1.Manager(self)
        elif generation == 2:
            self.download_manager = v2.Manager(self)
