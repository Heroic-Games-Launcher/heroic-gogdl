#!/usr/bin/env python3
import gogdl.args as args
from gogdl.dl import manager
import gogdl.api as api
import gogdl.imports as imports
import gogdl.launch as launch
import gogdl.saves as saves
import gogdl.auth as auth
from gogdl import version as gogdl_version
import logging

logging.basicConfig(format="[%(name)s] %(levelname)s: %(message)s", level=logging.INFO)
logger = logging.getLogger("MAIN")


def display_version():
    print(f"{gogdl_version}")


def main():
    arguments, unknown_args = args.init_parser()
    logger.info(arguments)
    if arguments.display_version:
        display_version()
        return
    if not arguments.command:
        print("No command provided!")
        return
    authorization_manager = auth.AuthorizationManager(arguments.auth_config_path)
    api_handler = api.ApiHandler(authorization_manager)
    download_manager = manager.DownloadManager(api_handler)
    clouds_storage_manager = saves.CloudStorageManager(api_handler, authorization_manager)
    switcher = {
        "download": download_manager.download,
        "repair": download_manager.download,
        "update": download_manager.download,
        "import": imports.get_info,
        "info": download_manager.calculate_download_size,
        "launch": launch.launch,
        "save-sync": clouds_storage_manager.sync,
        "save-clear": clouds_storage_manager.clear,
        "auth": authorization_manager.handle_cli
    }

    function = switcher.get(arguments.command)
    if function:
        function(arguments, unknown_args)


if __name__ == "__main__":
    main()
