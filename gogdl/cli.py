#!/usr/bin/env python3
import gogdl.args as args
from gogdl.dl.managers import manager
from gogdl.dl.managers import dependencies
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
    logger.debug(arguments)
    if arguments.display_version:
        display_version()
        return
    if not arguments.command:
        print("No command provided!")
        return
    api_handler = api.ApiHandler(
        arguments.token.strip('"') if arguments.token else None
    )
    clouds_storage_manager = saves.CloudStorageManager(api_handler)

    switcher = {}
    if arguments.command in ["download", "repair", "update", "info"]:
        download_manager = manager.Manager(arguments, unknown_args, api_handler)
        switcher = {
            "download": download_manager.download,
            "repair": download_manager.download,
            "update": download_manager.download,
            "info": download_manager.calculate_download_size,
        }
    elif arguments.command in ["redist", "dependencies"]:
        dependencies_handler = dependencies.DependenciesManager(arguments, unknown_args, api_handler)
        dependencies_handler.get()
    else:
        switcher = {
            "import": imports.get_info,
            "launch": launch.launch,
            "save-sync": clouds_storage_manager.sync,
            "save-clear": clouds_storage_manager.clear,
    }

    function = switcher.get(arguments.command)
    if function:
        function(arguments, unknown_args)


if __name__ == "__main__":
    main()
