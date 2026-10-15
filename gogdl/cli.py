#!/usr/bin/env python3
import json
import logging
from multiprocessing import freeze_support

from gogdl import api, args, auth, imports, languages, launch, saves
from gogdl import version as gogdl_version
from gogdl.dl.managers import dependencies, manager


def display_version():
    print(f"{gogdl_version}")


def match_lang(arguments, unknown_arguments):
    lang = languages.Language.parse(arguments.language)
    data = lang.__dict__ if lang else {}
    print(json.dumps(data))

def main():
    arguments, unknown_args = args.init_parser()
    level = logging.INFO
    if '-d' in unknown_args or '--debug' in unknown_args:
        level = logging.DEBUG
    logging.basicConfig(format="[%(name)s] %(levelname)s: %(message)s", level=level)
    logging.getLogger("urllib3").setLevel(level)
    logger = logging.getLogger("MAIN")
    logger.debug(arguments)
    if arguments.display_version:
        display_version()
        return
    if not arguments.command:
        print("No command provided!")
        return
    authorization_manager = auth.AuthorizationManager(arguments.auth_config_path)
    api_handler = api.ApiHandler(authorization_manager)
    clouds_storage_manager = saves.CloudStorageManager(api_handler, authorization_manager)

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
        dependencies_handler = dependencies.DependenciesManager(arguments.ids.split(","), arguments.path, arguments.workers_count, api_handler, print_manifest=arguments.print_manifest)
        if not arguments.print_manifest:
            dependencies_handler.get()
    else:
        switcher = {
            "import": imports.get_info,
            "launch": launch.launch,
            "save-sync": clouds_storage_manager.sync,
            "save-clear": clouds_storage_manager.clear,
            "auth": authorization_manager.handle_cli,
            "lang-match": match_lang
        }

    function = switcher.get(arguments.command)
    if function:
        function(arguments, unknown_args)


if __name__ == "__main__":
    freeze_support()
    main()
