# Initialize argparse module and return arguments
import argparse
from multiprocessing import cpu_count


def init_parser():
    parser = argparse.ArgumentParser(
        description="GOG downloader for Heroic Games Launcher"
    )

    parser.add_argument(
        "--version",
        "-v",
        dest="display_version",
        action="store_true",
        help="Display GOGDL version",
    )

    parser.add_argument("--auth-config-path", dest="auth_config_path",
                        help="Path to json file where tokens will be stored", required=True)

    subparsers = parser.add_subparsers(dest="command")

    import_parser = subparsers.add_parser(
        "import", help="Show data about game in the specified path"
    )
    import_parser.add_argument("path")

    # REDIST DOWNLOAD

    redist_download_parser = subparsers.add_parser("redist", aliases=["dependencies"],
                                                   help="Download specified dependencies to provided location")

    redist_download_parser.add_argument("ids", help="Coma separated ids")
    redist_download_parser.add_argument("--path", help="Location where to download the files", required=True)
    redist_download_parser.add_argument("--print-manifest", action="store_true", help="Prints manifest to stdout")
    redist_download_parser.add_argument(
        "--max-workers",
        dest="workers_count",
        default=cpu_count(),
        help="Specify number of worker threads, by default number of CPU threads",
    )


    # AUTH

    auth_parser = subparsers.add_parser("auth", help="Manage authorization")
    auth_parser.add_argument("--client-id", dest="client_id")
    auth_parser.add_argument("--client-secret", dest="client_secret")
    auth_parser.add_argument("--code", dest="authorization_code",
                             help="Pass authorization code (use for login), when passed client-id and secret are ignored")

    # DOWNLOAD

    download_parser = subparsers.add_parser(
        "download", aliases=["repair", "update"], help="Download/update/repair game"
    )
    download_parser.add_argument("id", help="Game id")
    download_parser.add_argument("--lang", "-l", help="Specify game language")
    download_parser.add_argument(
        "--build", "-b", dest="build", help="Specify buildId (allows repairing)"
    )
    download_parser.add_argument(
        "--path", "-p", dest="path", help="Specify download path", required=True
    )
    download_parser.add_argument(
        "--platform",
        "--os",
        dest="platform",
        help="Target opearting system",
        choices=["windows", "osx", "linux"],
    )
    download_parser.add_argument(
        "--with-dlcs", dest="dlcs", action="store_true", help="Should download all dlcs"
    )
    download_parser.add_argument(
        "--skip-dlcs", dest="dlcs", action="store_false", help="Should skip all dlcs"
    )
    download_parser.add_argument(
        "--dlcs",
        dest="dlcs_list",
        default=[],
        help="List of dlc ids to download (separated by coma)",
    )
    download_parser.add_argument(
        "--dlc-only", dest="dlc_only", action="store_true", help="Download only DLC"
    )
    download_parser.add_argument("--branch", help="Choose build branch to use")
    download_parser.add_argument("--password", help="Password to access other branches")
    download_parser.add_argument("--force-gen", choices=["1", "2"], dest="force_generation", help="Force specific manifest generation (FOR DEBUGGING)")
    download_parser.add_argument(
        "--max-workers",
        dest="workers_count",
        default=cpu_count(),
        help="Specify number of worker threads, by default number of CPU threads",
    )

    # SIZE CALCULATING, AND OTHER MANIFEST INFO

    calculate_size_parser = subparsers.add_parser(
        "info", help="Calculates estimated download size and list of DLCs"
    )

    calculate_size_parser.add_argument(
        "--with-dlcs",
        dest="dlcs",
        action="store_true",
        help="Should download all dlcs",
    )
    calculate_size_parser.add_argument(
        "--skip-dlcs", dest="dlcs", action="store_false", help="Should skip all dlcs"
    )
    calculate_size_parser.add_argument(
        "--dlcs",
        dest="dlcs_list",
        help="Coma separated list of dlc ids to download",
    )
    calculate_size_parser.add_argument(
        "--dlc-only", dest="dlc_only", action="store_true", help="Download only DLC"
    )
    calculate_size_parser.add_argument("id")
    calculate_size_parser.add_argument(
        "--platform",
        "--os",
        dest="platform",
        help="Target opearting system",
        choices=["windows", "osx", "linux"],
    )
    calculate_size_parser.add_argument(
        "--build", "-b", dest="build", help="Specify buildId"
    )
    calculate_size_parser.add_argument("--lang", "-l", help="Specify game language")
    calculate_size_parser.add_argument("--branch", help="Choose build branch to use")
    calculate_size_parser.add_argument("--password", help="Password to access other branches")
    calculate_size_parser.add_argument("--force-gen", choices=["1", "2"], dest="force_generation", help="Force specific manifest generation (FOR DEBUGGING)")
    calculate_size_parser.add_argument(
        "--max-workers",
        dest="workers_count",
        default=cpu_count(),
        help="Specify number of worker threads, by default number of CPU threads",
    )

    # LAUNCH

    launch_parser = subparsers.add_parser(
        "launch", help="Launch the game in specified path", add_help=False
    )
    launch_parser.add_argument("path")
    launch_parser.add_argument("id")
    launch_parser.add_argument(
        "--platform",
        "--os",
        dest="platform",
        help="Target opearting system",
        choices=["windows", "osx", "linux"],
        required=True,
    )
    launch_parser.add_argument("--prefer-task", dest="preferred_task", default=None, help="Select playTask index to be run")
    launch_parser.add_argument(
        "--no-wine", action="store_true", dest="dont_use_wine", default=False
    )
    launch_parser.add_argument("--wine", dest="wine", help="Specify wine bin path")
    launch_parser.add_argument("--wine-prefix", dest="wine_prefix")
    launch_parser.add_argument("--wrapper", dest="wrapper")
    launch_parser.add_argument(
        "--override-exe", dest="override_exe", help="Override executable to be run"
    )

    # SAVES

    save_parser = subparsers.add_parser("save-sync", help="Sync game saves")
    save_parser.add_argument("path", help="Path to sync files")
    save_parser.add_argument("id", help="Game id")
    save_parser.add_argument(
        "--ts", dest="timestamp", help="Last sync timestamp", required=True
    )
    save_parser.add_argument("--name", dest="dirname", default="__default")
    save_parser.add_argument(
        "--skip-download", dest="prefered_action", action="store_const", const="upload"
    )
    save_parser.add_argument(
        "--skip-upload", dest="prefered_action", action="store_const", const="download"
    )
    save_parser.add_argument(
        "--force-upload",
        dest="prefered_action",
        action="store_const",
        const="forceupload",
    )
    save_parser.add_argument(
        "--force-download",
        dest="prefered_action",
        action="store_const",
        const="forcedownload",
    )

    save_parser.add_argument(
        "--os",
        "--platform",
        dest="platform",
        help="Target opearting system",
        choices=["windows", "osx", "linux"],
        required=True,
    )

    # SAVES CLEAR

    clear_parser = subparsers.add_parser("save-clear", help="Clear cloud game saves")
    clear_parser.add_argument("path", help="Path to sync files")
    clear_parser.add_argument("id", help="Game id")
    clear_parser.add_argument("--name", dest="dirname", default="__default")

    clear_parser.add_argument(
        "--os",
        "--platform",
        dest="platform",
        help="Target opearting system",
        choices=["windows", "osx", "linux"],
        required=True,
    )


    return parser.parse_known_args()
