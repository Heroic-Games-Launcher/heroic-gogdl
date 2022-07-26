# Initialize argparse module and return arguments
import argparse
import sys


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

    subparsers = parser.add_subparsers(dest="command")

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
        "--with-dlcs", dest="dlcs", action="store_true", help="Should download dlcs"
    )
    download_parser.add_argument(
        "--skip-dlcs", dest="dlcs", action="store_false", help="Should skip dlcs"
    )
    download_parser.add_argument(
        "--token", "-t", dest="token", help="Provide access_token", required=True
    )
    download_parser.add_argument(
        "--max-workers",
        dest="workers_count",
        default=0,
        help="Specify number of worker threads, by default number of CPU threads",
    )

    import_parser = subparsers.add_parser(
        "import", help="Show data about game in the specified path"
    )
    import_parser.add_argument("path")
    import_parser.add_argument(
        "--token", "-t", dest="token", help="Provide access_token"
    )

    calculate_size_parser = subparsers.add_parser(
        "info", help="Calculates estimated download size and list of DLCs"
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
    calculate_size_parser.add_argument(
        "--token", "-t", dest="token", help="Provide access_token", required=True
    )
    calculate_size_parser.add_argument("--lang", "-l", help="Specify game language")
    calculate_size_parser.add_argument(
        "--max-workers",
        dest="workers_count",
        default=0,
        help="Specify number of worker threads, by default number of CPU threads",
    )

    launch_parser = subparsers.add_parser(
        "launch", help="Launch the game in specified path"
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
    launch_parser.add_argument(
        "--no-wine", action="store_true", dest="dont_use_wine", default=False
    )
    launch_parser.add_argument("--wine", dest="wine", help="Specify wine bin path")
    launch_parser.add_argument("--wine-prefix", dest="wine_prefix")
    launch_parser.add_argument("--wrapper", dest="wrapper")
    launch_parser.add_argument(
        "--override-exe", dest="override_exe", help="Override executable to be run"
    )
    launch_parser.add_argument(
        "--token", "-t", dest="token", help="Provide access_token", required=False
    )

    save_parser = subparsers.add_parser("save-sync", help="Sync game saves")
    save_parser.add_argument("path", help="Path to sync files")
    save_parser.add_argument("id", help="Game id")
    save_parser.add_argument(
        "--ts", dest="timestamp", help="Last sync timestamp", required=True
    )
    save_parser.add_argument(
        "--token",
        "-t",
        dest="token",
        help="Provide refresh_token to generate game specific auth keys",
        required=False,
    )

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

    return parser.parse_known_args()
