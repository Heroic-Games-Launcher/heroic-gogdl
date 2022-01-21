# Initialize argparse module and return arguments
import argparse

def init_parser():
    parser = argparse.ArgumentParser(description='GOG downloader for Heroic Games Launcher')
    subparsers = parser.add_subparsers(dest='command', required=True)
    download_parser = subparsers.add_parser('download', help='Download/update game')
    download_parser.add_argument('id', help='Game id')
    download_parser.add_argument('--lang', '-l', help='Specify game language')
    download_parser.add_argument('--build', '-b', dest="build", help='Specify buildId (allows repairing)')
    download_parser.add_argument('--path', '-p', dest='path', help='Specify download path', required=True)
    download_parser.add_argument('--platform', '--os',dest='platform', help='Target opearting system', choices=['windows', 'osx'])
    download_parser.add_argument('--with-dlcs', dest="dlcs", action="store_true", help='Should download dlcs')
    download_parser.add_argument('--skip-dlcs', dest="dlcs", action="store_false", help='Should skip dlcs')
    download_parser.add_argument('--token', '-t', dest='token',help='Provide access_token', required=True)

    import_parser = subparsers.add_parser('import', help='Show data about game in the specified path')
    import_parser.add_argument('path')
    import_parser.add_argument('--token', '-t', dest='token', help='Provide access_token')

    calculate_size_parser = subparsers.add_parser('info', help='Calculates estimated download size and list of DLCs')
    calculate_size_parser.add_argument('id')
    calculate_size_parser.add_argument('--platform', '--os',dest='platform', help='Target opearting system', choices=['windows', 'osx'])
    calculate_size_parser.add_argument('--build', '-b', dest="build", help='Specify buildId')
    calculate_size_parser.add_argument('--token', '-t', dest='token', help='Provide access_token', required=True)
    calculate_size_parser.add_argument('--lang', '-l', help='Specify game language')


    # TODO Create parser
    return parser.parse_args()