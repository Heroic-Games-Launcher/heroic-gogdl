import os
import glob
import json
import logging
from sys import exit
from gogdl import constants
import requests

def get_info(args, unknown_args):
    logger = logging.getLogger("IMPORT")
    path = args.path
    if not os.path.exists(path):
        logger.error("Provided path is invalid!")
        exit(1)
    game_details = load_game_details(path)

    info_file = game_details[0]
    build_id_file = game_details[1]
    platform = game_details[2]
    with_dlcs = game_details[3]
    build_id = ''
    installed_language = None
    info = {}
    if platform != 'linux':
        f = open(info_file, 'r')
        info = json.loads(f.read())
        f.close()

        title = info['name']
        game_id = info['rootGameId']
        build_id = info.get("buildId")
        if 'languages' in info:
            installed_language = info['languages'][0]
        elif 'language' in info:
            installed_language = info['language']
        else:
            installed_language = 'en-US'
        if build_id_file:
            f = open(build_id_file, 'r')
            build = json.loads(f.read())
            f.close()
            build_id = build.get("buildId")

    version_name = build_id
    if build_id and platform != 'linux':
        # Get version name
        builds_res = requests.get(f'{constants.GOG_CONTENT_SYSTEM}/products/{game_id}/os/{platform}/builds?generation=2')
        builds = builds_res.json()
        target_build = builds['items'][0]
        for build in builds['items']:
            if build['build_id'] == build_id:
                target_build = build
                break
        version_name = target_build['version_name']
    if platform == 'linux' and os.path.exists(os.path.join(path,'gameinfo')):
        # Linux version installed using installer
        gameinfo_file = open(os.path.join(path,'gameinfo'),'r')
        data = gameinfo_file.read()
        lines = data.split('\n')
        game_id = lines[4]
        title = lines[1]
        build_id = lines[6]
        version_name = lines[1]
        if not installed_language:
            installed_language = lines[3]

    print(json.dumps({
        "appName": game_id,
        "buildId": build_id,
        "title": title,
        "tasks": info["playTasks"] if info and info.get('playTasks') else None,
        "installedLanguage": installed_language,
        "installedWithDlcs":with_dlcs,
        "platform":platform,
        "versionName":version_name
    }))

def load_game_details(path):
    found = glob.glob(os.path.join(path, 'goggame-*.info'))
    build_id = glob.glob(os.path.join(path, 'goggame-*.id'))
    platform = "windows"
    if not found:
        found = glob.glob(os.path.join(path, "Contents", "Resources", 'goggame-*.info'))
        build_id = glob.glob(os.path.join(path, "Contents", "Resources", 'goggame-*.id'))
        platform='osx'
    if not found:
        found = glob.glob(os.path.join(path,'game', 'goggame-*.info'))
        build_id = glob.glob(os.path.join(path, "game", 'goggame-*.id'))
        platform = 'linux'
    if not found:
        if os.path.exists(os.path.join(path,'gameinfo')):
            return (None, None, 'linux')
    return (found[0], build_id[0] if build_id else None, platform, len(found) > 1)