import os
import json
import sys
import subprocess

# Supports launching linux builds
def launch(arguments):
    print(arguments)
    info = load_game_info(arguments.path, arguments.id, arguments.platform)

    wrapper = ''
    envvars = {}

    if arguments.dont_use_wine or sys.platform == 'win32':
        wrapper = arguments.wrapper
    else:
        envvars['WINEPREFIX'] = arguments.wine_prefix
        wrapper = f'{arguments.wine}'

    primary_task = get_primary_task(info)
    launch_arguments = primary_task.get('arguments')
    executable = os.path.join(arguments.path, primary_task['path'])
    if launch_arguments is None:
        launch_arguments = []
    command = list()
    command.append(wrapper)
    command.append(executable)
    command.extend(launch_arguments)
    
    enviroment = os.environ.copy()
    enviroment.update(envvars)
    

    subprocess.Popen(command, cwd=arguments.path, env=enviroment)


def get_primary_task(info):
    primaryTask = None
    for task in info['playTasks']:
        if task.get('isPrimary') == True:
            return task

def load_game_info(path, id, platform):
    filename = f'goggame-{id}.info'
    abs_path = (os.path.join(path, filename) if platform == "windows" else os.path.join(path, 'game', filename)) if platform != "osx" else os.path.join(path, 'Contents', 'Resources', filename)
    if not os.path.isfile(abs_path):
        exit(1)
    with open(abs_path) as f:
        data = f.read()
        f.close()
        return json.loads(data)