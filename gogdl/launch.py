import os
import json
import sys
import subprocess
import shlex

# Supports launching linux builds
def launch(arguments, unknown_args):
    print(arguments)
    info = load_game_info(arguments.path, arguments.id, arguments.platform)

    wrapper = []
    envvars = {}

    unified_platform = {
        'win32':'windows',
        'darwin':'osx',
        'linux':'linux'
    }
    command = list()
    working_dir = arguments.path
    if type(info) != str:
        if arguments.dont_use_wine == True or sys.platform == 'win32':
            wrapper_arg = arguments.wrapper
            wrapper = shlex.split(wrapper_arg)
        elif arguments.platform != unified_platform[sys.platform]:
            if arguments.wine_prefix:
                envvars['WINEPREFIX'] = arguments.wine_prefix
            wrapper = [arguments.wine]

        primary_task = get_primary_task(info)
        launch_arguments = primary_task.get('arguments')
        compatibility_flags = primary_task.get('compatibilityFlags')
        executable = os.path.join(arguments.path, primary_task['path'])
        if arguments.platform == 'linux':
            executable = os.path.join(arguments.path, 'game', primary_task['path'])
        if launch_arguments is None:
            launch_arguments = []
        if type(launch_arguments) == str:
            launch_arguments = shlex.split(launch_arguments)
        if compatibility_flags is None:
            compatibility_flags = []

        relative_working_dir =  primary_task['workingDir'] if primary_task.get('workingDir') else ''
        if sys.platform != 'win32':
            relative_working_dir = relative_working_dir.replace("\\", os.sep)
            executable = executable.replace("\\", os.sep)
        working_dir = os.path.join(arguments.path, relative_working_dir)
        
        if not os.path.exists(executable):
            executable = get_case_insensitive_name(arguments.path, executable)
        if len(wrapper) > 0 and wrapper[0] is not None:
            command.extend(wrapper)
        if arguments.override_exe:
            command.append(arguments.override_exe)
            working_dir = os.path.split(arguments.override_exe)[0]
        else:
            command.append(executable)
        command.extend(launch_arguments)
    else:
        if arguments.override_exe:
            command.append(arguments.override_exe)
            working_dir = os.path.split(arguments.override_exe)[0]
        else:
            command.append(info)

    command.extend(unknown_args)
    enviroment = os.environ.copy()
    enviroment.update(envvars)
    print("Launch command:",command)
    # Handle case sensitive file systems
    if not os.path.exists(working_dir):
        working_dir = get_case_insensitive_name(arguments.path, working_dir)
    
    process = subprocess.Popen(command, cwd=working_dir, env=enviroment)
    status = process.wait()
    sys.exit(status)

def get_primary_task(info):
    primaryTask = None
    for task in info['playTasks']:
        if task.get('isPrimary') == True:
            return task

def load_game_info(path, id, platform):
    filename = f'goggame-{id}.info'
    abs_path = (os.path.join(path, filename) if platform == "windows" else os.path.join(path, 'start.sh')) if platform != "osx" else os.path.join(path, 'Contents', 'Resources', filename)
    if not os.path.isfile(abs_path):
        sys.exit(1)
    if platform == 'linux':
        return abs_path
    with open(abs_path) as f:
        data = f.read()
        f.close()
        return json.loads(data)


def get_case_insensitive_name(root, path):
    if not root[len(root)-1] in ["/","\\"]:
        root = root + "/"
    s_working_dir = path.replace(root, '').split(os.sep)
    for directory in s_working_dir:
        dir_list = os.listdir(root)
        for existing_dir in dir_list:
            if existing_dir.lower() == directory.lower():
                root = os.path.join(root, existing_dir)
    return root