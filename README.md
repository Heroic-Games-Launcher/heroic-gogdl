# heroic-gogdl

GOG download module for [Heroic Games Launcher](https://github.com/Heroic-Games-Launcher/HeroicGamesLauncher)

## Important note

This is **not** user friendly cli, it's meant to be used by some other application wanting to download game files, manage cloud saves or conveniently launch the game

## Contributing

The only python dependency needed at this moment is `requests`

You can install it using your Linux distribution package manager or using pip

```
pip install requests
```

To run a code locally, use `bin/gogdl` script, which is a convenient python wrapper

gogdl now manages authentication, so it no longer needs --token parameter, although you now need to provide a path to json file where the tokens will be stored
Heroic uses `$XDG_CONFIG_HOME/heroic/gog_store/auth.json`

Here is the command to pull the source code

```bash
git clone https://github.com/Heroic-Games-Launcher/heroic-gogdl
cd heroic-gogdl
python -m venv venv
. venv/bin/activate
pip install .
gogdl --help
```

If you have any questions ask on our [Discord](https://discord.com/invite/rHJ2uqdquK) or through GitHub issue

## Building PyInstaller executable

If you wish to test the gogdl in Heroic flatpak you likely need to build `gogdl` executable using pyinstaller

- Get pyinstaller

```bash
pip install pyinstaller
```

- Build the binary (assuming you are in heroic-gogdl direcory)

```bash
pip install -e . # Ensure you build the C code to python module
pyinstaller --onefile --name gogdl gogdl/cli.py
```

## Great resources about GOG API

- https://github.com/Lariaa/GameLauncherResearch/wiki/
- https://github.com/Sude-/lgogdownloader
- https://gogapidocs.readthedocs.io/en/latest/
