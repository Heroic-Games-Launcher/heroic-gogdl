# This is Linux only right now

import os
import sys
import zlib
import zipfile

cache_path = os.path.join(
        os.getenv("XDG_CACHE_HOME", os.path.expanduser("~/.cache")), 'heroic_gogdl'
    )

vendored_packages_path = os.path.join(cache_path, 'vendored')
vendored_packages_lock = os.path.join(cache_path, 'vendored.lock')
if zipfile.is_zipfile(os.path.dirname(__file__)):
    with zipfile.ZipFile(os.path.dirname(__file__)) as zf:
        should_extract = True
        gogdl_xdelta = os.path.join(vendored_packages_path, 'gogdl_xdelta3.abi3.so')
        xdelta = zf.getinfo('gogdl_xdelta3.abi3.so')
        if os.path.exists(gogdl_xdelta):
            with open(gogdl_xdelta, 'rb') as f:
                crc = zlib.crc32(f.read())
                should_extract = xdelta.CRC != crc
        if should_extract:
            extracted = zf.extract(xdelta, vendored_packages_path)
            extracted = os.chmod(extracted, xdelta.external_attr >> 16)

    sys.path.insert(0, vendored_packages_path)

import gogdl.cli
gogdl.cli.main()