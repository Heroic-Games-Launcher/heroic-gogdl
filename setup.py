from setuptools import setup
from gogdl import version

setup(
    name="gogdl",
    version=version,
    license='GPL-3',
    author="imLinguin",
    packages=[
        "gogdl",
        "gogdl.dl"
    ],
    entry_points=dict(
        console_scripts=['gogdl = gogdl.cli:main']
    ),
    install_requires=[
        "requests",
        "setuptools"
    ]
)