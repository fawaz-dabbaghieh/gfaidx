from setuptools import find_packages, setup

setup(
    name="pygfaidx",
    version="0.1.0",
    description="Python helpers for indexed GFA graphs",
    packages=find_packages(where="python"),
    package_dir={"": "python"},
    entry_points={
        "console_scripts": [
            "pygfaidx-bfs=pygfaidx.cli_bfs:main",
        ]
    },
)
