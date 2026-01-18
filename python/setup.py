from setuptools import find_packages, setup

setup(
    name="pygfaidx",
    version="0.1.0",
    description="Python interface and command line tools for indexed GFA with gfaidx",
    author='Fawaz Dabbaghie',
    author_email='fawaz.dabbaghieh@gmail.com',
    license="LICENSE",
    packages=find_packages(),
    long_description=open("README.md").read(),
    long_description_content_type='text/markdown',
    # package_dir={"": "python"},
    entry_points={
        "console_scripts": [
            "pygfaidx-bfs=pygfaidx.cli_bfs:main",
        ]
    },
)
