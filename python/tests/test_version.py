import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_package_version_matches_cmake():
    cmake = re.search(r"project\(gfaidx VERSION ([0-9.]+)", (ROOT / "CMakeLists.txt").read_text())
    package = re.search(r'^version = "([^"]+)"', (ROOT / "pyproject.toml").read_text(), re.M)
    assert cmake and package
    assert package.group(1) == cmake.group(1)
