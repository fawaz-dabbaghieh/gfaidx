# Making a gfaidx release

1. **Set the version** in two places, which must match (a Python test checks
   this):
   - `CMakeLists.txt`, line 2: `project(gfaidx VERSION X.Y.Z ...)`
   - `pyproject.toml`: `version = "X.Y.Z"`

   `gfaidx --version` prints the CMake version.
2. **Merge to `main`** through a pull request, so CI builds and tests the
   change.
3. **Create the release** on GitHub with the tag `vX.Y.Z`, or push the tag:

   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

The tag starts the `Release binaries` workflow
(`.github/workflows/release-binaries.yml`). It builds and tests four packages:

- Linux x86-64 and ARM64, statically linked against musl
- macOS Intel and ARM64, linked only to libraries supplied by macOS

It stops before publishing if `gfaidx --version` does not match the tag. When
all four pass, it writes a `SHA256SUMS` file and attaches the packages to the
release, creating the release if the tag was pushed without one. This takes a
few minutes after the tag appears.

If the workflow failed and was fixed after the tag was pushed, run it again
from the Actions tab: choose `Release binaries`, then `Run workflow`, and enter
the existing tag as `release_tag`.

The Bioconda package is built from the recipe in
[bioconda-recipes](https://github.com/bioconda/bioconda-recipes), which needs
its own update for each release.
