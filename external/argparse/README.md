# Vendored argparse

This directory vendors the header-only `p-ranav/argparse` library used by the
`gfaidx` CLI parser.

- Upstream: https://github.com/p-ranav/argparse
- Source commit: d924b84eba1f0f0adf38b20b7b4829f6f65b6570
- License: MIT, preserved in `LICENSE`

Only the public header needed by `gfaidx` is included here. Keeping this as
ordinary source files instead of a git submodule makes release tarballs
self-contained for package managers such as Bioconda. The copied header has one
whitespace-only line normalized so `git diff --check` stays clean.
