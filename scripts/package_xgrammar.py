#!/usr/bin/env python3
"""Package Python XGrammar from the same revision as the native backend."""

import argparse
import gzip
import hashlib
import io
import json
import re
import tarfile
from pathlib import Path

BUILD_REQUIREMENTS = [
    "scikit-build-core==0.11.6",
    "apache-tvm-ffi==0.1.10",
    "cmake==3.31.6",
    "pathspec==0.12.1",
    "tomli==2.2.1; python_version < '3.11'",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    native_pin = (repo / "3rdparty/xgrammar/versions.bzl").read_text()
    commit = re.search(r'XGRAMMAR_SOURCE_COMMIT = "([0-9a-f]{40})"', native_pin).group(
        1
    )
    source = args.source.resolve()
    project = (source / "pyproject.toml").read_text()
    version = re.search(r'(?m)^version = "([^"\n]+)"', project).group(1)
    version += "+rtp." + commit[:8]
    project = re.sub(
        r'(?m)^version = "[^"\n]+"', f'version = "{version}"', project, count=1
    )
    # Build isolation must use the same FFI SDK as the tested runtime.
    project = re.sub(
        r"(?s)(\[build-system\]\s*)requires = \[.*?\]",
        lambda m: m.group(1) + "requires = " + json.dumps(BUILD_REQUIREMENTS),
        project,
        count=1,
    )
    contents = {}
    for path in sorted(source.rglob("*")):
        relative = path.relative_to(source)
        if not path.is_file() or any(
            part in {".git", "build", "__pycache__", ".github"}
            for part in relative.parts
        ):
            continue
        if path.suffix in {".pyc", ".so", ".a", ".o"}:
            continue
        contents[str(relative)] = path.read_bytes()
    contents["pyproject.toml"] = project.encode()
    # Bazel vendors this header; git archives omit the upstream dlpack submodule.
    header = repo / "3rdparty/dlpack/include/dlpack/dlpack.h"
    contents["3rdparty/dlpack/include/dlpack/dlpack.h"] = header.read_bytes()
    provenance = {
        "upstream": "https://github.com/mlc-ai/xgrammar",
        "commit": commit,
        "version": version,
        "build_requirements": BUILD_REQUIREMENTS,
        "files": {
            name: hashlib.sha256(data).hexdigest() for name, data in contents.items()
        },
    }
    contents["RTP_SOURCE_PROVENANCE.json"] = json.dumps(provenance, indent=2).encode()
    args.output.mkdir(parents=True, exist_ok=True)
    package = args.output / f"xgrammar-{version}.tar.gz"
    if package.exists():
        raise FileExistsError(package)
    prefix = f"xgrammar-{version}"
    with package.open("xb") as stream:
        with gzip.GzipFile(
            fileobj=stream, mode="wb", mtime=0, filename=""
        ) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as archive:
                for name, data in sorted(contents.items()):
                    info = tarfile.TarInfo(prefix + "/" + name)
                    info.size = len(data)
                    info.mode = 0o644
                    archive.addfile(info, io.BytesIO(data))
    sha = hashlib.sha256(package.read_bytes()).hexdigest()
    (args.output / "SHA256SUMS").write_text(sha + "  " + package.name + "\n")
    print(
        json.dumps(
            {
                "package": str(package),
                "sha256": sha,
                "commit": commit,
                "version": version,
            }
        )
    )


if __name__ == "__main__":
    main()
