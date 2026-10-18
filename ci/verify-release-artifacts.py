#!/usr/bin/env python3
"""Verify all four release archives before the workflow may create a tag.

Uses only the Python standard library; never extracts untrusted archive paths.
Checks outer digests, every inner manifest entry, exact source commit/version,
release profile, platform target, and the expected CLI/desktop payloads.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import posixpath
import re
import stat
import tarfile
import zipfile
from pathlib import Path, PurePosixPath

PLATFORMS = {
    "linux-x86_64-ubuntu20.04": (".tar.gz", "x86_64-unknown-linux-gnu.2.31", "Serial Platform.AppImage"),
    "windows-x86_64": (".zip", "x86_64-pc-windows-gnu", "Serial Platform.exe"),
    "macos-aarch64": (".zip", "aarch64-apple-darwin", "Serial Platform.app/Contents/MacOS/Serial Platform"),
    "macos-x86_64": (".zip", "x86_64-apple-darwin", "Serial Platform.app/Contents/MacOS/Serial Platform"),
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def digest(stream) -> str:
    result = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        result.update(chunk)
    return result.hexdigest()


def safe_path(name: str) -> str:
    while name.startswith("./"):
        name = name[2:]
    require(bool(name) and not name.startswith("/") and "\\" not in name, f"unsafe archive path: {name!r}")
    require(".." not in PurePosixPath(name).parts, f"unsafe archive path: {name!r}")
    return name.rstrip("/")


def checksums(text: str) -> dict[str, str]:
    result = {}
    for line in text.splitlines():
        match = re.fullmatch(r"([0-9A-Fa-f]{64}) [ *](.+)", line)
        require(match is not None, f"invalid checksum line: {line!r}")
        checksum, name = match.groups()
        name = safe_path(name)
        require(name not in result, f"duplicate checksum entry: {name}")
        result[name] = checksum.lower()
    require(bool(result), "checksum manifest is empty")
    return result


def verify_archive(path: Path, root: str, target: str, desktop: str, version: str, commit: str) -> None:
    archive = tarfile.open(path, "r:gz") if path.name.endswith(".tar.gz") else zipfile.ZipFile(path)
    with archive:
        members = archive.getmembers() if isinstance(archive, tarfile.TarFile) else archive.infolist()
        files = {}
        names = set()
        for member in members:
            name = safe_path(member.name if isinstance(archive, tarfile.TarFile) else member.filename)
            require(name not in names, f"duplicate archive member: {name}")
            names.add(name)
            require(name == root or name.startswith(root + "/"), f"unexpected archive root: {name}")
            if isinstance(archive, tarfile.TarFile):
                regular, directory, symlink = member.isfile(), member.isdir(), member.issym()
                link = member.linkname if symlink else ""
            else:
                symlink = stat.S_ISLNK(member.external_attr >> 16)
                directory = member.is_dir()
                regular = not directory and not symlink
                link = archive.read(member).decode("utf-8") if symlink else ""
            require(regular or directory or symlink, f"unsupported archive member: {name}")
            if symlink:
                require(not link.startswith("/") and "\\" not in link, f"unsafe symlink: {name}")
                resolved = posixpath.normpath(posixpath.join(posixpath.dirname(name), link))
                require(resolved.startswith(root + "/"), f"symlink escapes package: {name}")
            if regular:
                files[name[len(root) + 1:]] = member

        def open_member(name):
            member = files[name]
            return archive.extractfile(member) if isinstance(archive, tarfile.TarFile) else archive.open(member)

        suffix = ".exe" if target.endswith("windows-gnu") else ""
        required = {f"{binary}{suffix}" for binary in ("serial", "seriald", "serialctl", "serial-mcp")}
        required.update({desktop, "BUILD-INFO.json", "MANIFEST.sha256", "docs/MCP_TOOLS.md"})
        if "apple-darwin" in target:
            required.update({f"Serial Platform.app/Contents/Resources/bin/{binary}" for binary in ("serial", "seriald")})
            required.add("Serial Platform.app/Contents/Info.plist")
        require(required <= files.keys(), f"missing required payloads in {path.name}: {sorted(required - files.keys())}")
        with open_member("BUILD-INFO.json") as stream:
            metadata = json.load(stream)
        for field, expected in {"version": version, "git_commit": commit, "cargo_profile": "release", "rust_target": target, "mcp_tool_count": 18}.items():
            require(metadata.get(field) == expected, f"{path.name}: unexpected {field}: {metadata.get(field)!r}")
        with open_member("MANIFEST.sha256") as stream:
            manifest = checksums(stream.read().decode("utf-8"))
        require(set(manifest) == set(files) - {"MANIFEST.sha256"}, f"{path.name}: manifest must cover every regular payload exactly once")
        for name, expected in manifest.items():
            with open_member(name) as stream:
                require(digest(stream) == expected, f"{path.name}: inner checksum mismatch for {name}")
    print(f"Verified source, payloads, and manifest: {path.name}")


def verify_release(directory: Path, version: str, commit: str) -> None:
    require(re.fullmatch(r"[0-9a-f]{40}", commit) is not None, "source commit must be a full lowercase SHA")
    require(re.fullmatch(r"(0|[1-9][0-9]*)\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.-]+)?", version) is not None, "invalid release version")
    expected = {f"serial-platform-v{version}-{platform}{extension}" for platform, (extension, _, _) in PLATFORMS.items()}
    require({path.name for path in directory.iterdir()} == expected | {"SHA256SUMS"}, "release directory must contain exactly four archives and SHA256SUMS")
    manifest = checksums((directory / "SHA256SUMS").read_text())
    require(set(manifest) == expected, "SHA256SUMS must cover exactly the four release archives")
    for platform, (extension, target, desktop) in PLATFORMS.items():
        root = f"serial-platform-v{version}-{platform}"
        path = directory / (root + extension)
        with path.open("rb") as stream:
            require(digest(stream) == manifest[path.name], f"archive checksum mismatch: {path.name}")
        verify_archive(path, root, target, desktop, version, commit)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("version")
    parser.add_argument("commit")
    arguments = parser.parse_args()
    try:
        verify_release(arguments.directory, arguments.version, arguments.commit)
    except (ValueError, OSError, KeyError, tarfile.TarError, zipfile.BadZipFile) as error:
        parser.exit(1, f"error: {error}\n")


if __name__ == "__main__":
    main()
