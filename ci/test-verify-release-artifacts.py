#!/usr/bin/env python3
"""Offline adversarial fixtures for the release archive gate."""
import hashlib
import importlib.util
import io
import json
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("release_artifacts", Path(__file__).with_name("verify-release-artifacts.py"))
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)
VERSION = "0.8.9"
COMMIT = "1" * 40


class ReleaseArtifactsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def fixture(self, edit=None):
        outer = []
        for platform, (extension, target, desktop) in release.PLATFORMS.items():
            root = f"serial-platform-v{VERSION}-{platform}"
            suffix = ".exe" if "windows" in platform else ""
            files = {f"{binary}{suffix}": b"test executable" for binary in ("serial", "seriald", "serialctl", "serial-mcp")}
            files[desktop] = b"desktop fixture"
            files["docs/MCP_TOOLS.md"] = b"tools documentation"
            if platform.startswith("macos"):
                files["Serial Platform.app/Contents/Info.plist"] = b"plist fixture"
                for binary in ("serial", "seriald"):
                    files[f"Serial Platform.app/Contents/Resources/bin/{binary}"] = b"sidecar fixture"
            metadata = {"version": VERSION, "git_commit": COMMIT, "cargo_profile": "release", "rust_target": target, "mcp_tool_count": 18}
            files["BUILD-INFO.json"] = json.dumps(metadata).encode()
            if edit:
                edit(platform, files)
            files["MANIFEST.sha256"] = "".join(f"{hashlib.sha256(data).hexdigest()}  ./{name}\n" for name, data in files.items()).encode()
            path = self.directory / (root + extension)
            if extension == ".tar.gz":
                with tarfile.open(path, "w:gz") as archive:
                    for name, data in files.items():
                        member = tarfile.TarInfo(root + "/" + name)
                        member.size = len(data)
                        archive.addfile(member, io.BytesIO(data))
            else:
                with zipfile.ZipFile(path, "w") as archive:
                    for name, data in files.items():
                        archive.writestr(root + "/" + name, data)
            outer.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n")
        (self.directory / "SHA256SUMS").write_text("".join(outer))

    def verify(self):
        release.verify_release(self.directory, VERSION, COMMIT)

    def rewrite_windows_zip(self, transform):
        path = next(self.directory.glob("*windows*.zip"))
        with zipfile.ZipFile(path) as archive:
            entries = [(member, archive.read(member)) for member in archive.infolist()]
        entries = transform(entries)
        with zipfile.ZipFile(path, "w") as archive:
            for member, data in entries:
                archive.writestr(member, data)
        checksums = self.directory / "SHA256SUMS"
        lines = checksums.read_text().splitlines()
        lines = [f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}" if line.endswith(path.name) else line for line in lines]
        checksums.write_text("\n".join(lines) + "\n")

    def test_inner_checksum_mismatch(self):
        self.fixture()
        self.rewrite_windows_zip(lambda entries: [(member, b"changed" if member.filename.endswith("/serial.exe") else data) for member, data in entries])
        with self.assertRaisesRegex(ValueError, "inner checksum mismatch"):
            self.verify()

    def test_manifest_covers_every_regular_file(self):
        self.fixture()
        def edit(entries):
            return [(member, b"\n".join(data.splitlines()[1:]) + b"\n" if member.filename.endswith("/MANIFEST.sha256") else data) for member, data in entries]
        self.rewrite_windows_zip(edit)
        with self.assertRaisesRegex(ValueError, "cover every regular payload"):
            self.verify()

    def test_escaping_symlink(self):
        self.fixture()
        def edit(entries):
            info = zipfile.ZipInfo("serial-platform-v0.8.9-windows-x86_64/unsafe-link")
            info.create_system = 3
            info.external_attr = 0o120777 << 16
            return entries + [(info, b"../../outside")]
        self.rewrite_windows_zip(edit)
        with self.assertRaisesRegex(ValueError, "symlink escapes"):
            self.verify()

    def test_valid_four_platform_release(self):
        self.fixture()
        self.verify()

    def test_wrong_source_commit(self):
        def edit(platform, files):
            metadata = json.loads(files["BUILD-INFO.json"])
            metadata["git_commit"] = "2" * 40
            files["BUILD-INFO.json"] = json.dumps(metadata).encode()
        self.fixture(edit)
        with self.assertRaisesRegex(ValueError, "git_commit"):
            self.verify()

    def test_debug_build_refused(self):
        def edit(platform, files):
            metadata = json.loads(files["BUILD-INFO.json"])
            metadata["cargo_profile"] = "debug"
            files["BUILD-INFO.json"] = json.dumps(metadata).encode()
        self.fixture(edit)
        with self.assertRaisesRegex(ValueError, "cargo_profile"):
            self.verify()

    def test_missing_desktop(self):
        def edit(platform, files):
            files.pop(release.PLATFORMS[platform][2])
        self.fixture(edit)
        with self.assertRaisesRegex(ValueError, "missing required"):
            self.verify()

    def test_missing_sidecar(self):
        def edit(platform, files):
            if platform.startswith("macos"):
                files.pop("Serial Platform.app/Contents/Resources/bin/seriald")
        self.fixture(edit)
        with self.assertRaisesRegex(ValueError, "missing required"):
            self.verify()

    def test_archive_corruption(self):
        self.fixture()
        path = next(self.directory.glob("*.zip"))
        path.write_bytes(path.read_bytes() + b"corrupted")
        with self.assertRaisesRegex(ValueError, "archive checksum"):
            self.verify()

    def test_path_traversal(self):
        self.fixture(lambda platform, files: files.update({"../escape": b"unsafe"}))
        with self.assertRaisesRegex(ValueError, "unsafe archive path"):
            self.verify()

    def test_extra_asset(self):
        self.fixture()
        (self.directory / "debug.zip").write_bytes(b"unexpected")
        with self.assertRaisesRegex(ValueError, "exactly four"):
            self.verify()

    def test_duplicate_checksums(self):
        self.fixture()
        path = self.directory / "SHA256SUMS"
        path.write_text(path.read_text() * 2)
        with self.assertRaisesRegex(ValueError, "duplicate checksum"):
            self.verify()

    def test_missing_archive(self):
        self.fixture()
        next(self.directory.glob("*.zip")).unlink()
        with self.assertRaisesRegex(ValueError, "exactly four"):
            self.verify()

    def test_malformed_checksum(self):
        with self.assertRaisesRegex(ValueError, "invalid checksum"):
            release.checksums("bad manifest\n")

    def test_invalid_source_sha(self):
        self.fixture()
        with self.assertRaisesRegex(ValueError, "full lowercase SHA"):
            release.verify_release(self.directory, VERSION, "HEAD")


if __name__ == "__main__":
    unittest.main()
