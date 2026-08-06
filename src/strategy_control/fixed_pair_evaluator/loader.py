"""Manifest-bound development loader and fail-closed holdout guard."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


class DataIdentityError(ValueError):
    """Input identity or boundary contract is invalid."""


@dataclass(frozen=True)
class DevelopmentManifest:
    source_root: Path
    allowlisted_paths: tuple[str, ...]
    source_commit: str
    manifest_sha256: str

    def resolve_development(self, relative_path: str) -> Path:
        if relative_path not in self.allowlisted_paths:
            raise DataIdentityError("path is not in the development allowlist")
        if "2026" in relative_path:
            raise DataIdentityError("development loader rejects holdout-labelled paths")
        path = (self.source_root / relative_path).resolve()
        if not path.is_relative_to(self.source_root.resolve()):
            raise DataIdentityError("path escapes source root")
        return path


class HoldoutGuard:
    """A permanently closed guard for Phase 4 development runs."""

    def __init__(self) -> None:
        self._resolved = False

    @property
    def resolved_count(self) -> int:
        return int(self._resolved)

    def reject(self, path: Path) -> None:
        if "2026" in str(path):
            raise DataIdentityError("holdout path resolution is prohibited")
