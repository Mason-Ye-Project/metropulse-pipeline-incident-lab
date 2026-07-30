"""Path-containment, safe-cleanup, and run-lease primitives.

Every destructive or path-taking operation in the lab routes through this
module. The rules are deliberately conservative: a refusal is always safer than
deleting or writing outside an allowlisted run directory.
"""

from __future__ import annotations

import os
from pathlib import Path


class UnsafePathError(Exception):
    """Raised when a path fails containment or allowlist validation."""


def real(path: Path) -> Path:
    """Fully resolve a path (symlinks included) without requiring existence."""
    return Path(os.path.realpath(str(path)))


def resolve_within(base: Path, *parts: str) -> Path:
    """Join ``parts`` onto ``base`` and prove the result stays within ``base``.

    Rejects absolute escapes, ``..`` traversal, and symlink escapes. The base
    itself must already be a real, absolute path.
    """
    base_real = real(base)
    candidate = base
    for part in parts:
        if part in ("", "."):
            continue
        p = Path(part)
        if p.is_absolute() or part.startswith("~"):
            raise UnsafePathError(f"absolute or home path component refused: {part!r}")
        candidate = candidate / part
    candidate_real = real(candidate)
    try:
        candidate_real.relative_to(base_real)
    except ValueError as exc:  # pragma: no cover - defensive
        raise UnsafePathError(
            f"path {candidate_real} escapes base {base_real}"
        ) from exc
    return candidate_real


def assert_no_symlink_in_tree(root: Path) -> None:
    """Refuse if any symlink exists at or below ``root`` (fixtures/run paths)."""
    root = Path(root)
    if root.is_symlink():
        raise UnsafePathError(f"symlink refused: {root}")
    if not root.exists():
        return
    for dirpath, dirnames, filenames in os.walk(root):
        for name in list(dirnames) + list(filenames):
            p = Path(dirpath) / name
            if p.is_symlink():
                raise UnsafePathError(f"symlink refused inside tree: {p}")


def validate_run_dir(run_dir: Path, runs_root: Path) -> Path:
    """Validate that ``run_dir`` is a legitimate, allowlisted run directory.

    A run directory must be a direct child of ``runs_root``, must not be a
    symlink, and must not be an empty path, the runs root itself, the workspace
    root, the repository root, or an arbitrary user path.
    """
    if run_dir is None or str(run_dir).strip() == "":
        raise UnsafePathError("empty run path refused")
    if str(run_dir).startswith("~"):
        raise UnsafePathError("home-relative run path refused")
    run_real = real(run_dir)
    runs_real = real(runs_root)
    if run_real == runs_real:
        raise UnsafePathError("refusing to treat the runs root as a run directory")
    if run_dir.is_symlink() or run_real.is_symlink():
        raise UnsafePathError(f"symlink run directory refused: {run_dir}")
    parent = run_real.parent
    if parent != runs_real:
        raise UnsafePathError(
            f"run directory must be a direct child of {runs_real}, got parent {parent}"
        )
    # A run directory name is a single path component with our run- prefix.
    if os.sep in run_real.name or not run_real.name.startswith("run-"):
        raise UnsafePathError(f"unexpected run directory name: {run_real.name!r}")
    return run_real


def safe_rmtree_run(run_dir: Path, runs_root: Path) -> Path:
    """Delete exactly one validated run directory. Never recurse into a parent."""
    import shutil

    run_real = validate_run_dir(run_dir, runs_root)
    if not run_real.exists():
        raise UnsafePathError(f"run directory does not exist: {run_real.name}")
    shutil.rmtree(run_real)
    return run_real


def recover_lease(run_dir: Path, runs_root: Path) -> bool:
    """Remove only the selected run's operation lease (lab_architecture §10.1).

    Validates the run directory, then unlinks that run's ``.op.lock`` if present.
    It may remove only the selected run's lease and nothing else.
    """
    run_real = validate_run_dir(run_dir, runs_root)
    if not run_real.exists():
        raise UnsafePathError(f"run directory does not exist: {run_real.name}")
    lock = run_real / ".op.lock"
    if lock.is_symlink():
        raise UnsafePathError("refusing to follow a symlinked lease")
    if lock.exists():
        lock.unlink()
        return True
    return False


class RunLease:
    """Run-local exclusive operation lease via ``O_CREAT | O_EXCL``.

    Mutating commands against the same run are unsupported and fail closed. A
    normal exit removes the lease; an interrupted command leaves it for
    inspection and requires an explicit run-scoped recovery action.
    """

    def __init__(self, run_dir: Path, operation: str = "operation") -> None:
        self.lock_path = Path(run_dir) / ".op.lock"
        self.operation = operation
        self._fd: int | None = None
        self._held = False

    def acquire(self, operation: str | None = None) -> None:
        op = operation if operation is not None else self.operation
        run_name = self.lock_path.parent.name
        try:
            self._fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            raise UnsafePathError(
                f"another operation holds run {run_name!r}'s lease; if the previous "
                f"command was interrupted, run: recover-lease --run {run_name}"
            ) from exc
        os.write(self._fd, op.encode("utf-8"))
        self._held = True

    def release(self) -> None:
        # Only remove the lock if this instance actually holds it. This keeps a
        # foreign/stale lock intact when acquisition failed (fail closed).
        if not self._held:
            return
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        try:
            os.unlink(str(self.lock_path))
        except FileNotFoundError:
            pass
        self._held = False

    def __enter__(self) -> "RunLease":
        # The context-managed path MUST acquire the lease, or mutating commands
        # would run without exclusivity.
        self.acquire(self.operation)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
