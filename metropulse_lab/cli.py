"""Command-line interface: ``python -m metropulse_lab <command>``.

Human output is concise; ``--json`` emits a stable machine-readable object.
Every command prints the incident id, run id, lifecycle state, and run-relative
paths. Normal output never exposes the user's name, home directory, or absolute
checkout path.
"""

from __future__ import annotations

import argparse
import json
from typing import Any

from . import commands, config, state
from .safety import UnsafePathError
from .state import LifecycleError


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m metropulse_lab",
        description="MetroPulse incident lab (fictional, synthetic transit data).",
    )
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_run_cmd(name: str, help_text: str) -> argparse.ArgumentParser:
        p = sub.add_parser(name, help=help_text)
        p.add_argument("--run", required=True, help="run id")
        p.add_argument("--workspace", default=None, help="workspace parent (dev/test only)")
        return p

    def add_incident_cmd(name: str, help_text: str) -> argparse.ArgumentParser:
        p = sub.add_parser(name, help=help_text)
        p.add_argument("--incident", required=True, help="incident id, e.g. MP-01")
        p.add_argument("--workspace", default=None, help="workspace parent (dev/test only)")
        return p

    sub.add_parser("doctor", help="check Python and SQLite capabilities (no network)")

    incidents_p = sub.add_parser("incidents", help="list incidents")
    incidents_p.add_argument("action", nargs="?", default="list", choices=["list"])

    add_incident_cmd("create", "create a run and build the clean baseline")
    add_incident_cmd("start", "create + inject + faulty run + detect + evidence")

    add_run_cmd("inject", "arm the incident fault")
    add_run_cmd("run", "execute the faulty pipeline")
    add_run_cmd("detect", "detect the failed invariant (exit 10 when found)")
    add_run_cmd("evidence", "produce the read-only evidence pack")
    add_run_cmd("contain", "contain blast radius, preserve evidence")
    add_run_cmd("repair", "apply the smallest safe rule change")
    add_run_cmd("recover", "execute the bounded replay/rollback/no-replay decision")
    add_run_cmd("verify", "verify business data, control state, and side effects")
    add_run_cmd("reset", "remove exactly this run directory")
    add_run_cmd("recover-lease", "remove a stale operation lease for this run")
    add_run_cmd("reference-recovery", "contain + repair + recover + verify")

    test_all_p = sub.add_parser("test-all", help="run every implemented reference lifecycle")
    test_all_p.add_argument("--workspace", default=None)

    return parser


_DISPATCH = {
    "doctor": commands.doctor,
    "incidents": commands.incidents_list,
    "create": commands.create,
    "start": commands.start,
    "inject": commands.inject,
    "run": commands.run,
    "detect": commands.detect,
    "evidence": commands.evidence,
    "contain": commands.contain,
    "repair": commands.repair,
    "recover": commands.recover,
    "verify": commands.verify,
    "reset": commands.reset,
    "recover-lease": commands.recover_lease,
    "reference-recovery": commands.reference_recovery,
    "test-all": commands.test_all,
}


def _print_human(command: str, data: dict[str, Any]) -> None:
    d = data.get("data", {})
    if command == "doctor":
        print(f"doctor: {d['status']}  python={d['python_version']} "
              f"sqlite={d['sqlite_version']} window_functions={d['window_functions']}")
        return
    if command == "incidents":
        for row in d["incidents"]:
            mark = "x" if row["implemented"] else " "
            title = row.get("reader_title", "(not implemented)")
            print(f"[{mark}] {row['incident_id']}  {title}")
        return
    if command == "reset":
        print(f"reset: removed run {d['removed_run']}")
        return
    if command == "recover-lease":
        state = "removed stale lease" if d["lease_removed"] else "no stale lease found"
        print(f"recover-lease: run {d['run']} -> {state}")
        return
    if command == "test-all":
        for r in d["results"]:
            print(f"{r['incident_id']}: {'PASS' if r['passed'] else 'FAIL'}")
        return
    # generic lifecycle step
    run_id = d.get("run_id")
    incident_id = d.get("incident_id")
    status = d.get("status") or d.get("verify") or d.get("detection")
    print(f"{command}: incident={incident_id} run={run_id} -> {status}")
    if "fields" in d and d["fields"]:
        print("  " + json.dumps(d["fields"], sort_keys=True))
    if d.get("assertions"):
        for a in d["assertions"]:
            print(f"  [{a['status']}] {a['assertion_id']} ({a['dimension']}): "
                  f"expected={a['expected']} actual={a['actual']}")


def main(argv: list[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    handler = _DISPATCH.get(args.command)
    if handler is None:
        parser.error(f"unknown command: {args.command}")
        return config.EXIT_USAGE
    try:
        result = handler(args)
    except commands.CommandError as exc:
        print(f"error: {exc}", flush=True)
        return exc.exit_code
    except LifecycleError as exc:
        print(f"error: invalid lifecycle state: {exc}", flush=True)
        return config.EXIT_BAD_STATE
    except UnsafePathError as exc:
        print(f"error: refused unsafe operation: {exc}", flush=True)
        return config.EXIT_UNSAFE
    if args.json:
        print(json.dumps(result["data"], sort_keys=True, indent=2))
    else:
        _print_human(args.command, result)
    return result["exit_code"]


def _console_main() -> int:
    """Entry point for the optional installed ``metropulse-lab`` script."""
    import sys

    return main(sys.argv[1:])
