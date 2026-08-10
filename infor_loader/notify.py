"""Post-load daily email report built from the ETLHealth table.

After the daily batch runs, this reads today's ETLHealth rows for the configured
daily loaders and emails a report: a one-line summary (all success, or X/Y failed),
a per-target status table (Process / Status / DB Connection / Target Type / Last
Run / Error), the log file(s) for any failure attached, and a copy-pasteable rerun
command per failed loader.

The database access here is strictly read-only (a single SELECT) -- it never
truncates or loads, so it is safe to run against live prod at any time.

Entry point: :func:`send_daily_report`, wired into the CLI as the ``notify``
subcommand and the ``--notify`` flag on a batch run.
"""

from __future__ import annotations

import datetime as dt
import html
import os
from pathlib import Path
from typing import Any, Iterable

from .config import TableRef


# ETLHealth columns the report reads.
_HEALTH_COLUMNS = [
    "ProcessName",
    "ProcessID",
    "LastRunTime",
    "TaskStatus",
    "DBConnection",
    "TargetTableType",
    "TargetTableName",
    "LogFilePath",
    "Error",
    "RowCount",
    "Duration",
    "ProcessFrequency",
]

# Status buckets. FILE_NOT_FOUND is a benign skip (no fresh download), not a failure
# -- it must not count toward the failed tally (mirrors cli.run's exit-code rule).
_STATUS_SKIPPED = "FILE_NOT_FOUND"
_STATUS_FAILED = "FAILED"
_STATUS_SUCCESS = "SUCCESS"

# Sort order for the table: failures first, then skips, then successes.
_STATUS_RANK = {_STATUS_FAILED: 0, _STATUS_SKIPPED: 1, _STATUS_SUCCESS: 2}


# ── Secrets + config ────────────────────────────────────────────


def _normalize_secret_value(value: str) -> str:
    value = value.strip()
    if not value:
        return value
    if value[0] in {'"', "'"}:
        quote = value[0]
        return value[1:-1] if value.endswith(quote) else value
    comment_index = value.find(" #")
    if comment_index != -1:
        value = value[:comment_index]
    return value.strip()


def load_secrets(env_path: str = ".env") -> dict[str, str]:
    """Parse a ``.env`` file and decrypt any ``*_HASHED`` secret back to its plain key.

    Mirrors A13-MedlinePBO's ``config_loader.load_secrets``: ``CLIENT_SECRET_HASHED``
    (an ``enc::`` value) is decrypted with the ``E0_SECRET_PASSPHRASE`` passphrase and
    exposed as ``CLIENT_SECRET``.
    """
    from .secret_crypto import decrypt_secret_value, get_secret_passphrase

    if not os.path.exists(env_path):
        raise FileNotFoundError(
            f"{env_path} not found. Copy .env.example to .env (or run "
            "'python decrypt_env.py' to produce it from .env.enc)."
        )

    secrets: dict[str, str] = {}
    with open(env_path, "r", encoding="utf-8-sig") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            key, _, value = line.partition("=")
            if not key:
                continue
            secrets[key.strip()] = _normalize_secret_value(value)

    # Resolve the passphrase once (clear error if unset), then decrypt each hashed
    # secret with it so a mismatch reports a helpful message instead of a bare
    # InvalidTag from the crypto layer.
    hashed_keys = [
        key for key in ("CLIENT_SECRET", "CLIENT_SECRET_FUTURE")
        if secrets.get(f"{key}_HASHED")
    ]
    passphrase = get_secret_passphrase(required=True) if hashed_keys else None
    for secret_key in hashed_keys:
        try:
            secrets[secret_key] = decrypt_secret_value(
                secrets[f"{secret_key}_HASHED"], passphrase=passphrase
            )
        except RuntimeError:
            raise
        except Exception as exc:  # noqa: BLE001 - surface a readable passphrase-mismatch error.
            raise RuntimeError(
                f"Failed to decrypt {secret_key}_HASHED — wrong passphrase or corrupted "
                f"value. Ensure E0_SECRET_PASSPHRASE (or PBO_SECRET_PASSPHRASE) matches the "
                f"passphrase used to encrypt it."
            ) from exc
    return secrets


def load_email_config(path: str = "configs/email.yaml") -> dict[str, Any]:
    import yaml

    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Email config must be a mapping: {path}")
    return data


# ── Daily loader identity (for filtering + rerun commands) ───────


def _daily_process_map(config_ref: str) -> dict[str, str]:
    """Map ``ProcessName`` -> loader ``name`` for the daily (non-on-demand) loaders.

    ETLHealth stores the friendly ``ProcessName`` (e.g. ``Requisition Line``); the
    rerun command needs the loader ``name`` (e.g. ``requisition_line``). Loaded from
    the same configs the batch runs from, so the two never drift.
    """
    from .cli import load_loader_configs
    from .file_folder import ON_DEMAND_TAG

    process_map: dict[str, str] = {}
    for loader in load_loader_configs(config_ref):
        if ON_DEMAND_TAG in loader.tags:
            continue
        process_map[loader.process_name] = loader.name
    return process_map


# ── ETLHealth read + consolidation ──────────────────────────────


def _health_table_from_config(email_config: dict[str, Any]) -> TableRef:
    table = email_config.get("health_table")
    if not table:
        raise ValueError("email config is missing the 'health_table' block.")
    return TableRef.from_dict(table)


def fetch_health_rows(
    health_table: TableRef,
    report_date: dt.date,
    process_names: Iterable[str],
) -> list[dict[str, Any]]:
    """Read the ETLHealth rows for ``report_date`` and the given process names.

    Read-only: one parameterized SELECT. When ``process_names`` is empty (no configs
    resolved), the process filter is dropped and every row for the date is returned.
    """
    from .config import bracket_identifier
    from .db import connect_sql_server

    names = [str(name) for name in process_names]
    columns_sql = ", ".join(bracket_identifier(column) for column in _HEALTH_COLUMNS)
    sql = (
        f"SELECT {columns_sql} FROM {health_table.qualified_name()} "
        "WHERE CAST([LastRunTime] AS date) = ?"
    )
    params: list[Any] = [report_date]
    if names:
        placeholders = ", ".join("?" for _ in names)
        sql += f" AND [ProcessName] IN ({placeholders})"
        params.extend(names)

    cnxn = connect_sql_server(health_table.server, health_table.database)
    try:
        cursor = cnxn.cursor()
        cursor.execute(sql, *params)
        fetched = cursor.fetchall()
        cursor.close()
    finally:
        cnxn.close()

    return [dict(zip(_HEALTH_COLUMNS, row)) for row in fetched]


def _group_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("ProcessName") or ""),
        str(row.get("DBConnection") or ""),
        str(row.get("TargetTableType") or ""),
    )


def _run_sort_value(row: dict[str, Any]) -> tuple[Any, Any]:
    """Recency key for choosing the latest row within a group: newest LastRunTime,
    then longest Duration as a tie-break (a re-run's later start wins outright)."""
    last_run = row.get("LastRunTime") or dt.datetime.min
    duration = row.get("Duration") or 0
    return (last_run, duration)


def consolidate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the latest row per (ProcessName, DBConnection, TargetTableType).

    A loader re-run on the same day writes a fresh set of rows; the newest
    LastRunTime supersedes earlier attempts for that target.
    """
    latest: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = _group_key(row)
        current = latest.get(key)
        if current is None or _run_sort_value(row) >= _run_sort_value(current):
            latest[key] = row
    return list(latest.values())


# ── Summary + rerun commands ─────────────────────────────────────


def _process_status(process_rows: list[dict[str, Any]]) -> str:
    statuses = {str(row.get("TaskStatus") or "") for row in process_rows}
    if _STATUS_FAILED in statuses:
        return _STATUS_FAILED
    if statuses and statuses <= {_STATUS_SKIPPED}:
        return _STATUS_SKIPPED
    return _STATUS_SUCCESS


def summarize(
    consolidated: list[dict[str, Any]],
    report_date: dt.date,
    process_map: dict[str, str],
) -> dict[str, Any]:
    """Build the process-level summary from the consolidated rows."""
    by_process: dict[str, list[dict[str, Any]]] = {}
    for row in consolidated:
        by_process.setdefault(str(row.get("ProcessName") or ""), []).append(row)

    process_status = {name: _process_status(rows) for name, rows in by_process.items()}
    attempted = sorted(process_status)
    failed = sorted(n for n, s in process_status.items() if s == _STATUS_FAILED)
    skipped = sorted(n for n, s in process_status.items() if s == _STATUS_SKIPPED)
    succeeded = sorted(n for n, s in process_status.items() if s == _STATUS_SUCCESS)
    # Configured daily loaders that produced no ETLHealth row today (never ran).
    not_run = sorted(name for name in process_map if name not in process_status)

    # y = the full daily roster (ran + never-ran) so every count reads against the same
    # denominator; falls back to just what ran when no configs resolved.
    total = len(process_map) if process_map else len(attempted)
    segments = [
        f"{len(failed)}/{total} failed",
        f"{len(skipped)}/{total} skipped",
        f"{len(succeeded)}/{total} succeeded",
    ]
    if not_run:
        segments.append(f"{len(not_run)}/{total} not run")
    headline = "; ".join(segments) + f" for {report_date.isoformat()}"

    return {
        "headline": headline,
        "all_success": not failed and not skipped and not not_run,
        "has_failures": bool(failed),
        "total": total,
        "n_failed": len(failed),
        "n_skipped": len(skipped),
        "n_succeeded": len(succeeded),
        "n_not_run": len(not_run),
        "failed": failed,
        "skipped": skipped,
        "succeeded": succeeded,
        "not_run": not_run,
        "process_status": process_status,
        "by_process": by_process,
    }


def build_rerun_commands(
    summary: dict[str, Any],
    process_map: dict[str, str],
) -> list[dict[str, str]]:
    """Consolidated rerun commands for the failed AND skipped processes.

    Loaders that share the same rerun flags are batched into ONE command -- the
    CLI's ``--loader`` accepts several names (``--loader a b c``) -- so a day with
    several failures yields one copy-pasteable line per flag group instead of one
    per loader. Two groups:

      * ``--prd-only``: failed processes whose only failed targets are ``PRD``
        (staging already succeeded -- promote without re-reading the file).
      * full rerun (no phase flag): failed processes with a staging/read failure,
        plus every skipped (FILE_NOT_FOUND) process -- re-read the file once it lands.

    A process with no matching loader config cannot be batched; it gets its own
    ``# ... rerun by hand`` note. Uses the real loader ``name`` from the configs.
    """
    prd_only_loaders: list[str] = []
    prd_only_processes: list[str] = []
    full_loaders: list[str] = []
    full_processes: list[str] = []
    unmatched: list[dict[str, str]] = []

    def route(process_name: str, *, prd_only: bool) -> None:
        loader_name = process_map.get(process_name)
        if loader_name is None:
            unmatched.append(
                {
                    "command": f"# no loader config matched ProcessName '{process_name}'; rerun by hand",
                    "processes": process_name,
                    "reason": "unmatched",
                }
            )
        elif prd_only:
            prd_only_loaders.append(loader_name)
            prd_only_processes.append(process_name)
        else:
            full_loaders.append(loader_name)
            full_processes.append(process_name)

    for process_name in summary["failed"]:
        process_rows = summary["by_process"].get(process_name, [])
        failed_rows = [r for r in process_rows if str(r.get("TaskStatus")) == _STATUS_FAILED]
        prd_only = bool(failed_rows) and all(
            str(row.get("TargetTableType") or "").upper() == "PRD" for row in failed_rows
        )
        route(process_name, prd_only=prd_only)
    for process_name in summary["skipped"]:
        route(process_name, prd_only=False)

    commands: list[dict[str, str]] = []
    if prd_only_loaders:
        commands.append(
            {
                "command": (
                    "python -B run_daily_loaders.py --loader "
                    f"{' '.join(prd_only_loaders)} --prd-only --auto"
                ),
                "processes": ", ".join(prd_only_processes),
                "reason": "failed — prod promotion only",
            }
        )
    if full_loaders:
        commands.append(
            {
                "command": (
                    "python -B run_daily_loaders.py --loader "
                    f"{' '.join(full_loaders)} --auto"
                ),
                "processes": ", ".join(full_processes),
                "reason": "failed / skipped — full rerun",
            }
        )
    commands.extend(unmatched)
    return commands


def failed_log_paths(consolidated: list[dict[str, Any]]) -> list[str]:
    """Distinct log-file paths from the failed consolidated rows (order-preserving)."""
    paths: list[str] = []
    for row in consolidated:
        if str(row.get("TaskStatus")) != _STATUS_FAILED:
            continue
        path = row.get("LogFilePath")
        if path and path not in paths:
            paths.append(str(path))
    return paths


# ── HTML rendering ───────────────────────────────────────────────


_BANNER_COLORS = {
    "success": ("#e6f4ea", "#137333", "#a8dab5"),
    "warn": ("#fef7e0", "#b06000", "#fdd663"),
    "fail": ("#fce8e6", "#c5221f", "#f5b5b0"),
}
_STATUS_COLORS = {
    _STATUS_SUCCESS: ("#e6f4ea", "#137333"),
    _STATUS_FAILED: ("#fce8e6", "#c5221f"),
    _STATUS_SKIPPED: ("#fef7e0", "#b06000"),
}


def _label_connection(server: Any, db_labels: dict[str, str]) -> str:
    server = str(server or "")
    return db_labels.get(server, server or "—")


def _format_last_run(value: Any) -> str:
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return html.escape(str(value)) if value else "—"


def _truncate(text: str, limit: int = 200) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _table_sort_key(
    row: dict[str, Any],
    db_order: dict[str, int],
    process_status: dict[str, str],
) -> tuple:
    """Order rows by whole-process group. Any process with a non-SUCCESS row (failed,
    then skipped) rises to the top as a single unit; the rest follow alphabetically.
    The primary key is the *process*-level status, not the row's own, so a process's
    entire group (des1/des2 × STG/PRD/aux) stays contiguous and moves together.
    Within a process: connection in config order (des1/PRIME, then des2/O2), then STG
    before PRD."""
    process = str(row.get("ProcessName") or "")
    process_rank = _STATUS_RANK.get(process_status.get(process, ""), 99)
    conn_rank = db_order.get(str(row.get("DBConnection") or ""), len(db_order))
    type_rank = 0 if str(row.get("TargetTableType") or "").upper() == "STG" else 1
    return (process_rank, process, conn_rank, type_rank)


def render_html(
    summary: dict[str, Any],
    consolidated: list[dict[str, Any]],
    rerun_commands: list[dict[str, str]],
    *,
    report_date: dt.date,
    db_labels: dict[str, str],
    report_name: str,
    mode: str,
    recipients: list[str],
    attached_logs: list[str],
    skipped_attachments: list[str],
    generated_at: dt.datetime,
) -> str:
    if summary["has_failures"]:
        banner_kind = "fail"
    elif summary["all_success"]:
        banner_kind = "success"
    else:
        banner_kind = "warn"
    banner_bg, banner_fg, banner_border = _BANNER_COLORS[banner_kind]

    parts: list[str] = []
    parts.append(
        '<div style="font-family:Segoe UI,Arial,sans-serif;color:#202124;'
        'max-width:920px;margin:0 auto;">'
    )
    parts.append(
        f'<h2 style="margin:0 0 12px;font-size:18px;">{html.escape(report_name)} '
        f'— {report_date.isoformat()}</h2>'
    )

    # Unified summary line: every metric is the same size, each colored by its own
    # meaning -- failed red, skipped amber, succeeded green (not-run amber) -- against
    # the same y denominator. The surrounding box is tinted by the overall worst state
    # (red / amber / green). The date keeps a small muted style.
    y = summary["total"]
    seg = "font-size:17px;font-weight:600;"
    sep = '<span style="font-size:17px;color:#bdc1c6;">&nbsp;&nbsp;·&nbsp;&nbsp;</span>'
    unified = (
        f'<span style="{seg}color:#c5221f;">{summary["n_failed"]}/{y} failed</span>'
        f'{sep}<span style="{seg}color:#b06000;">{summary["n_skipped"]}/{y} skipped</span>'
        f'{sep}<span style="{seg}color:#137333;">{summary["n_succeeded"]}/{y} succeeded</span>'
    )
    if summary["n_not_run"]:
        unified += f'{sep}<span style="{seg}color:#b06000;">{summary["n_not_run"]}/{y} not run</span>'
    unified += f'{sep}<span style="font-size:12px;color:#80868b;">{report_date.isoformat()}</span>'
    parts.append(
        f'<div style="background:{banner_bg};border:1px solid {banner_border};'
        f'border-radius:6px;padding:10px 14px;margin-bottom:12px;">{unified}</div>'
    )

    # Named callout for loaders that never ran today: they carry no ETLHealth row, so
    # (unlike skips/failures) they would otherwise be invisible in the issues table.
    if summary["not_run"]:
        parts.append(
            '<p style="font-size:11px;color:#b06000;margin:0 0 12px;">'
            f'Did not run today: {", ".join(html.escape(n) for n in summary["not_run"])}</p>'
        )

    # Issues-only table: successful loaders are collapsed away; show just the process
    # groups with a failure or a skip. The whole group (des1/des2 × STG/PRD) is kept so
    # a failure's context (what succeeded alongside it) is visible.
    process_status = summary["process_status"]
    issue_rows = [
        row
        for row in consolidated
        if process_status.get(str(row.get("ProcessName") or "")) != _STATUS_SUCCESS
    ]
    if not issue_rows:
        parts.append(
            '<p style="font-size:13px;color:#137333;margin:0 0 20px;">'
            "No failed or skipped loaders — nothing needs attention.</p>"
        )
        return _finish_html(
            parts, rerun_commands, attached_logs, skipped_attachments,
            mode=mode, recipients=recipients, generated_at=generated_at,
        )

    parts.append(
        '<p style="font-size:12px;color:#5f6368;margin:0 0 6px;">'
        "Showing only loaders that failed or were skipped; successful loaders are omitted.</p>"
    )

    # Status table.
    parts.append(
        '<table cellspacing="0" cellpadding="0" style="border-collapse:collapse;'
        'width:100%;font-size:13px;line-height:1.25;margin-bottom:20px;">'
    )
    headers = ["Process", "Frequency", "Status", "DB Connection", "Target Type", "Last Run", "Error"]
    parts.append('<tr style="background:#f1f3f4;">')
    for header in headers:
        parts.append(
            f'<th style="padding:3px 9px;text-align:left;vertical-align:middle;'
            f'border-bottom:2px solid #dadce0;font-weight:600;white-space:nowrap;">{header}</th>'
        )
    parts.append("</tr>")

    # Sort at the process-group level: connection order comes from db_labels' config
    # order (first entry = des1/PRIME), and the group's rank from its process status.
    db_order = {str(server): index for index, server in enumerate(db_labels)}
    ordered = sorted(
        issue_rows, key=lambda row: _table_sort_key(row, db_order, process_status)
    )
    for row in ordered:
        status = str(row.get("TaskStatus") or "")
        status_bg, status_fg = _STATUS_COLORS.get(status, ("#f1f3f4", "#5f6368"))
        error_text = ""
        if status != _STATUS_SUCCESS and row.get("Error"):
            error_text = html.escape(_truncate(str(row.get("Error"))))
        conn_label = _label_connection(row.get("DBConnection"), db_labels)
        # O2 (des2) rows get a light-blue wash so the two DB targets read apart at a
        # glance. Background is set per-<td> (not on <tr>) because Outlook's Word
        # engine ignores row-level backgrounds.
        row_bg = "#e8f0fe" if conn_label == "O2" else "#ffffff"
        # PRD renders as a dark badge (white on grey); STG stays plain text so the
        # production row is the one that pops.
        raw_type = str(row.get("TargetTableType") or "—")
        if raw_type.upper() == "PRD":
            type_cell = (
                '<span style="background:#5f6368;color:#ffffff;padding:1px 7px;'
                'border-radius:4px;font-weight:600;white-space:nowrap;">PRD</span>'
            )
        else:
            type_cell = html.escape(raw_type)
        cells = [
            html.escape(str(row.get("ProcessName") or "—")),
            html.escape(str(row.get("ProcessFrequency") or "—")),
            (
                f'<span style="background:{status_bg};color:{status_fg};'
                f'padding:1px 7px;border-radius:10px;font-weight:600;'
                f'white-space:nowrap;">{html.escape(status or "—")}</span>'
            ),
            html.escape(conn_label),
            type_cell,
            _format_last_run(row.get("LastRunTime")),
            error_text or "—",
        ]
        parts.append("<tr>")
        for cell in cells:
            parts.append(
                f'<td style="padding:2px 9px;text-align:left;vertical-align:top;'
                f'border-bottom:1px solid #dadce0;background:{row_bg};">{cell}</td>'
            )
        parts.append("</tr>")
    parts.append("</table>")

    return _finish_html(
        parts, rerun_commands, attached_logs, skipped_attachments,
        mode=mode, recipients=recipients, generated_at=generated_at,
    )


def _finish_html(
    parts: list[str],
    rerun_commands: list[dict[str, str]],
    attached_logs: list[str],
    skipped_attachments: list[str],
    *,
    mode: str,
    recipients: list[str],
    generated_at: dt.datetime,
) -> str:
    """Append the shared tail (rerun block, attachment notes, footer) and close out.
    Called from both the normal table path and the no-issues early return."""
    # Rerun block.
    if rerun_commands:
        parts.append(
            '<h3 style="font-size:15px;margin:0 0 8px;">Rerun failed / skipped loaders</h3>'
        )
        lines: list[str] = []
        for entry in rerun_commands:
            # Unmatched processes carry a self-describing ``#`` note as their command
            # (no loader to batch); a real command gets a header naming its loaders.
            if entry["command"].startswith("#"):
                lines.append(entry["command"])
            else:
                lines.append(f"# {entry['reason']}: {entry['processes']}")
                lines.append(entry["command"])
        code = html.escape("\n".join(lines))
        parts.append(
            '<pre style="background:#202124;color:#e8eaed;padding:14px 16px;'
            'border-radius:6px;font-size:13px;overflow-x:auto;'
            f'font-family:Consolas,monospace;white-space:pre-wrap;">{code}</pre>'
        )

    # Attachment note.
    if attached_logs:
        listed = ", ".join(html.escape(os.path.basename(p)) for p in attached_logs)
        parts.append(
            f'<p style="font-size:13px;color:#5f6368;margin:12px 0 0;">'
            f'Attached log file(s): {listed}</p>'
        )
    if skipped_attachments:
        listed = "; ".join(html.escape(note) for note in skipped_attachments)
        parts.append(
            f'<p style="font-size:12px;color:#b06000;margin:6px 0 0;">'
            f'Log(s) not attached: {listed}</p>'
        )

    # Footer.
    parts.append(
        '<p style="font-size:11px;color:#9aa0a6;margin:20px 0 0;border-top:1px solid '
        '#e8eaed;padding-top:10px;">'
        f'Generated {generated_at.strftime("%Y-%m-%d %H:%M:%S")} · mode={html.escape(mode)} · '
        f'sent to {html.escape(", ".join(recipients))}</p>'
    )
    parts.append("</div>")
    return "".join(parts)


# ── Orchestration ────────────────────────────────────────────────


def _resolve_recipients(
    email_config: dict[str, Any],
    mode: str,
    to_override: list[str] | None,
) -> list[str]:
    if to_override:
        return list(to_override)
    notification = email_config.get("notification", {})
    if mode == "prd":
        return list(notification.get("recipients") or [])
    return list(notification.get("test_recipients") or [])


def send_daily_report(
    config_ref: str = "configs",
    email_config_path: str = "configs/email.yaml",
    *,
    report_date: dt.date | None = None,
    mode: str = "test",
    to_override: list[str] | None = None,
    env_path: str = ".env",
    dry_run: bool = False,
    save_html: str | None = None,
) -> int:
    """Build and (unless ``dry_run``) send the daily ETLHealth report.

    Returns 0 on success, 1 on error. Reading ETLHealth is read-only.
    """
    report_date = report_date or dt.date.today()
    email_config = load_email_config(email_config_path)
    health_table = _health_table_from_config(email_config)
    db_labels = {str(k): str(v) for k, v in (email_config.get("db_labels") or {}).items()}
    notification = email_config.get("notification", {})
    report_name = notification.get("report_name", "Daily Loader Report")

    process_map = _daily_process_map(config_ref)
    rows = fetch_health_rows(health_table, report_date, process_map.keys())
    consolidated = consolidate(rows)
    summary = summarize(consolidated, report_date, process_map)
    rerun_commands = build_rerun_commands(summary, process_map)
    log_paths = failed_log_paths(consolidated)

    print(summary["headline"])
    if not consolidated:
        print(
            f"No ETLHealth rows found for {report_date.isoformat()} "
            f"among {len(process_map)} configured daily loader(s)."
        )

    max_mb = notification.get("max_attachment_mb", email_config.get("email", {}).get("max_attachment_mb", 20))
    max_bytes = int(max_mb) * 1024 * 1024 if max_mb else None

    recipients = _resolve_recipients(email_config, mode, to_override)
    cc = list(notification.get("cc") or [])
    generated_at = dt.datetime.now()

    # Resolve attachments up front so the "not attached" note is accurate even in a
    # dry run (build_attachments does the same missing/oversized check the send uses).
    from .msgraph import build_attachments

    attachments, skipped_attachments = build_attachments(log_paths, max_bytes=max_bytes)
    attached_names = {a["name"] for a in attachments}
    attached_logs = [p for p in log_paths if os.path.basename(p) in attached_names]

    subject_prefix = notification.get("subject_prefix", report_name)
    status_word = "FAILED" if summary["has_failures"] else ("REVIEW" if not summary["all_success"] else "SUCCESS")
    subject = f"{subject_prefix} — {report_date.isoformat()} — {status_word}"

    html_body = render_html(
        summary,
        consolidated,
        rerun_commands,
        report_date=report_date,
        db_labels=db_labels,
        report_name=report_name,
        mode=mode,
        recipients=recipients or ["(none)"],
        attached_logs=attached_logs,
        skipped_attachments=skipped_attachments,
        generated_at=generated_at,
    )

    if save_html:
        Path(save_html).write_text(html_body, encoding="utf-8")
        print(f"Wrote report HTML -> {save_html}")

    if dry_run:
        print(f"[dry-run] subject: {subject}")
        print(f"[dry-run] recipients ({mode}): {recipients or '(none configured)'}")
        if log_paths:
            print(f"[dry-run] would attach: {log_paths}")
        if skipped_attachments:
            print(f"[dry-run] not attachable: {skipped_attachments}")
        print("[dry-run] no email sent.")
        return 0

    if not recipients:
        print(
            f"ERROR: no recipients configured for mode '{mode}'. Add them to "
            f"notification.{'recipients' if mode == 'prd' else 'test_recipients'} in "
            f"{email_config_path} or pass --to.",
        )
        return 1

    from .msgraph import send_email

    secrets = load_secrets(env_path)
    send_email(
        email_config,
        secrets,
        recipients,
        subject,
        html_body,
        attachment_paths=log_paths,
        cc_recipients=cc,
        max_attachment_bytes=max_bytes,
    )
    return 0
