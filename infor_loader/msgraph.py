"""Microsoft Graph email sending, adapted from A13-MedlinePBO/src/msgraph.py.

Only the send side is needed here: acquire a client-credentials token for the
service account and POST a ``sendMail`` with an HTML body and any log-file
attachments. ``requests`` and the secrets are only touched when a mail is actually
sent, so importing this module has no cost for the normal loader path.
"""

from __future__ import annotations

import base64
import os
from typing import Any, Iterable

# ``requests`` is imported lazily inside the send functions so that building/previewing
# a report (build_attachments) works with only stdlib + PyYAML installed, and the
# normal loader path never pays for the import.


def _raise_for_status_with_details(response, context):
    """Raise an HTTPError whose message includes the Graph error code/description."""
    import requests

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        detail = ""
        try:
            payload = response.json()
            error_data = payload.get("error")
            if isinstance(error_data, dict):
                error_code = error_data.get("code")
                description = error_data.get("message")
            else:
                error_code = error_data
                description = payload.get("error_description")
            if error_code or description:
                detail = f" ({error_code}: {description})"
        except ValueError:
            pass
        raise requests.HTTPError(f"{context}{detail}", response=response) from exc


def get_access_token(secrets: dict[str, str], config: dict[str, Any]) -> str:
    """Authenticate via OAuth2 client credentials and return a Bearer token."""
    import requests

    aad = config["email"]["aad_endpoint"]
    tenant = secrets["TENANT_ID"]
    url = f"{aad}/{tenant}/oauth2/v2.0/token"
    data = {
        "client_id": secrets["CLIENT_ID"],
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": secrets["CLIENT_SECRET"],
        "grant_type": "client_credentials",
    }
    resp = requests.post(url, data=data, timeout=20)
    _raise_for_status_with_details(resp, "Failed to acquire Microsoft Graph access token")
    return resp.json()["access_token"]


def _file_attachment(path: str) -> dict[str, Any]:
    with open(path, "rb") as handle:
        content = base64.b64encode(handle.read()).decode("utf-8")
    return {
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": os.path.basename(path),
        "contentBytes": content,
    }


def build_attachments(
    attachment_paths: Iterable[str] | None,
    *,
    max_bytes: int | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Turn local file paths into Graph ``fileAttachment`` payloads.

    Returns ``(attachments, skipped_notes)``. A path that is missing, unreadable, or
    larger than ``max_bytes`` is skipped with a human-readable note rather than
    failing the whole send (a log on an unreachable share must not block the report).
    """
    attachments: list[dict[str, Any]] = []
    skipped: list[str] = []
    seen: set[str] = set()
    for path in attachment_paths or []:
        if not path or path in seen:
            continue
        seen.add(path)
        try:
            if not os.path.isfile(path):
                skipped.append(f"{os.path.basename(path)} (not found: {path})")
                continue
            size = os.path.getsize(path)
            if max_bytes is not None and size > max_bytes:
                skipped.append(
                    f"{os.path.basename(path)} (too large: {size // 1024} KB > "
                    f"{max_bytes // 1024} KB limit)"
                )
                continue
            attachments.append(_file_attachment(path))
        except OSError as exc:
            skipped.append(f"{os.path.basename(path)} (unreadable: {exc})")
    return attachments, skipped


def send_email(
    config: dict[str, Any],
    secrets: dict[str, str],
    recipients: list[str],
    subject: str,
    html_body: str,
    *,
    attachment_paths: Iterable[str] | None = None,
    cc_recipients: list[str] | None = None,
    max_attachment_bytes: int | None = None,
) -> list[str]:
    """Send an HTML email (with optional attachments) via Microsoft Graph.

    Returns the list of skipped-attachment notes (empty when everything attached),
    so the caller can surface them. Raises on auth/send failure.
    """
    import requests

    if not recipients:
        raise ValueError("send_email requires at least one recipient.")

    token = get_access_token(secrets, config)
    graph = config["email"]["graph_endpoint"]
    sender = config["email"]["from_email"]
    url = f"{graph}/v1.0/users/{sender}/sendMail"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    message: dict[str, Any] = {
        "subject": subject,
        "body": {"contentType": "HTML", "content": html_body},
        "toRecipients": [{"emailAddress": {"address": addr}} for addr in recipients],
    }
    if cc_recipients:
        message["ccRecipients"] = [
            {"emailAddress": {"address": addr}} for addr in cc_recipients
        ]

    attachments, skipped = build_attachments(attachment_paths, max_bytes=max_attachment_bytes)
    if attachments:
        message["attachments"] = attachments

    resp = requests.post(url, headers=headers, json={"message": message}, timeout=60)
    _raise_for_status_with_details(resp, "Failed to send email via Graph API")
    print(f"Email sent to {recipients} — {subject}")
    return skipped
