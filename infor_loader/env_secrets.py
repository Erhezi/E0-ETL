"""Reusable encrypted-``.env`` support: declare which variables hold secrets,
encrypt them in place, decrypt them back at runtime.

Encrypting rewrites ``KEY=plaintext`` into ``KEY_HASHED=enc::...`` so nothing
sensitive sits on disk; :func:`load_env` decrypts it again using the passphrase in
the ``E0_SECRET_PASSPHRASE`` environment variable, which is what makes unattended
Task Scheduler runs work.

**Which keys get encrypted** is resolved in this order (first non-empty wins):

1. the ``secret_keys=`` argument (``--keys A,B`` on the CLI)
2. the ``E0_SECRET_KEYS`` process environment variable
3. a ``SECRET_KEYS=`` line inside the ``.env`` file itself -- the normal place to
   declare a new app's secret, e.g. ``SECRET_KEYS=CLIENT_SECRET,SFTP_PASSWORD``
4. :data:`DEFAULT_SECRET_KEYS`

Decryption needs no list at all: every ``enc::`` value in the file is decrypted,
and a ``KEY_HASHED`` entry is exposed under its base name ``KEY``. So adding a
secret for a new app only touches the encrypt side.

CLI (all subcommands take ``--env``, default ``.env``)::

    python -m infor_loader.env_secrets status            # what is plaintext vs encrypted
    python -m infor_loader.env_secrets setup             # save passphrase + encrypt (first run)
    python -m infor_loader.env_secrets encrypt           # encrypt the declared keys in place
    python -m infor_loader.env_secrets encrypt --keys SFTP_PASSWORD
    python -m infor_loader.env_secrets pack              # whole .env -> .env.enc for transport
    python -m infor_loader.env_secrets unpack            # .env.enc -> .env
"""

from __future__ import annotations

import argparse
import getpass
import os
import subprocess
import sys
from pathlib import Path

from .secret_crypto import (
    SECRET_ENV_VAR,
    SECRET_ENV_VARS,
    SECRET_PREFIX,
    decrypt_bytes,
    decrypt_secret_value,
    encrypt_bytes,
    encrypt_secret_env_lines,
    get_secret_passphrase,
)

# Fallback list, used only when neither --keys, E0_SECRET_KEYS, nor a SECRET_KEYS
# line in .env declares one. Prefer declaring keys in .env so the file documents
# its own secrets; this default only keeps the original Graph setup working.
DEFAULT_SECRET_KEYS = ("CLIENT_SECRET", "CLIENT_SECRET_FUTURE")

# Name of the optional declaration line inside .env, and of the process env var
# that overrides it.
SECRET_KEYS_DECLARATION = "SECRET_KEYS"
SECRET_KEYS_ENV_VAR = "E0_SECRET_KEYS"

HASHED_SUFFIX = "_HASHED"

DEFAULT_ENV_PATH = ".env"
DEFAULT_ENC_PATH = ".env.enc"


# --------------------------------------------------------------------------- #
# .env parsing
# --------------------------------------------------------------------------- #

def normalize_env_value(value: str) -> str:
    """Strip quotes and a trailing ``# comment`` from a raw ``KEY=`` value."""
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


def parse_env_file(env_path: str = DEFAULT_ENV_PATH) -> dict[str, str]:
    """Read ``.env`` into a dict with no decryption (``enc::`` values stay as-is)."""
    values: dict[str, str] = {}
    with open(env_path, "r", encoding="utf-8-sig") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            key, sep, value = line.partition("=")
            if not sep or not key.strip():
                continue
            values[key.strip()] = normalize_env_value(value)
    return values


def _split_keys(text: str) -> list[str]:
    """Parse a comma/whitespace-separated key list, deduped, order preserved."""
    keys: list[str] = []
    for chunk in text.replace(",", " ").split():
        key = chunk.strip().strip('"').strip("'")
        if key and key not in keys:
            keys.append(key)
    return keys


def resolve_secret_keys(
    env_path: str = DEFAULT_ENV_PATH,
    secret_keys: object = None,
) -> tuple[str, ...]:
    """Return the keys to encrypt, following the precedence documented above.

    ``secret_keys`` may be a sequence of names or a single comma-separated string.
    """
    if secret_keys:
        if isinstance(secret_keys, str):
            return tuple(_split_keys(secret_keys))
        return tuple(_split_keys(" ".join(secret_keys)))

    override = os.getenv(SECRET_KEYS_ENV_VAR, "").strip()
    if override:
        return tuple(_split_keys(override))

    if os.path.exists(env_path):
        declared = parse_env_file(env_path).get(SECRET_KEYS_DECLARATION, "").strip()
        if declared:
            return tuple(_split_keys(declared))

    return tuple(DEFAULT_SECRET_KEYS)


# --------------------------------------------------------------------------- #
# Runtime loading
# --------------------------------------------------------------------------- #

def load_env(
    env_path: str = DEFAULT_ENV_PATH,
    *,
    passphrase: str | None = None,
) -> dict[str, str]:
    """Parse ``.env`` and decrypt every encrypted value in it.

    ``KEY_HASHED=enc::...`` is exposed as ``KEY`` (the ``KEY_HASHED`` entry is kept
    too), and a bare ``KEY=enc::...`` is decrypted in place. Values that are not
    ``enc::`` are returned untouched, so a partially-encrypted file works.
    """
    if not os.path.exists(env_path):
        raise FileNotFoundError(
            f"{env_path} not found. Copy .env.example to .env (or run "
            f"'python decrypt_env.py' to produce it from {DEFAULT_ENC_PATH})."
        )

    values = parse_env_file(env_path)

    encrypted = [key for key, value in values.items() if value.startswith(SECRET_PREFIX)]
    if not encrypted:
        return values

    # Resolve the passphrase once so a missing one reports a single clear error
    # rather than one per secret.
    if passphrase is None:
        passphrase = get_secret_passphrase(required=True)

    for key in encrypted:
        target = key[: -len(HASHED_SUFFIX)] if key.endswith(HASHED_SUFFIX) else key
        try:
            values[target] = decrypt_secret_value(values[key], passphrase=passphrase)
        except Exception as exc:  # noqa: BLE001 - surface a readable passphrase error.
            raise RuntimeError(
                f"Failed to decrypt {key} — wrong passphrase or corrupted value. "
                f"Ensure {' or '.join(SECRET_ENV_VARS)} matches the passphrase used "
                f"to encrypt it."
            ) from exc

    return values


# --------------------------------------------------------------------------- #
# Encrypting keys in place
# --------------------------------------------------------------------------- #

def encrypt_env_file(
    env_path: str = DEFAULT_ENV_PATH,
    *,
    secret_keys: object = None,
    passphrase: str | None = None,
) -> list[str]:
    """Rewrite the declared plaintext keys in ``.env`` as ``KEY_HASHED=enc::...``.

    Idempotent: keys that are already hashed no longer match and are left alone.
    Returns the list of keys that were converted.
    """
    keys = resolve_secret_keys(env_path, secret_keys)
    if not keys:
        return []

    if passphrase is None:
        passphrase = get_secret_passphrase(required=True)

    with open(env_path, "r", encoding="utf-8-sig") as handle:
        lines = handle.readlines()

    updated_lines, updated_keys = encrypt_secret_env_lines(
        lines, secret_keys=keys, passphrase=passphrase
    )
    if not updated_keys:
        return []

    with open(env_path, "w", encoding="utf-8") as handle:
        handle.writelines(updated_lines)
    return updated_keys


def env_secret_status(
    env_path: str = DEFAULT_ENV_PATH,
    *,
    secret_keys: object = None,
) -> list[tuple[str, str]]:
    """Return ``(key, state)`` for each declared key plus any other encrypted entry.

    State is one of ``encrypted``, ``plaintext`` (needs encrypting) or ``missing``.
    """
    values = parse_env_file(env_path)
    keys = list(resolve_secret_keys(env_path, secret_keys))

    # Anything already encrypted counts even if it is no longer declared, so a key
    # dropped from SECRET_KEYS does not silently vanish from the report.
    for key, value in values.items():
        if not value.startswith(SECRET_PREFIX):
            continue
        base = key[: -len(HASHED_SUFFIX)] if key.endswith(HASHED_SUFFIX) else key
        if base not in keys:
            keys.append(base)

    rows: list[tuple[str, str]] = []
    for key in keys:
        hashed = values.get(f"{key}{HASHED_SUFFIX}", "")
        plain = values.get(key, "")
        if hashed.startswith(SECRET_PREFIX) or plain.startswith(SECRET_PREFIX):
            rows.append((key, "encrypted"))
        elif plain:
            rows.append((key, "plaintext"))
        else:
            rows.append((key, "missing"))
    return rows


# --------------------------------------------------------------------------- #
# Whole-file transport (.env <-> .env.enc)
# --------------------------------------------------------------------------- #

def pack_env_file(
    src: str = DEFAULT_ENV_PATH,
    dst: str = DEFAULT_ENC_PATH,
    *,
    passphrase: str,
) -> str:
    """AES-256-GCM the whole ``.env`` into ``.env.enc`` for moving between machines."""
    with open(src, "rb") as handle:
        data = handle.read()
    with open(dst, "wb") as handle:
        handle.write(encrypt_bytes(data, passphrase))
    return dst


def unpack_env_file(
    src: str = DEFAULT_ENC_PATH,
    dst: str = DEFAULT_ENV_PATH,
    *,
    passphrase: str,
) -> str:
    """Rebuild a plaintext ``.env`` from a ``.env.enc`` produced by :func:`pack_env_file`."""
    with open(src, "rb") as handle:
        blob = handle.read()
    with open(dst, "wb") as handle:
        handle.write(decrypt_bytes(blob, passphrase))
    return dst


# --------------------------------------------------------------------------- #
# First-time setup
# --------------------------------------------------------------------------- #

def persist_user_env_var(name: str, value: str) -> None:
    """Save a persistent per-user environment variable (Windows ``setx``)."""
    if os.name != "nt":
        raise RuntimeError(
            "Persisting environment variables is only supported on Windows here. "
            f"Set {name} yourself (e.g. in ~/.profile) on other platforms."
        )

    result = subprocess.run(["setx", name, value], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(message or f"Failed to persist {name}.")

    os.environ[name] = value


def prompt_passphrase(*, confirm: bool = True, label: str | None = None) -> str:
    """Prompt for the shared passphrase, optionally twice, and return it."""
    prompt = label or f"Enter shared passphrase for {SECRET_ENV_VAR}: "
    passphrase = getpass.getpass(prompt).strip()
    if not passphrase:
        raise ValueError("Passphrase cannot be empty.")
    if confirm:
        if passphrase != getpass.getpass("Re-enter passphrase: ").strip():
            raise ValueError("Passphrases do not match.")
    return passphrase


def first_time_setup(
    env_path: str = DEFAULT_ENV_PATH,
    *,
    secret_keys: object = None,
) -> int:
    """Interactive one-time setup: persist the passphrase, then encrypt declared keys."""
    keys = resolve_secret_keys(env_path, secret_keys)

    print("E0-ETL secret first-time setup")
    print("This saves the shared passphrase for your Windows user account, then")
    print(f"encrypts these keys in {env_path}: {', '.join(keys) or '(none declared)'}")
    print("")

    existing = os.getenv(SECRET_ENV_VAR, "").strip()
    if existing:
        overwrite = input(
            f"{SECRET_ENV_VAR} is already set in this terminal. Overwrite it? [y/N]: "
        ).strip().lower()
        if overwrite not in {"y", "yes"}:
            print("No changes made.")
            return 0

    try:
        passphrase = prompt_passphrase()
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    persist_user_env_var(SECRET_ENV_VAR, passphrase)
    print(f"Saved persistent user environment variable: {SECRET_ENV_VAR}")

    if keys:
        convert = input(
            f"Encrypt those keys in {env_path} now? [Y/n]: "
        ).strip().lower()
        if convert in {"", "y", "yes"}:
            _encrypt_and_report(env_path, secret_keys=keys, passphrase=passphrase)

    print("")
    print("Setup complete.")
    print("Open a new terminal before future runs so the saved variable is loaded.")
    print("Then send a test report: python -B run_daily_loaders.py notify --dry-run")
    return 0


def _encrypt_and_report(env_path: str, *, secret_keys: object, passphrase: str) -> int:
    if not Path(env_path).exists():
        print(f"Skipped: {env_path} was not found.", file=sys.stderr)
        return 1

    updated = encrypt_env_file(env_path, secret_keys=secret_keys, passphrase=passphrase)
    if not updated:
        print(f"Nothing to encrypt in {env_path} — no plaintext values for the declared keys.")
        return 0

    print(f"Updated {env_path}:")
    for key in updated:
        print(f"  {key} -> {key}{HASHED_SUFFIX}")
    print(f"Set {SECRET_ENV_VAR} on every machine that runs this pipeline.")
    return 0


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def _passphrase_for_cli(*, confirm: bool) -> str:
    """Use the passphrase from the environment, else prompt for it."""
    existing = get_secret_passphrase(required=False)
    if existing:
        return existing
    return prompt_passphrase(
        confirm=confirm, label=f"Enter passphrase ({SECRET_ENV_VAR} is not set): "
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m infor_loader.env_secrets",
        description="Manage encrypted secrets inside a .env file.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_env_arg(p):
        p.add_argument("--env", default=DEFAULT_ENV_PATH, help="Path to .env (default: .env)")

    def add_keys_arg(p):
        p.add_argument(
            "--keys",
            default=None,
            help=(
                "Comma-separated keys to encrypt, overriding "
                f"{SECRET_KEYS_ENV_VAR} and the {SECRET_KEYS_DECLARATION} line in .env."
            ),
        )

    p_status = sub.add_parser("status", help="Show which declared secrets are encrypted.")
    add_env_arg(p_status)
    add_keys_arg(p_status)

    p_setup = sub.add_parser("setup", help="Save the passphrase, then encrypt declared keys.")
    add_env_arg(p_setup)
    add_keys_arg(p_setup)

    p_encrypt = sub.add_parser("encrypt", help="Encrypt the declared keys in .env in place.")
    add_env_arg(p_encrypt)
    add_keys_arg(p_encrypt)

    p_pack = sub.add_parser("pack", help="Encrypt the whole .env into .env.enc for transport.")
    p_pack.add_argument("--in", dest="src", default=DEFAULT_ENV_PATH)
    p_pack.add_argument("--out", dest="dst", default=DEFAULT_ENC_PATH)

    p_unpack = sub.add_parser("unpack", help="Rebuild .env from .env.enc.")
    p_unpack.add_argument("--in", dest="src", default=DEFAULT_ENC_PATH)
    p_unpack.add_argument("--out", dest="dst", default=DEFAULT_ENV_PATH)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "setup":
        return first_time_setup(args.env, secret_keys=args.keys)

    if args.command == "status":
        if not Path(args.env).exists():
            print(f"ERROR: {args.env} not found.", file=sys.stderr)
            return 1
        rows = env_secret_status(args.env, secret_keys=args.keys)
        if not rows:
            print(f"No secret keys declared for {args.env}.")
            return 0
        width = max(len(key) for key, _ in rows)
        print(f"{args.env}:")
        for key, state in rows:
            print(f"  {key.ljust(width)}  {state}")
        passphrase_set = "set" if get_secret_passphrase(required=False) else "NOT set"
        print(f"\n{SECRET_ENV_VAR}: {passphrase_set}")
        if any(state == "plaintext" for _, state in rows):
            print("Run 'python -m infor_loader.env_secrets encrypt' to encrypt the plaintext keys.")
        return 0

    if args.command == "encrypt":
        if not Path(args.env).exists():
            print(f"ERROR: {args.env} not found.", file=sys.stderr)
            return 1
        try:
            passphrase = _passphrase_for_cli(confirm=True)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        return _encrypt_and_report(args.env, secret_keys=args.keys, passphrase=passphrase)

    if args.command == "pack":
        if not Path(args.src).exists():
            print(f"ERROR: input file not found: {args.src}", file=sys.stderr)
            return 1
        try:
            passphrase = prompt_passphrase(confirm=True, label="Enter passphrase: ")
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        pack_env_file(args.src, args.dst, passphrase=passphrase)
        print(f"Encrypted -> {args.dst}")
        print("Share the passphrase out-of-band (phone/Teams).")
        return 0

    if args.command == "unpack":
        if not Path(args.src).exists():
            print(f"ERROR: input file not found: {args.src}", file=sys.stderr)
            return 1
        try:
            passphrase = prompt_passphrase(confirm=False, label="Enter passphrase: ")
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        try:
            unpack_env_file(args.src, args.dst, passphrase=passphrase)
        except ValueError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 3
        print(f"Decrypted -> {args.dst}")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
