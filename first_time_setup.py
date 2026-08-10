#!/usr/bin/env python3
"""One-time setup for the email-notification secrets (Windows).

Persists the shared passphrase as the E0_SECRET_PASSPHRASE user environment variable
(so unattended runs can decrypt the hashed CLIENT_SECRET), then optionally converts a
plaintext CLIENT_SECRET in .env into an encrypted CLIENT_SECRET_HASHED value.
"""
import getpass
import os
import subprocess
import sys
from pathlib import Path

from infor_loader.secret_crypto import SECRET_ENV_VAR, encrypt_secret_env_lines


def persist_user_env_var(name, value):
    if os.name != "nt":
        raise RuntimeError(
            "This setup helper currently supports persistent environment variables on Windows only."
        )

    result = subprocess.run(
        ["setx", name, value],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(message or f"Failed to persist {name}.")

    os.environ[name] = value


def maybe_hash_env_file(env_path, passphrase):
    path = Path(env_path)
    if not path.exists():
        print(f"Skipped .env conversion because {env_path} was not found.")
        return

    with open(path, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()

    updated_lines, updated_keys = encrypt_secret_env_lines(
        lines,
        secret_keys=("CLIENT_SECRET", "CLIENT_SECRET_FUTURE"),
        passphrase=passphrase,
    )

    if not updated_keys:
        print("No raw CLIENT_SECRET values were found in .env. No conversion was needed.")
        return

    with open(path, "w", encoding="utf-8") as f:
        f.writelines(updated_lines)

    print(f"Updated {env_path} with encrypted *_HASHED values:")
    for key in updated_keys:
        print(f"  {key} -> {key}_HASHED")


def main():
    print("E0-ETL email-notification first-time setup")
    print("This will save the shared secret passphrase for your Windows user account.")
    print("")

    existing = os.getenv(SECRET_ENV_VAR, "").strip()
    if existing:
        overwrite = input(
            f"{SECRET_ENV_VAR} is already set in this terminal. Overwrite it? [y/N]: "
        ).strip().lower()
        if overwrite not in {"y", "yes"}:
            print("No changes made.")
            return 0

    passphrase = getpass.getpass(f"Enter shared passphrase for {SECRET_ENV_VAR}: ").strip()
    confirm = getpass.getpass("Re-enter passphrase: ").strip()

    if not passphrase:
        print("Passphrase cannot be empty.", file=sys.stderr)
        return 1

    if passphrase != confirm:
        print("Passphrases do not match.", file=sys.stderr)
        return 2

    persist_user_env_var(SECRET_ENV_VAR, passphrase)
    print(f"Saved persistent user environment variable: {SECRET_ENV_VAR}")

    convert_now = input(
        "Convert raw CLIENT_SECRET values in .env to encrypted *_HASHED values now? [Y/n]: "
    ).strip().lower()
    if convert_now in {"", "y", "yes"}:
        maybe_hash_env_file(".env", passphrase)

    print("")
    print("Setup complete.")
    print("Open a new terminal before future runs so the saved environment variable is loaded.")
    print("Then send a test report: python -B run_daily_loaders.py notify --dry-run")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
