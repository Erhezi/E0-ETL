#!/usr/bin/env python3
"""One-time secret setup (Windows).

Persists the shared passphrase as the E0_SECRET_PASSPHRASE user environment variable
(so unattended runs can decrypt secrets), then converts the declared plaintext keys
in .env into encrypted *_HASHED values.

Which keys get encrypted comes from --keys, then the E0_SECRET_KEYS environment
variable, then a SECRET_KEYS= line in .env, then infor_loader.env_secrets'
DEFAULT_SECRET_KEYS. All the logic lives in infor_loader/env_secrets.py; this file
is just the familiar entry point.
"""
import argparse

from infor_loader.env_secrets import (
    DEFAULT_ENV_PATH,
    SECRET_KEYS_DECLARATION,
    SECRET_KEYS_ENV_VAR,
    first_time_setup,
)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--env", default=DEFAULT_ENV_PATH, help="Path to .env (default: .env)")
    ap.add_argument(
        "--keys",
        default=None,
        help=(
            "Comma-separated keys to encrypt, overriding "
            f"{SECRET_KEYS_ENV_VAR} and the {SECRET_KEYS_DECLARATION} line in .env."
        ),
    )
    args = ap.parse_args()
    return first_time_setup(args.env, secret_keys=args.keys)


if __name__ == "__main__":
    raise SystemExit(main())
