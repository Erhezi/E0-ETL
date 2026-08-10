#!/usr/bin/env python3
"""Encrypt .env for transport, or encrypt its declared secret keys in place.

Two modes (both implemented in infor_loader/env_secrets.py):
- default: AES-256-GCM encrypt the whole .env into .env.enc (scrypt-derived key).
  Move .env.enc between machines and rebuild .env with decrypt_env.py.
- --hash-secrets-only: replace each declared secret key in .env with a portable
  KEY_HASHED=enc:: value that env_secrets.load_env decrypts at runtime using the
  E0_SECRET_PASSPHRASE env var.

The declared keys come from --keys, then the E0_SECRET_KEYS environment variable,
then a SECRET_KEYS= line in .env, then DEFAULT_SECRET_KEYS.
"""
import argparse
import sys

from infor_loader.env_secrets import (
    DEFAULT_ENC_PATH,
    DEFAULT_ENV_PATH,
    SECRET_KEYS_DECLARATION,
    SECRET_KEYS_ENV_VAR,
    main as env_secrets_main,
)


def main():
    ap = argparse.ArgumentParser(
        description="Encrypt a .env file with AES-256-GCM using scrypt key derivation."
    )
    ap.add_argument("--in", dest="src", default=DEFAULT_ENV_PATH, help="Input file (default: .env)")
    ap.add_argument(
        "--out", dest="dst", default=DEFAULT_ENC_PATH, help="Output file (default: .env.enc)"
    )
    ap.add_argument(
        "--hash-secrets-only",
        action="store_true",
        help=(
            "Replace the declared secret keys with portable encrypted *_HASHED "
            "entries instead of encrypting the whole file. Uses the passphrase from "
            "the E0_SECRET_PASSPHRASE environment variable, or prompts if unset."
        ),
    )
    ap.add_argument(
        "--keys",
        default=None,
        help=(
            "Comma-separated keys to encrypt with --hash-secrets-only, overriding "
            f"{SECRET_KEYS_ENV_VAR} and the {SECRET_KEYS_DECLARATION} line in .env."
        ),
    )
    args = ap.parse_args()

    if args.hash_secrets_only:
        argv = ["encrypt", "--env", args.src]
        if args.keys:
            argv += ["--keys", args.keys]
        return env_secrets_main(argv)

    if args.keys:
        print(
            "ERROR: --keys only applies with --hash-secrets-only (whole-file mode "
            "encrypts everything).",
            file=sys.stderr,
        )
        return 2

    return env_secrets_main(["pack", "--in", args.src, "--out", args.dst])


if __name__ == "__main__":
    raise SystemExit(main())
