#!/usr/bin/env python3
"""Decrypt a .env.enc produced by encrypt_env.py (AES-256-GCM + scrypt).

Thin wrapper over infor_loader.env_secrets' `unpack` command.
"""
import argparse

from infor_loader.env_secrets import (
    DEFAULT_ENC_PATH,
    DEFAULT_ENV_PATH,
    main as env_secrets_main,
)


def main():
    ap = argparse.ArgumentParser(
        description="Decrypt a .env.enc created by encrypt_env.py (AES-256-GCM + scrypt)."
    )
    ap.add_argument(
        "--in", dest="src", default=DEFAULT_ENC_PATH,
        help="Input encrypted file (default: .env.enc)",
    )
    ap.add_argument(
        "--out", dest="dst", default=DEFAULT_ENV_PATH,
        help="Output plaintext file (default: .env)",
    )
    args = ap.parse_args()
    return env_secrets_main(["unpack", "--in", args.src, "--out", args.dst])


if __name__ == "__main__":
    raise SystemExit(main())
