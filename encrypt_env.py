#!/usr/bin/env python3
"""Encrypt .env for transport, or hash its CLIENT_SECRET in place for runtime use.

Two modes (ported from A13-MedlinePBO):
- default: AES-256-GCM encrypt the whole .env into .env.enc (scrypt-derived key).
  Move .env.enc between machines and rebuild .env with decrypt_env.py.
- --hash-secrets-only: replace CLIENT_SECRET (and CLIENT_SECRET_FUTURE) in .env with
  portable CLIENT_SECRET_HASHED=enc:: values that notify.load_secrets decrypts at
  runtime using the E0_SECRET_PASSPHRASE env var.
"""
import argparse
import getpass
import os
import sys
from secrets import token_bytes

from infor_loader.secret_crypto import SECRET_ENV_VAR, encrypt_secret_env_lines

MAGIC = b"ENV1"  # file marker
SALT_LEN = 16
NONCE_LEN = 12


def derive_key(passphrase: bytes, salt: bytes) -> bytes:
    from cryptography.hazmat.primitives.kdf.scrypt import Scrypt

    kdf = Scrypt(salt=salt, length=32, n=2**14, r=8, p=1)
    return kdf.derive(passphrase)


def main():
    ap = argparse.ArgumentParser(
        description="Encrypt a .env file with AES-256-GCM using scrypt key derivation."
    )
    ap.add_argument("--in", dest="src", default=".env", help="Input file (default: .env)")
    ap.add_argument("--out", dest="dst", default=".env.enc", help="Output file (default: .env.enc)")
    ap.add_argument(
        "--hash-secrets-only",
        action="store_true",
        help=(
            "Replace CLIENT_SECRET and CLIENT_SECRET_FUTURE with portable encrypted "
            "*_HASHED entries. Uses the passphrase from the E0_SECRET_PASSPHRASE "
            "environment variable or prompts if it is not set."
        ),
    )
    args = ap.parse_args()

    if not os.path.exists(args.src):
        print(f"ERROR: input file not found: {args.src}", file=sys.stderr)
        sys.exit(1)

    if args.hash_secrets_only:
        passphrase = os.getenv(SECRET_ENV_VAR, "")
        if not passphrase:
            passphrase = getpass.getpass(
                f"Enter passphrase for *_HASHED secrets ({SECRET_ENV_VAR}): "
            )
            passphrase2 = getpass.getpass("Re-enter passphrase: ")
            if passphrase != passphrase2:
                print("ERROR: passphrases do not match.", file=sys.stderr)
                sys.exit(2)

        with open(args.src, "r", encoding="utf-8-sig") as f:
            lines = f.readlines()

        updated_lines, updated_keys = encrypt_secret_env_lines(
            lines,
            secret_keys=("CLIENT_SECRET", "CLIENT_SECRET_FUTURE"),
            passphrase=passphrase,
        )

        if not updated_keys:
            print("No CLIENT_SECRET or CLIENT_SECRET_FUTURE entries were found to hash.")
            return

        with open(args.src, "w", encoding="utf-8") as f:
            f.writelines(updated_lines)

        print(f"Hashed secrets in-place -> {args.src}")
        print(f"Set {SECRET_ENV_VAR} on every machine that will run this pipeline.")
        for key in updated_keys:
            print(f"Converted {key} -> {key}_HASHED")
        return

    pw = getpass.getpass("Enter passphrase: ").encode("utf-8")
    pw2 = getpass.getpass("Re-enter passphrase: ").encode("utf-8")
    if pw != pw2:
        print("ERROR: passphrases do not match.", file=sys.stderr)
        sys.exit(2)

    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    salt = token_bytes(SALT_LEN)
    key = derive_key(pw, salt)
    aes = AESGCM(key)
    nonce = token_bytes(NONCE_LEN)

    data = open(args.src, "rb").read()
    ct = aes.encrypt(nonce, data, associated_data=None)

    with open(args.dst, "wb") as f:
        f.write(MAGIC + salt + nonce + ct)

    print(f"Encrypted -> {args.dst}")
    print("Share the passphrase out-of-band (phone/Teams).")


if __name__ == "__main__":
    main()
