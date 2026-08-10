#!/usr/bin/env python3
"""Decrypt a .env.enc produced by encrypt_env.py (AES-256-GCM + scrypt)."""
import argparse
import getpass
import os
import sys

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt

MAGIC = b"ENV1"
SALT_LEN = 16
NONCE_LEN = 12


def derive_key(passphrase: bytes, salt: bytes) -> bytes:
    kdf = Scrypt(salt=salt, length=32, n=2**14, r=8, p=1)
    return kdf.derive(passphrase)


def main():
    ap = argparse.ArgumentParser(
        description="Decrypt a .env.enc created by encrypt_env.py (AES-256-GCM + scrypt)."
    )
    ap.add_argument("--in", dest="src", default=".env.enc", help="Input encrypted file (default: .env.enc)")
    ap.add_argument("--out", dest="dst", default=".env", help="Output plaintext file (default: .env)")
    args = ap.parse_args()

    if not os.path.exists(args.src):
        print(f"ERROR: input file not found: {args.src}", file=sys.stderr)
        sys.exit(1)

    blob = open(args.src, "rb").read()
    if len(blob) < 4 + SALT_LEN + NONCE_LEN + 16:
        print("ERROR: file too short or corrupted.", file=sys.stderr)
        sys.exit(2)

    magic = blob[:4]
    if magic != MAGIC:
        print("ERROR: wrong file format (magic mismatch).", file=sys.stderr)
        sys.exit(3)

    salt = blob[4:4 + SALT_LEN]
    nonce = blob[4 + SALT_LEN:4 + SALT_LEN + NONCE_LEN]
    ct = blob[4 + SALT_LEN + NONCE_LEN:]

    pw = getpass.getpass("Enter passphrase: ").encode("utf-8")
    key = derive_key(pw, salt)
    aes = AESGCM(key)
    try:
        pt = aes.decrypt(nonce, ct, associated_data=None)
    except Exception:
        print("ERROR: decryption failed. Wrong passphrase or corrupted file.", file=sys.stderr)
        sys.exit(4)

    with open(args.dst, "wb") as f:
        f.write(pt)

    print(f"Decrypted -> {args.dst}")


if __name__ == "__main__":
    main()
