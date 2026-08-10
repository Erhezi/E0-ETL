"""AES-256-GCM secret encryption, ported from A13-MedlinePBO/src/secret_crypto.py.

Low-level crypto only -- ``env_secrets`` holds the ``.env`` file semantics and CLI.
Two mechanisms, both keyed off a scrypt-derived key:

- Portable per-secret hashing (``enc::`` prefix): ``CLIENT_SECRET`` in ``.env`` is
  replaced by ``CLIENT_SECRET_HASHED=enc::...`` and decrypted at *runtime* by
  ``env_secrets.load_env`` using the passphrase from the ``E0_SECRET_PASSPHRASE``
  environment variable. This is the unattended-friendly path: the plaintext secret
  never sits on disk, and Task Scheduler only needs the passphrase env var set.
- Full-file transport (:func:`encrypt_bytes`/:func:`decrypt_bytes`) encrypts the
  whole ``.env`` into ``.env.enc`` for moving between machines out-of-band.
"""

import base64
import os
from secrets import token_bytes

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt


SECRET_PREFIX = "enc::"
# Passphrase env vars, checked in order. E0's own var wins; PBO_SECRET_PASSPHRASE is
# accepted as a fallback so a workstation already set up for A13-MedlinePBO -- which
# shares this service account and its encrypted CLIENT_SECRET -- works with no extra
# setup (the hashed secret decrypts with the same passphrase either way).
SECRET_ENV_VARS = ("E0_SECRET_PASSPHRASE", "PBO_SECRET_PASSPHRASE")
# Canonical name used by first_time_setup.py / encrypt_env.py when persisting/prompting.
SECRET_ENV_VAR = SECRET_ENV_VARS[0]
SALT_LEN = 16
NONCE_LEN = 12
# Whole-file (.env.enc) header marker. Layout: MAGIC | salt | nonce | ciphertext.
FILE_MAGIC = b"ENV1"


def _derive_key(passphrase: str, salt: bytes) -> bytes:
    kdf = Scrypt(salt=salt, length=32, n=2**14, r=8, p=1)
    return kdf.derive(passphrase.encode("utf-8"))


def get_secret_passphrase(required=True):
    for name in SECRET_ENV_VARS:
        passphrase = os.getenv(name, "").strip()
        if passphrase:
            return passphrase
    if required:
        raise RuntimeError(
            f"Set the {SECRET_ENV_VAR} environment variable before loading encrypted "
            f"secrets (or {SECRET_ENV_VARS[1]} if this machine is already set up for "
            f"A13-MedlinePBO)."
        )
    return ""


def encrypt_secret_value(value: str, passphrase: str | None = None) -> str:
    if passphrase is None:
        passphrase = get_secret_passphrase(required=True)

    salt = token_bytes(SALT_LEN)
    nonce = token_bytes(NONCE_LEN)
    key = _derive_key(passphrase, salt)
    ciphertext = AESGCM(key).encrypt(nonce, value.encode("utf-8"), None)
    payload = base64.urlsafe_b64encode(salt + nonce + ciphertext).decode("ascii")
    return f"{SECRET_PREFIX}{payload}"


def decrypt_secret_value(value: str, passphrase: str | None = None) -> str:
    if not value or not value.startswith(SECRET_PREFIX):
        return value

    if passphrase is None:
        passphrase = get_secret_passphrase(required=True)

    payload = base64.urlsafe_b64decode(value[len(SECRET_PREFIX):].encode("ascii"))
    salt = payload[:SALT_LEN]
    nonce = payload[SALT_LEN:SALT_LEN + NONCE_LEN]
    ciphertext = payload[SALT_LEN + NONCE_LEN:]
    key = _derive_key(passphrase, salt)
    plaintext = AESGCM(key).decrypt(nonce, ciphertext, None)
    return plaintext.decode("utf-8")


def encrypt_bytes(data: bytes, passphrase: str) -> bytes:
    """Encrypt a whole file's bytes into the ``.env.enc`` transport format."""
    salt = token_bytes(SALT_LEN)
    nonce = token_bytes(NONCE_LEN)
    key = _derive_key(passphrase, salt)
    return FILE_MAGIC + salt + nonce + AESGCM(key).encrypt(nonce, data, None)


def decrypt_bytes(blob: bytes, passphrase: str) -> bytes:
    """Reverse :func:`encrypt_bytes`. Raises ``ValueError`` on a bad file or passphrase."""
    header = 4 + SALT_LEN + NONCE_LEN
    if len(blob) < header + 16:
        raise ValueError("File too short or corrupted.")
    if blob[:4] != FILE_MAGIC:
        raise ValueError("Wrong file format (magic mismatch).")

    salt = blob[4:4 + SALT_LEN]
    nonce = blob[4 + SALT_LEN:header]
    key = _derive_key(passphrase, salt)
    try:
        return AESGCM(key).decrypt(nonce, blob[header:], None)
    except Exception as exc:  # noqa: BLE001 - InvalidTag means wrong passphrase.
        raise ValueError("Decryption failed. Wrong passphrase or corrupted file.") from exc


def encrypt_secret_env_lines(lines, secret_keys, passphrase: str | None = None):
    """Rewrite ``KEY=value`` lines whose key is in ``secret_keys`` to
    ``KEY_HASHED=enc::...``, preserving any trailing ``# comment``. Returns the new
    lines and the list of keys that were converted."""
    updated_lines = []
    updated_keys = []

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            updated_lines.append(line)
            continue

        line_ending = "\n" if line.endswith("\n") else ""
        content = line[:-1] if line_ending else line
        key, _, raw_value = content.partition("=")
        normalized_key = key.strip()

        # Keep an `export ` prefix on the rewritten line so shell-sourced .env files
        # keep working.
        export_prefix = ""
        if normalized_key.startswith("export "):
            export_prefix = "export "
            normalized_key = normalized_key[len(export_prefix):].strip()

        if normalized_key not in secret_keys:
            updated_lines.append(line)
            continue

        value_text = raw_value.rstrip()
        comment = ""
        comment_index = value_text.find(" #")
        if comment_index != -1:
            comment = value_text[comment_index:]
            value_text = value_text[:comment_index]

        secret_value = value_text.strip().strip('"').strip("'")
        hashed_key = f"{normalized_key}_HASHED"
        hashed_value = encrypt_secret_value(secret_value, passphrase=passphrase)
        updated_lines.append(f"{export_prefix}{hashed_key}={hashed_value}{comment}{line_ending}")
        updated_keys.append(normalized_key)

    return updated_lines, updated_keys
