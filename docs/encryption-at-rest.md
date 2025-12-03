# Encryption at Rest in Dgraph and Badger

Badger provides encryption at rest using AES encryption, enabling compliance with security standards
such as HIPAA and PCI DSS. This feature was introduced in Badger v2 and is available to all systems
built on Badger, including Dgraph.

## Overview

Badger implements encryption at the storage layer, allowing systems like Dgraph to inherit
encryption capabilities without additional implementation. This separation of concerns means:

- Badger manages data security and encryption at the disk level
- Higher-level systems like Dgraph focus on distributed operations and graph semantics
- All Badger-based systems benefit from encryption improvements

## Encryption Algorithm

Badger uses the
[Advanced Encryption Standard (AES)](https://en.wikipedia.org/wiki/Advanced_Encryption_Standard),
standardized by NIST and widely adopted across databases including MongoDB, SQLite, and RocksDB. AES
is a symmetric encryption algorithm: the same key encrypts and decrypts data.

AES key sizes: 128, 192, or 256 bits. All provide strong security; 128-bit keys are computationally
infeasible to brute force.

## Key Management

Badger uses a two-tier key system:

### Master Key

The user-provided AES encryption key that encrypts data keys. Master key length determines AES
variant:

- 16 bytes: AES-128
- 24 bytes: AES-192
- 32 bytes: AES-256

**Important:** Use a cryptographically secure random key. Never use predictable strings. Generate
keys using a password manager or secure random generator.

### Data Keys

Auto-generated keys that encrypt actual data on disk. Each encrypted data key is stored alongside
the encrypted data. Master keys encrypt data keys, not data directly.

**Benefits:**

- Master key rotation only requires re-encrypting data keys (small, fast operation)
- Data keys rotate automatically without re-encrypting all data
- Minimal performance impact during key rotation

## Key Rotation

### Data Key Rotation

Badger automatically rotates data keys every 10 days by default. Configure the rotation interval
using `Options.WithEncryptionKeyRotationDuration`.

All historical data keys are retained to decrypt older data. Each data key is 32 bytes; 1000 keys
consume 32KB. At 10-day intervals, this represents approximately 27 years of keys.

### Master Key Rotation

Users must manually rotate master keys. Use the `rotate` command:

```shell
badger rotate --dir=badger_dir --old-key-path=old/path --new-key-path=new/path
```

**Requirements:**

- Database must be offline during master key rotation
- Only data keys are re-encrypted (fast operation)
- Future versions may support online rotation

## Initialization Vectors

To prevent identical plaintext from producing identical ciphertext, Badger uses Initialization
Vectors (IVs).

### SSTable Encryption

Each 4KB block in SSTables uses a unique 16-byte IV stored in plaintext at the end of the encrypted
block. Storage overhead: 0.4% (16 bytes per 4KB block).

**Security:** IVs can be stored in plaintext. Decryption requires the data key, which requires the
master key. Knowledge of the IV alone is insufficient.

### Value Log Encryption

Value log entries are encrypted individually to match access patterns. To minimize storage overhead,
Badger uses a 12-byte file-level IV combined with a 4-byte value offset to form the 16-byte IV.

**Benefits:**

- Saves 16 bytes per value entry
- 12-byte overhead per vlog file (vs 16 bytes per value)
- For 10,000 entries: 12 bytes total vs 160,000 bytes with per-value IVs

## Enabling Encryption

### New Database

Enable encryption when creating a new database:

```go
opts := badger.DefaultOptions("/tmp/badger").
    WithEncryptionKey(masterKey).
    WithEncryptionKeyRotationDuration(dataKeyRotationDuration) // defaults to 10 days
```

### Existing Database

Enable encryption on an unencrypted database:

```shell
badger rotate --dir=badger_dir --new-key-path=new/path
```

**Note:** This enables encryption for new files only. Existing data is encrypted during compaction
as new files are generated. Badger operates in hybrid mode, tracking encryption status per file.

### Immediate Full Encryption

To encrypt all existing data immediately:

1. Export the database: `badger backup --dir=badger_dir -f backup.bak`
2. Create a new encrypted database instance
3. Restore the data: `badger restore --dir=new_badger_dir -f backup.bak`

Alternatively, use the Stream Framework and StreamWriter interface for in-place encryption with high
throughput.

## Security Considerations

### Key Security

- Store master keys securely (key management service, secure vault)
- Rotate master keys regularly
- Use strong, randomly generated keys
- Protect physical access to systems performing encryption

### Key Leakage

Key security is more critical than key size. Threats include:

- Side-channel attacks (electromagnetic radiation analysis)
- Key reuse patterns enabling cryptanalysis
- Physical access to encryption systems

Regular key rotation mitigates these risks.

## Terminology

In this context, "key" refers to:

- **Database key**: The key in a key-value pair stored in Badger
- **Encryption key**: The cryptographic key used for encryption/decryption (master key or data key)

When ambiguous, this document uses "encryption key" for cryptographic keys.
