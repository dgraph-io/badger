# Key Parser Utility

A utility for parsing large Badger database key dump files and optionally storing the results in PostgreSQL.

## Features

- **Memory-efficient**: Reads files line-by-line, suitable for very large files (60GB+)
- **Hex decoding**: Automatically decodes hex-encoded keys to readable strings
- **PostgreSQL integration**: Optional database storage for analysis
- **Progress tracking**: Shows progress every 100,000 lines
- **Flexible output**: Print to stdout or write to database (or both)

## Input Format

The utility parses lines matching this pattern:
```
Key: 000000000000000000000b6467726170682e74797065000000000002e8ce88	version: 1	size: 92	meta: b1000
```

## Installation

```bash
cd utility/key-parser
go mod init key-parser
go get github.com/lib/pq
go build
```

## Usage

### Print decoded keys only (default)

```bash
./key-parser -file /path/to/large/file.txt
```

### Write to PostgreSQL

First, create the database:
```sql
CREATE DATABASE badger_keys;
```

Then run with database flags:
```bash
./key-parser \
  -file /path/to/large/file.txt \
  -write-db \
  -db-host localhost \
  -db-port 5432 \
  -db-user postgres \
  -db-password yourpassword \
  -db-name badger_keys
```

### Both print and write to database

```bash
./key-parser \
  -file /path/to/large/file.txt \
  -print \
  -write-db \
  -db-password yourpassword
```

### Print only without database

```bash
./key-parser -file /path/to/large/file.txt -print -write-db=false
```

## Command-line Flags

- `-file`: Path to the input file (required)
- `-print`: Print decoded keys to stdout (default: true)
- `-write-db`: Write records to PostgreSQL (default: false)
- `-db-host`: PostgreSQL host (default: localhost)
- `-db-port`: PostgreSQL port (default: 5432)
- `-db-user`: PostgreSQL user (default: postgres)
- `-db-password`: PostgreSQL password (default: "")
- `-db-name`: PostgreSQL database name (default: badger_keys)

## Database Schema

When using `-write-db`, the utility creates this table:

```sql
CREATE TABLE badger_keys (
    id SERIAL PRIMARY KEY,
    key_decoded TEXT NOT NULL,
    key_hex TEXT NOT NULL,
    version BIGINT NOT NULL,
    size BIGINT NOT NULL,
    meta TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Indexes are automatically created on `key_hex` and `key_decoded` for faster queries.

## Example Output

```
Line 1:
  Key (hex): 000000000000000000000b6467726170682e74797065000000000002e8ce88
  Key (decoded): dgraph.type��
  Version: 1
  Size: 92
  Meta: b1000

Processed 100000 lines, found 45231 matches...
Processed 200000 lines, found 89456 matches...
...

Processing complete!
Total lines processed: 1500000
Total matches found: 678923
```

## Performance Tips

- For very large files, consider using `-print=false` and only `-write-db` to reduce I/O
- The utility shows progress every 100,000 lines to stderr
- Database writes are done individually; for better performance, consider batching (modify the code)
- PostgreSQL connection pooling is handled automatically by the driver

## Sample Queries

After loading data into PostgreSQL:

```sql
-- Count total keys
SELECT COUNT(*) FROM badger_keys;

-- Find all keys with a specific prefix
SELECT key_decoded, version, size FROM badger_keys 
WHERE key_decoded LIKE 'dgraph%' 
LIMIT 10;

-- Keys by version
SELECT version, COUNT(*) FROM badger_keys 
GROUP BY version 
ORDER BY version;

-- Largest keys
SELECT key_decoded, size FROM badger_keys 
ORDER BY size DESC 
LIMIT 20;
```
