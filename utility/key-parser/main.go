package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

type KeyRecord struct {
	Key       string
	KeyBytes  []byte
	KeyHex    string
	Version   int64
	Size      int64
	Meta      string
	Discard   bool
	ParsedKey ParsedKey
}

var (
	filePath   = flag.String("file", "../../badger/info-readonly-memtable-levels-showkeys-histogram--show-tables-2.log", "Path to the input file")
	dbHost     = flag.String("db-host", "localhost", "PostgreSQL host")
	dbPort     = flag.Int("db-port", 5432, "PostgreSQL port")
	dbUser     = flag.String("db-user", "postgres", "PostgreSQL user")
	dbPassword = flag.String("db-password", "password", "PostgreSQL password")
	dbName     = flag.String("db-name", "badger_keys", "PostgreSQL database name")
	writeToDB  = flag.Bool("write-db", true, "Write to PostgreSQL database")
	printOnly  = flag.Bool("print", false, "Print decoded keys to stdout")
)

func main() {
	flag.Parse()

	if *filePath == "" {
		log.Fatal("Please provide a file path using -file flag")
	}

	file, err := os.Open(*filePath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	var db *sql.DB
	if *writeToDB {
		db, err = initDB()
		if err != nil {
			log.Fatalf("Error initializing database: %v", err)
		}
		defer db.Close()

		if err := createTableIfNotExists(db); err != nil {
			log.Fatalf("Error creating table: %v", err)
		}
	}

	scanner := bufio.NewScanner(file)
	// Increase buffer size for large lines if needed
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	lineCount := 0
	matchCount := 0

	for scanner.Scan() {
		lineCount++
		line := scanner.Text()

		record, err := parseLine(line)
		if err != nil {
			// Line doesn't match pattern, skip
			continue
		}

		matchCount++

		if *printOnly {
			fmt.Printf("Line %d:\n", lineCount)
			fmt.Printf("  Key (hex): %s\n", record.KeyHex)
			fmt.Printf("  Key (decoded): %s\n", record.Key)
			fmt.Printf("  Version: %d\n", record.Version)
			fmt.Printf("  Size: %d\n", record.Size)
			fmt.Printf("  Meta: %s\n", record.Meta)
			fmt.Printf("  Discard: %v\n", record.Discard)
			fmt.Println()
		}

		if *writeToDB && db != nil {
			if err := insertRecord(db, record); err != nil {
				log.Printf("Error inserting record at line %d: %v", lineCount, err)
			}
		}

		// Progress indicator for large files
		if lineCount%100000 == 0 {
			fmt.Fprintf(os.Stderr, "Processed %d lines, found %d matches...\n", lineCount, matchCount)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	fmt.Fprintf(os.Stderr, "\nProcessing complete!\n")
	fmt.Fprintf(os.Stderr, "Total lines processed: %d\n", lineCount)
	fmt.Fprintf(os.Stderr, "Total matches found: %d\n", matchCount)
}

func parseLine(line string) (*KeyRecord, error) {
	matches := LinePattern.FindStringSubmatch(line)
	if matches == nil || len(matches) != 6 {
		return nil, fmt.Errorf("line doesn't match pattern")
	}

	keyHex := matches[1]
	versionStr := matches[2]
	sizeStr := matches[3]
	meta := matches[4]
	var discard bool
	if len(matches) > 5 && matches[5] != "" {
		discard = matches[5] == "\t{discard}"
	}

	// Decode hex key to bytes
	keyBytes, err := hex.DecodeString(keyHex)
	if err != nil {
		return nil, fmt.Errorf("error decoding hex key: %v", err)
	}

	// Convert to string only if it doesn't contain null bytes
	var keyStr string
	if bytes.IndexByte(keyBytes, 0) == -1 {
		keyStr = string(keyBytes)
	} else {
		// Keep as hex if it contains null bytes
		sliced := bytes.Trim(keyBytes, "\x00")
		keyStr = string(sliced)
	}

	version, err := strconv.ParseInt(versionStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing version: %v", err)
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing size: %v", err)
	}

	r := &KeyRecord{
		Key:      keyStr,
		KeyBytes: keyBytes,
		KeyHex:   keyHex,
		Version:  version,
		Size:     size,
		Meta:     meta,
		Discard:  discard,
	}

	r.ParsedKey, err = Parse(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("error parsing key: %v", err)
	}

	return r, nil
}

func initDB() (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		*dbHost, *dbPort, *dbUser, *dbPassword, *dbName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("cannot connect to database: %v", err)
	}

	return db, nil
}

func createTableIfNotExists(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS badger_keys (
		id SERIAL PRIMARY KEY,
		key_hex TEXT NOT NULL,
		version BIGINT NOT NULL,
		size BIGINT NOT NULL,
		meta TEXT NOT NULL,
		discard BOOLEAN DEFAULT FALSE,
		attr TEXT,
		uid TEXT,
		has_start_uid BOOLEAN,
		start_uid BIGINT,
		term TEXT,
		count INTEGER,
		byte_prefix BYTEA,
		byte_type TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_key_hex ON badger_keys(key_hex);
	CREATE INDEX IF NOT EXISTS idx_uid ON badger_keys(uid);
	`

	_, err := db.Exec(query)
	return err
}

func insertRecord(db *sql.DB, record *KeyRecord) error {
	query := `
	INSERT INTO badger_keys (
		key_hex,
		version,
		size,
		meta,
		discard,
		attr,
		uid,
		has_start_uid,
		term,
		start_uid,
		count,
		byte_prefix,
		byte_type
	)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
	`

	attribute := strings.ReplaceAll(record.ParsedKey.Attr, "\x00", "")

	_, err := db.Exec(query,
		record.KeyHex,
		record.Version,
		record.Size,
		record.Meta,
		record.Discard,
		attribute,
		record.ParsedKey.Uid,
		record.ParsedKey.HasStartUid,
		record.ParsedKey.Term,
		record.ParsedKey.StartUid,
		record.ParsedKey.Count,
		record.ParsedKey.BytePrefix,
		record.ParsedKey.ByteType,)
	return err
}
