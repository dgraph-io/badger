package main

import (
	"bufio"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type SSTableRecord struct {
	Level               int
	ID                  int
	TotalKeys           int
	CompressionRatio    float64
	StaleDataRatio      float64
	UncompressedSize    int64
	IndexSize           int64
	BFSize              int64
	LeftKeyHex          string
	LeftKeyDecoded      string
	LeftVersion         string
	RightKeyHex         string
	RightKeyDecoded     string
	RightVersion        string
}

func main() {
	// Input file path (hardcoded as per requirements)
	inputFile := "../../badger/info-readonly-memtable-levels-showkeys-histogram--show-tables.log"
	outputFile := "sstable_summary.csv"

	// Open input file
	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Printf("Error opening input file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Create output CSV file
	csvFile, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer csvFile.Close()

	// Create CSV writer
	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// Write CSV header
	header := []string{
		"level", "id", "total_keys", "compression_ratio", "staleData",
		"uncompressed_size_bytes", "index_size_bytes", "BF_size_bytes",
		"left_key_hex", "left_key_decoded", "left_version",
		"right_key_hex", "right_key_decoded", "right_version",
	}
	if err := writer.Write(header); err != nil {
		fmt.Printf("Error writing header: %v\n", err)
		os.Exit(1)
	}

	// Compile regex pattern to match SSTable lines
	// Note: The pattern matches across line breaks since the file has wrapped lines
	// Updated to properly capture size values with units (MiB, KiB, B)
	pattern := `SSTable \[L(\d+), (\d+), (\d+)\] \[([\d.]+), ([\d.]+), ([^,]+?), ([^,]+?), ([^\]]+?)\] \[([^,]+), v(\d+) -> ([^,]+), v(\d+)\]`
	regex := regexp.MustCompile(pattern)

	// Read file line by line, handling line continuations
	scanner := bufio.NewScanner(file)
	// Increase buffer size to handle long lines
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024*1024)
	lineNum := 0
	recordsProcessed := 0

	// Buffer to accumulate lines that might be continuations
	var lineBuffer strings.Builder

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Check if line ends with a continuation (not closed bracket)
		if !strings.HasSuffix(line, "]") {
			lineBuffer.WriteString(line)
			continue
		}

		// If we have buffered content, prepend it
		if lineBuffer.Len() > 0 {
			line = lineBuffer.String() + line
			lineBuffer.Reset()
		}

		// Match SSTable lines
		matches := regex.FindStringSubmatch(line)
		if matches == nil {
			continue
		}

		// Parse the matched groups
		record := SSTableRecord{
			Level:          parseInt(matches[1]),
			ID:             parseInt(matches[2]),
			TotalKeys:      parseInt(matches[3]),
			CompressionRatio: parseFloat(matches[4]),
			StaleDataRatio:  parseFloat(matches[5]),
			LeftKeyHex:     strings.TrimSpace(matches[9]),
			LeftVersion:    "v" + matches[10],
			RightKeyHex:    strings.TrimSpace(matches[11]),
			RightVersion:   "v" + matches[12],
		}

		// Parse sizes (convert MiB/KiB/B to bytes)
		uncompressedSize := parseSize(strings.TrimSpace(matches[6]))
		indexSize := parseSize(strings.TrimSpace(matches[7]))
		bfSize := parseSize(strings.TrimSpace(matches[8]))
		record.UncompressedSize = uncompressedSize
		record.IndexSize = indexSize
		record.BFSize = bfSize

		// Decode keys
		record.LeftKeyDecoded = decodeKey(record.LeftKeyHex)
		record.RightKeyDecoded = decodeKey(record.RightKeyHex)

		// Write to CSV
		row := []string{
			strconv.Itoa(record.Level),
			strconv.Itoa(record.ID),
			strconv.Itoa(record.TotalKeys),
			fmt.Sprintf("%.2f", record.CompressionRatio),
			fmt.Sprintf("%.2f", record.StaleDataRatio),
			strconv.FormatInt(record.UncompressedSize, 10),
			strconv.FormatInt(record.IndexSize, 10),
			strconv.FormatInt(record.BFSize, 10),
			record.LeftKeyHex,
			record.LeftKeyDecoded,
			record.LeftVersion,
			record.RightKeyHex,
			record.RightKeyDecoded,
			record.RightVersion,
		}

		if err := writer.Write(row); err != nil {
			fmt.Printf("Error writing record at line %d: %v\n", lineNum, err)
			continue
		}

		recordsProcessed++
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Processing complete. Processed %d SSTable records.\n", recordsProcessed)
	fmt.Printf("Output written to: %s\n", outputFile)
}

func parseInt(s string) int {
	val, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return val
}

func parseFloat(s string) float64 {
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0.0
	}
	return val
}

func parseSize(s string) int64 {
	s = strings.TrimSpace(s)
	s = strings.TrimRight(s, " B") // Remove trailing " B" if present

	// Check for MiB, KiB, or B
	if strings.HasSuffix(s, "MiB") {
		val := strings.TrimSuffix(s, "MiB")
		f, _ := strconv.ParseFloat(val, 64)
		return int64(f * 1024 * 1024)
	} else if strings.HasSuffix(s, "KiB") {
		val := strings.TrimSuffix(s, "KiB")
		f, _ := strconv.ParseFloat(val, 64)
		return int64(f * 1024)
	} else if strings.HasSuffix(s, "B") {
		val := strings.TrimSuffix(s, "B")
		f, _ := strconv.ParseFloat(val, 64)
		return int64(f)
	}

	// Try to parse as plain number (in case format varies)
	f, _ := strconv.ParseFloat(s, 64)
	return int64(f)
}

func decodeKey(hexKey string) string {
	// Decode hex to bytes
	keyBytes, err := hex.DecodeString(hexKey)
	if err != nil {
		return hexKey // Return original if decoding fails
	}

	// Try to find entity.attribute pattern in the bytes
	// Look for a dot (0x2E) followed by non-null bytes
	for i := 0; i < len(keyBytes)-1; i++ {
		if keyBytes[i] == 0x2E { // Dot character
			// Find the start of the entity name (backwards from dot)
			start := i
			for j := i - 1; j >= 0; j-- {
				if keyBytes[j] == 0x00 {
					start = j + 1
					break
				}
			}

			// Find the end of the attribute name (forwards from dot)
			end := len(keyBytes)
			for j := i + 1; j < len(keyBytes); j++ {
				if keyBytes[j] == 0x00 {
					end = j
					break
				}
			}

			// Extract and convert to string
			if start < i && i < end {
				entityAttr := keyBytes[start:end]
				return string(entityAttr)
			}
		}
	}

	// If no pattern found, return the hex string
	return hexKey
}
