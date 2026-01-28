package main

import "regexp"

const pattern = `Key:\s+([0-9a-fA-F]+)\s+version:\s+(\d+)\s+size:\s+(\d+)\s+meta:\s+([b0-9a-fA-F]+)(\s+\{discard\})?`

// Pattern matches: Key: <hex>	version: <num>	size: <num>	meta: <hex>
// Key: 000000000000000000000b4576656e742e76616c75650000000000006347be\tversion: 1\tsize: 83\tmeta: b1000
var LinePattern = regexp.MustCompile(pattern)
