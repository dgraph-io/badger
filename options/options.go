package options

// FileLoadingMode specifies how data in LSM table files and value log files should
// be loaded.
type FileLoadingMode int

const (
	// FileIO indicates that files must be loaded using standard I/O
	FileIO FileLoadingMode = iota
	// LoadToRAM indicates that file must be loaded into RAM
	LoadToRAM
	// MemoryMap indicates that that the file must be memory-mapped
	MemoryMap
)
