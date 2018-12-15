package cmd

import (
	"github.com/dgraph-io/badger"
	"github.com/spf13/cobra"
)

var flattenCmd = &cobra.Command{
	Use:   "flatten",
	Short: "Flatten the LSM tree.",
	Long: `
This command would compact all the LSM tables into one level.
`,
	RunE: flatten,
}

func init() {
	RootCmd.AddCommand(flattenCmd)
}

func flatten(cmd *cobra.Command, args []string) error {
	opts := badger.DefaultOptions
	opts.Dir = sstDir
	opts.ValueDir = vlogDir
	opts.Truncate = truncate
	opts.NumCompactors = 0

	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Flatten()
}
