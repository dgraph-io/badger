/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"reflect"
	"testing"

	"github.com/dgraph-io/badger/v4/options"
)

func TestOptions(t *testing.T) {
	t.Run("default options", func(t *testing.T) {
		// copy all the default options over to a big SuperFlag string
		defaultSuperFlag := generateSuperFlag(DefaultOptions(""))
		// fill an empty Options with values from the SuperFlag
		generated := Options{}.FromSuperFlag(defaultSuperFlag)
		// make sure they're equal
		if !optionsEqual(DefaultOptions(""), generated) {
			t.Fatal("generated default SuperFlag != default Options")
		}
		// check that values are overwritten properly
		overwritten := DefaultOptions("").FromSuperFlag("numgoroutines=1234")
		if overwritten.NumGoroutines != 1234 {
			t.Fatal("Option value not overwritten by SuperFlag value")
		}
	})

	t.Run("special flags", func(t *testing.T) {
		o1 := DefaultOptions("")
		o1.NamespaceOffset = 10
		o1.Compression = options.ZSTD
		o1.ZSTDCompressionLevel = 2
		o1.NumGoroutines = 20

		o2 := DefaultOptions("")
		o2.NamespaceOffset = 10
		o2 = o2.FromSuperFlag("compression=zstd:2; numgoroutines=20;")

		// make sure they're equal
		if !optionsEqual(o1, o2) {
			t.Fatal("generated superFlag != expected options")
		}
	})
}

// optionsEqual just compares the values of two Options structs
func optionsEqual(o1, o2 Options) bool {
	o1v := reflect.ValueOf(&o1).Elem()
	o2v := reflect.ValueOf(&o2).Elem()
	for i := 0; i < o1v.NumField(); i++ {
		if o1v.Field(i).CanInterface() {
			kind := o1v.Field(i).Kind()
			// compare values
			switch kind {
			case reflect.Bool:
				if o1v.Field(i).Bool() != o2v.Field(i).Bool() {
					return false
				}
			case reflect.Int, reflect.Int64:
				if o1v.Field(i).Int() != o2v.Field(i).Int() {
					return false
				}
			case reflect.Uint32, reflect.Uint64:
				if o1v.Field(i).Uint() != o2v.Field(i).Uint() {
					return false
				}
			case reflect.Float64:
				if o1v.Field(i).Float() != o2v.Field(i).Float() {
					return false
				}
			case reflect.String:
				if o1v.Field(i).String() != o2v.Field(i).String() {
					return false
				}
			}
		}
	}
	return true
}
