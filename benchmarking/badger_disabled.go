// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build !badger

package main

import "log"

func newBadgerDB(dir string) DB {
	log.Fatalf("pebble not compiled with Badger support: recompile with \"-tags badger\"\n")
	return nil
}
