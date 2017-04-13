/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package y

import (
	"context"

	"os"
	"syscall"

	"golang.org/x/net/trace"
)

var EmptySlice = []byte{}

func Trace(ctx context.Context, format string, args ...interface{}) {
	tr, ok := trace.FromContext(ctx)
	if !ok {
		return
	}
	tr.LazyPrintf(format, args...)
}

func OpenSyncedFile(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_RDWR|os.O_CREATE|syscall.O_DSYNC, 0666)
}
