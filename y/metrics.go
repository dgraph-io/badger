/*
 * Copyright (C) 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package y

import (
	"expvar"
)

var (
	// These are cummulative
	NumGets         *expvar.Int
	NumPuts         *expvar.Int
	NumReads        *expvar.Int
	NumWrites       *expvar.Int
	NumBytesRead    *expvar.Int
	NumBytesWritten *expvar.Int
)

func init() {
	NumReads = expvar.NewInt("NumBadgerDiskReads")
	NumWrites = expvar.NewInt("NumBadgerDiskWrites")
	NumGets = expvar.NewInt("NumBadgerGets")
	NumPuts = expvar.NewInt("NumBadgerPuts")
	NumBytesRead = expvar.NewInt("BadgerBytesRead")
	NumBytesWritten = expvar.NewInt("BadgerBytesWritten")
}
