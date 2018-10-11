/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
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

package debug

// Logger is implemented by any logging system that is used for standard logs.
type Logger interface {
	Printf(string, ...interface{})
	Println(...interface{})
}

// stdLogger is the logger used for standard (info) logs.
var stdLogger Logger

// Printf prints to the standard logger using fmt.Sprintf formatting.
func Printf(format string, v ...interface{}) {
	stdLogger.Printf(format, v...)
}

// Println prints to the standard logger.
func Println(v ...interface{}) {
	stdLogger.Println(v...)
}
