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

import (
	"log"
	"os"

	"golang.org/x/net/trace"
)

type compatEventLogger struct {
	el trace.EventLog
}

// New would be the function that initalizes an existing EventLog object.
// ie., create trace object and attach it.
func (l *compatEventLogger) New(family, title string) EventLogger {
	l.el = trace.NewEventLog(family, title)
	return l
}

// Printf formats its arguments with fmt.Sprintf and adds the
// result to the event log.
func (l *compatEventLogger) Printf(fmt string, a ...interface{}) {
	l.el.Printf(fmt, a...)
}

// Errorf is like Printf, but it marks this event as an error.
func (l *compatEventLogger) Errorf(fmt string, a ...interface{}) {
	l.el.Errorf(fmt, a...)
}

// Finish declares that this event log is complete.
// The event log should not be used after calling this method.
func (l *compatEventLogger) Finish() {
	l.el.Finish()
}

// UseCompatEventLogger uses an instance of trace.EventLog as EventLogger.
func UseCompatEventLogger() {
	Use(&compatEventLogger{})
}

// UseCompatStdLogger uses the standard Go log package as StdLogger.
func UseCompatStdLogger() {
	Use(log.New(os.Stderr, "", log.LstdFlags))
}
