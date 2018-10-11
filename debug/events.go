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

// EventLogger is composed from trace.EventLog interface.
type EventLogger interface {
	// Printf formats its arguments with fmt.Sprintf and adds the
	// result to the event log.
	Printf(string, ...interface{})

	// Errorf is like Printf, but it marks this event as an error.
	Errorf(string, ...interface{})

	// Finish declares that this event log is complete.
	// The event log should not be used after calling this method.
	Finish()

	// New would be the function that initalizes an existing EventLog object.
	// ie., create trace object and attach it.
	New(string, string) EventLogger
}

var eventLogger EventLogger

// NewEventLog returns a new EventLog with the specified family name
// and title.
func NewEventLog(family, title string) EventLogger {
	return eventLogger.New(family, title)
}
