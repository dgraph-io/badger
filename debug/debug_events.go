// +build debugevents,!debugcompat

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
	"golang.org/x/net/trace"
)

type EventLog struct {
	elog trace.EventLog
}

func Printf(format string, v ...interface{}) {}

func Println(v ...interface{}) {}

func (e *EventLog) Printf(fmt string, a ...interface{}) {
	e.elog.Printf(fmt, a...)
}

func (e *EventLog) Errorf(fmt string, a ...interface{}) {
	e.elog.Errorf(fmt, a...)
}

func (e *EventLog) Finish() {
	e.elog.Finish()
}

func NewEventLog(family, title string) *EventLog {
	return &EventLog{elog: trace.NewEventLog(family, title)}
}
