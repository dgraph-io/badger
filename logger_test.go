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

package badger

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockLogger struct {
	output string
}

func (l *mockLogger) Errorf(f string, v ...interface{}) {
	l.output = fmt.Sprintf("ERROR: "+f, v...)
}

func (l *mockLogger) Infof(f string, v ...interface{}) {
	l.output = fmt.Sprintf("INFO: "+f, v...)
}

func (l *mockLogger) Warningf(f string, v ...interface{}) {
	l.output = fmt.Sprintf("WARNING: "+f, v...)
}

// Test that the DB-specific log is used instead of the global log.
func TestDbLog(t *testing.T) {
	db := DB{}
	l := &mockLogger{}
	db.SetLogger(l)

	SafeErrorf(&db, "test")
	require.Equal(t, "ERROR: test", l.output)
	SafeInfof(&db, "test")
	require.Equal(t, "INFO: test", l.output)
	SafeWarningf(&db, "test")
	require.Equal(t, "WARNING: test", l.output)
}

// Test there are no errors when the db is nil or does not have a logger.
func TestNoDbLog(t *testing.T) {
	l := &mockLogger{}
	SetLogger(l)

	SafeErrorf(nil, "test")
	require.Equal(t, "ERROR: test", l.output)
	SafeInfof(nil, "test")
	require.Equal(t, "INFO: test", l.output)
	SafeWarningf(nil, "test")
	require.Equal(t, "WARNING: test", l.output)

	db := DB{}
	SafeErrorf(&db, "test")
	require.Equal(t, "ERROR: test", l.output)
	SafeInfof(&db, "test")
	require.Equal(t, "INFO: test", l.output)
	SafeWarningf(&db, "test")
	require.Equal(t, "WARNING: test", l.output)
}
