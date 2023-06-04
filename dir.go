//go:build !windows && !plan9
// +build !windows,!plan9

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
	"errors"
)

type directoryLockGuard struct {
}

func acquireDirectoryLock(dirPath string, pidFileName string, readOnly bool) (
	*directoryLockGuard, error) {
	panic(errors.New("acquireDirectoryLock not implemented"))

}

func (guard *directoryLockGuard) release() error {
	panic(errors.New("guard not implemented"))
}

func syncDir(dir string) error {
	panic(errors.New("syncDir not implemented"))
}
