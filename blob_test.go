/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
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
	"io/ioutil"
	"testing"

	"github.com/dgraph-io/badger/v2/y"
	"github.com/stretchr/testify/require"
)

func TestBlobInsertAndRead(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(tmpDir)

	bfb := newBlobFileBuilder(1, tmpDir)

	var vpList []*valuePointer
	// Insert 100 entries.
	for i := 0; i < 100; i++ {
		vp, err := bfb.addValue([]byte(fmt.Sprintf("foo-%d", i)))
		require.NoError(t, err)
		vpList = append(vpList, vp)
	}

	bf, err := bfb.finish()
	require.NoError(t, err)
	defer bf.close()

	var s y.Slice
	for i, bp := range vpList {
		val, err := bf.read(bp, &s)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("foo-%d", i), string(val))
	}
}

func TestBlobPointer(t *testing.T) {
	vpList := []valuePointer{
		valuePointer{}, // Empty value
		valuePointer{108192383, 22179400, 130081203}, // Large values
	}
	for _, vp := range vpList {
		var newvp valuePointer
		buf := vp.Encode()
		newvp.Decode(buf)
		require.EqualValues(t, vp, newvp)
	}

}
