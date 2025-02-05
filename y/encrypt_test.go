/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"crypto/aes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestXORBlock(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)

	var iv []byte
	{
		b, err := aes.NewCipher(key)
		require.NoError(t, err)
		iv = make([]byte, b.BlockSize())
		_, _ = rand.Read(iv)
		t.Logf("Using %d size IV\n", len(iv))
	}

	src := make([]byte, 1024)
	_, _ = rand.Read(src)

	dst := make([]byte, 1024)
	err := XORBlock(dst, src, key, iv)
	require.NoError(t, err)

	act := make([]byte, 1024)
	err = XORBlock(act, dst, key, iv)
	require.NoError(t, err)
	require.Equal(t, src, act)

	// Now check if we can use the same byte slice as src and dst. While this is useful to know that
	// we can use src and dst as the same slice, this isn't applicable to Badger because we're
	// reading data right off mmap. We should not modify that data, so we have to use a different
	// slice for dst anyway.
	cp := append([]byte{}, src...)
	err = XORBlock(cp, cp, key, iv)
	require.NoError(t, err)
	require.Equal(t, dst, cp)

	err = XORBlock(cp, cp, key, iv)
	require.NoError(t, err)
	require.Equal(t, src, cp)
}
