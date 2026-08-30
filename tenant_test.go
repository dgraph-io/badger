/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/y"
)

func tenantTestOptions(dir string) Options {
	return getTestOptions(dir).WithMultiTenancy(true) // sets NamespaceOffset=0
}

func TestTenantCreateAndGet(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	tm := db.Tenants()
	require.NotNil(t, tm)

	ta, err := tm.Create("acme")
	require.NoError(t, err)
	require.Equal(t, uint64(1), ta.ID)
	require.Equal(t, "acme", ta.Name)

	got, err := tm.Get(ta.ID)
	require.NoError(t, err)
	require.Equal(t, "acme", got.Name)

	got2, err := tm.GetByName("acme")
	require.NoError(t, err)
	require.Equal(t, ta.ID, got2.ID)

	_, err = tm.Create("acme")
	require.ErrorIs(t, err, ErrTenantExists)

	_, err = tm.Get(9999)
	require.ErrorIs(t, err, ErrTenantNotFound)
}

func TestTenantList(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-list")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	tm := db.Tenants()
	for _, n := range []string{"a", "b", "c"} {
		_, err := tm.Create(n)
		require.NoError(t, err)
	}
	list, err := tm.List()
	require.NoError(t, err)
	require.Len(t, list, 3)
}

// TestTenantNameLengthsNoKeyCollision guards against the id-key vs name-index-key
// collision: id records and the name index must use disjoint prefixes so a name of any
// length (notably 3 chars, which previously produced a name key the same length as an id
// key) is never misclassified. Exercises Create, List, GetByName, and restart reload.
func TestTenantNameLengthsNoKeyCollision(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-namelen")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	names := []string{"a", "ab", "abc", "abcd", "abcde", "abcdef", "abcdefgh"}

	doOpen := func() *DB {
		db, err := Open(tenantTestOptions(dir))
		require.NoError(t, err)
		return db
	}

	db := doOpen()
	tm := db.Tenants()
	for _, n := range names {
		_, err := tm.Create(n)
		require.NoError(t, err)
	}

	list, err := tm.List()
	require.NoError(t, err)
	require.Len(t, list, len(names))

	for _, n := range names {
		got, err := tm.GetByName(n)
		require.NoError(t, err)
		require.Equal(t, n, got.Name)
	}
	require.NoError(t, db.Close())

	// Restart: the known-id cache must reload from id records only (no bogus ids from
	// name-index keys), and the next id must continue past the real max.
	db2 := doOpen()
	defer db2.Close()
	tm2 := db2.Tenants()
	list2, err := tm2.List()
	require.NoError(t, err)
	require.Len(t, list2, len(names))

	next, err := tm2.Create("brand-new")
	require.NoError(t, err)
	require.Equal(t, uint64(len(names)+1), next.ID)
}

func TestTenantBanUnban(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-ban")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	tm := db.Tenants()
	ta, err := tm.Create("acme")
	require.NoError(t, err)

	require.NoError(t, tm.Ban(ta.ID))
	require.Contains(t, db.BannedNamespaces(), ta.ID)

	// Writes to the banned namespace are rejected by the existing isBanned check.
	key := append(y.U64ToBytes(ta.ID), []byte("k")...)
	require.ErrorIs(t, db.Update(func(txn *Txn) error { return txn.Set(key, []byte("v")) }), ErrBannedKey)

	require.NoError(t, tm.Unban(ta.ID))
	require.NotContains(t, db.BannedNamespaces(), ta.ID)
	require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set(key, []byte("v")) }))
}

// TestTenantBanUnbanRestart verifies that an unban survives a DB restart: the ban key's
// delete tombstone must hide it from initBannedNamespaces on reopen.
func TestTenantBanUnbanRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-ban-restart")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	doOpen := func() *DB {
		db, err := Open(tenantTestOptions(dir))
		require.NoError(t, err)
		return db
	}

	db := doOpen()
	tm := db.Tenants()
	ta, err := tm.Create("acme")
	require.NoError(t, err)
	require.NoError(t, tm.Ban(ta.ID))
	require.NoError(t, tm.Unban(ta.ID))
	require.NotContains(t, db.BannedNamespaces(), ta.ID)
	require.NoError(t, db.Close())

	db2 := doOpen()
	defer db2.Close()
	// The namespace must stay unbanned after restart.
	require.NotContains(t, db2.BannedNamespaces(), ta.ID)
	// And writes to it must succeed.
	key := append(y.U64ToBytes(ta.ID), []byte("k")...)
	require.NoError(t, db2.Update(func(txn *Txn) error { return txn.Set(key, []byte("v")) }))
}

func TestTenantDelete(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-delete")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	tm := db.Tenants()
	ta, err := tm.Create("acme")
	require.NoError(t, err)

	require.NoError(t, tm.Delete(ta.ID))
	_, err = tm.Get(ta.ID)
	require.ErrorIs(t, err, ErrTenantNotFound)
	_, err = tm.GetByName("acme")
	require.ErrorIs(t, err, ErrTenantNotFound)
}

// TestTenantPurge verifies that Purge deregisters the tenant AND physically removes its
// keys, while leaving the namespace banned to prevent resurrection.
func TestTenantPurge(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-purge")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	tm := db.Tenants()
	ta, err := tm.Create("acme")
	require.NoError(t, err)

	scope, err := db.TenantScope(ta.ID)
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		require.NoError(t, scope.Set([]byte{byte('a' + i)}, []byte("v")))
	}

	require.NoError(t, tm.Purge(ta.ID))

	// Registry entry is gone.
	_, err = tm.Get(ta.ID)
	require.ErrorIs(t, err, ErrTenantNotFound)

	// All physical keys under the tenant prefix are gone.
	prefix := y.U64ToBytes(ta.ID)
	var count int
	require.NoError(t, db.View(func(txn *Txn) error {
		iopts := DefaultIteratorOptions
		iopts.Prefix = prefix
		iopts.PrefetchValues = false
		itr := txn.NewIterator(iopts)
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			count++
		}
		return nil
	}))
	require.Equal(t, 0, count)

	// Namespace stays banned so no writes can resurrect the purged tenant.
	require.Contains(t, db.BannedNamespaces(), ta.ID)
	key := append(y.U64ToBytes(ta.ID), []byte("k")...)
	require.ErrorIs(t, db.Update(func(txn *Txn) error { return txn.Set(key, []byte("v")) }), ErrBannedKey)
}

// TestTenantBanRestart verifies a ban survives restart (durable ban key re-applied by
// initBannedNamespaces).
func TestTenantBanRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-ban-restart")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	doOpen := func() *DB {
		db, err := Open(tenantTestOptions(dir))
		require.NoError(t, err)
		return db
	}

	db := doOpen()
	tm := db.Tenants()
	ta, err := tm.Create("acme")
	require.NoError(t, err)
	require.NoError(t, tm.Ban(ta.ID))
	require.NoError(t, db.Close())

	db2 := doOpen()
	defer db2.Close()
	require.Contains(t, db2.BannedNamespaces(), ta.ID)
	key := append(y.U64ToBytes(ta.ID), []byte("k")...)
	require.ErrorIs(t, db2.Update(func(txn *Txn) error { return txn.Set(key, []byte("v")) }), ErrBannedKey)
}

// TestTenantDeleteRestart verifies that after Delete + restart the namespace stays banned
// and the tenant is absent from the registry.
func TestTenantDeleteRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-delete-restart")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	doOpen := func() *DB {
		db, err := Open(tenantTestOptions(dir))
		require.NoError(t, err)
		return db
	}

	db := doOpen()
	tm := db.Tenants()
	ta, err := tm.Create("acme")
	require.NoError(t, err)
	require.NoError(t, tm.Delete(ta.ID))
	require.NoError(t, db.Close())

	db2 := doOpen()
	defer db2.Close()
	tm2 := db2.Tenants()
	_, err = tm2.Get(ta.ID)
	require.ErrorIs(t, err, ErrTenantNotFound)
	// Namespace stays banned so no further writes can resurrect it.
	require.Contains(t, db2.BannedNamespaces(), ta.ID)
	key := append(y.U64ToBytes(ta.ID), []byte("k")...)
	require.ErrorIs(t, db2.Update(func(txn *Txn) error { return txn.Set(key, []byte("v")) }), ErrBannedKey)
}

func TestTenantRestartPersistence(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-restart")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	doOpen := func() *DB {
		db, err := Open(tenantTestOptions(dir))
		require.NoError(t, err)
		return db
	}

	db := doOpen()
	tm := db.Tenants()
	ta, err := tm.Create("acme")
	require.NoError(t, err)
	require.NoError(t, tm.Ban(ta.ID))
	require.NoError(t, db.Close())

	db2 := doOpen()
	tm2 := db2.Tenants()
	got, err := tm2.Get(ta.ID)
	require.NoError(t, err)
	require.Equal(t, "acme", got.Name)

	// Known-id cache must be primed on reload so write enforcement works.
	require.True(t, tm2.isKnown(ta.ID))

	// Next id continues past the existing one.
	tb, err := tm2.Create("beta")
	require.NoError(t, err)
	require.Greater(t, tb.ID, ta.ID)
	require.NoError(t, db2.Close())
}

func TestTenantScopeIsolation(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-scope")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	tm := db.Tenants()
	ta, err := tm.Create("acme")
	require.NoError(t, err)
	tb, err := tm.Create("beta")
	require.NoError(t, err)

	sa, err := db.TenantScope(ta.ID)
	require.NoError(t, err)
	sb, err := db.TenantScope(tb.ID)
	require.NoError(t, err)

	// Same logical key under different tenants must not collide.
	require.NoError(t, sa.Set([]byte("k"), []byte("acme-val")))
	require.NoError(t, sb.Set([]byte("k"), []byte("beta-val")))

	it, err := sa.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, "acme-val", string(it))

	it2, err := sb.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, "beta-val", string(it2))

	_, err = db.TenantScope(9999)
	require.ErrorIs(t, err, ErrTenantNotFound)
}

func TestTenantScopeIterator(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-iter")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	tm := db.Tenants()
	ta, err := tm.Create("acme")
	require.NoError(t, err)
	tb, err := tm.Create("beta")
	require.NoError(t, err)

	sa, err := db.TenantScope(ta.ID)
	require.NoError(t, err)
	sb, err := db.TenantScope(tb.ID)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		require.NoError(t, sa.Set([]byte{byte('a' + i)}, []byte("acme")))
	}
	for i := 0; i < 2; i++ {
		require.NoError(t, sb.Set([]byte{byte('a' + i)}, []byte("beta")))
	}

	// Each tenant's iterator sees only its own keys, exposed as logical keys.
	itr, err := sa.NewIterator(DefaultIteratorOptions)
	require.NoError(t, err)
	defer itr.Close()
	var got []byte
	for itr.Rewind(); itr.Valid(); itr.Next() {
		got = append(got, itr.LogicalKey()[0])
	}
	require.Equal(t, []byte{'a', 'b', 'c'}, got)

	itr2, err := sb.NewIterator(DefaultIteratorOptions)
	require.NoError(t, err)
	defer itr2.Close()
	got = nil
	for itr2.Rewind(); itr2.Valid(); itr2.Next() {
		got = append(got, itr2.LogicalKey()[0])
	}
	require.Equal(t, []byte{'a', 'b'}, got)
}

func TestEnforceUnknownTenant(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-enforce")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	// NamespaceOffset == 0: an unknown 8-byte namespace prefix is rejected on writes.
	unknownKey := append(y.U64ToBytes(4242), []byte("k")...)
	require.ErrorIs(t, db.Update(func(txn *Txn) error { return txn.Set(unknownKey, []byte("v")) }), ErrUnknownTenant)

	// Register the tenant and the same write succeeds.
	ta, err := db.Tenants().Create("acme")
	require.NoError(t, err)
	registeredKey := append(y.U64ToBytes(ta.ID), []byte("k")...)
	require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set(registeredKey, []byte("v")) }))
}

func TestMultiTenancyDisabledBackwardsCompat(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-off")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Default options: multi-tenancy off; behavior is unchanged.
	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	require.Nil(t, db.Tenants())
	_, err = db.TenantScope(1)
	require.ErrorIs(t, err, ErrMultiTenancyNotEnabled)

	require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("k"), []byte("v")) }))
}

func TestTenantScopeGetMissing(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-get-missing")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	ta, err := db.Tenants().Create("acme")
	require.NoError(t, err)
	s, err := db.TenantScope(ta.ID)
	require.NoError(t, err)

	_, err = s.Get([]byte("missing"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

// TestTenantScopeTxn verifies atomic multi-key read-modify-write within a tenant via
// TenantScope.Update/View, and that isolation holds across tenants.
func TestTenantScopeTxn(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-txn")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	tm := db.Tenants()
	ta, err := tm.Create("acme")
	require.NoError(t, err)
	tb, err := tm.Create("beta")
	require.NoError(t, err)

	sa, err := db.TenantScope(ta.ID)
	require.NoError(t, err)
	sb, err := db.TenantScope(tb.ID)
	require.NoError(t, err)

	// Atomic multi-key write for tenant A.
	require.NoError(t, sa.Update(func(txn *TenantTxn) error {
		if err := txn.Set([]byte("x"), []byte("1")); err != nil {
			return err
		}
		return txn.Set([]byte("y"), []byte("2"))
	}))

	// Read-modify-write: increment-like update reading its own prior writes.
	require.NoError(t, sa.Update(func(txn *TenantTxn) error {
		it, err := txn.Get([]byte("x"))
		if err != nil {
			return err
		}
		var cur []byte
		require.NoError(t, it.Value(func(v []byte) error { cur = append(cur, v...); return nil }))
		require.Equal(t, []byte("1"), cur)
		return txn.Set([]byte("x"), []byte("11"))
	}))

	// View sees committed state; iterator within the txn exposes logical keys only.
	require.NoError(t, sa.View(func(txn *TenantTxn) error {
		itr, err := txn.NewIterator(DefaultIteratorOptions)
		if err != nil {
			return err
		}
		defer itr.Close()
		var keys []string
		for itr.Rewind(); itr.Valid(); itr.Next() {
			keys = append(keys, string(itr.LogicalKey()))
		}
		require.Equal(t, []string{"x", "y"}, keys)
		return nil
	}))

	// Tenant B cannot see tenant A's keys.
	require.NoError(t, sb.View(func(txn *TenantTxn) error {
		_, err := txn.Get([]byte("x"))
		require.ErrorIs(t, err, ErrKeyNotFound)
		return nil
	}))
}

// TestTenantScopeTxnRollback verifies that returning an error from Update discards all of
// its writes.
func TestTenantScopeTxnRollback(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-txn-rollback")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	ta, err := db.Tenants().Create("acme")
	require.NoError(t, err)
	s, err := db.TenantScope(ta.ID)
	require.NoError(t, err)

	sentinel := errors.New("boom")
	err = s.Update(func(txn *TenantTxn) error {
		if err := txn.Set([]byte("k"), []byte("v")); err != nil {
			return err
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)

	_, err = s.Get([]byte("k"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

// TestTenantScopeWriteBatch verifies bulk ingest via a tenant-scoped WriteBatch: keys are
// namespaced, readable back through the scope, and isolated from other tenants.
func TestTenantScopeWriteBatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-wb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	tm := db.Tenants()
	ta, err := tm.Create("acme")
	require.NoError(t, err)
	tb, err := tm.Create("beta")
	require.NoError(t, err)

	sa, err := db.TenantScope(ta.ID)
	require.NoError(t, err)
	sb, err := db.TenantScope(tb.ID)
	require.NoError(t, err)

	const n = 500
	wb := sa.NewWriteBatch()
	for i := 0; i < n; i++ {
		require.NoError(t, wb.Set([]byte(fmt.Sprintf("k%05d", i)), []byte("v")))
	}
	require.NoError(t, wb.Flush())

	// Tenant B batch writes a smaller, disjoint set.
	wb2 := sb.NewWriteBatch()
	require.NoError(t, wb2.Set([]byte("only"), []byte("beta")))
	require.NoError(t, wb2.Flush())

	// All of A's keys are present and readable through the scope.
	got, err := sa.Get([]byte("k00042"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)

	// A's iterator sees exactly n keys; B's data does not leak in.
	itr, err := sa.NewIterator(DefaultIteratorOptions)
	require.NoError(t, err)
	defer itr.Close()
	var count int
	for itr.Rewind(); itr.Valid(); itr.Next() {
		count++
	}
	require.Equal(t, n, count)

	// B sees only its single key.
	_, err = sb.Get([]byte("only"))
	require.NoError(t, err)
	_, err = sb.Get([]byte("k00042"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

// TestTenantScopeRejectedInManagedMode verifies DB.TenantScope returns
// ErrTenantScopeManaged under managed transactions (where scope ops cannot assign
// timestamps), while the registry itself remains usable.
func TestTenantScopeRejectedInManagedMode(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-scope-managed")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := OpenManaged(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	ta, err := db.Tenants().Create("acme")
	require.NoError(t, err)

	_, err = db.TenantScope(ta.ID)
	require.ErrorIs(t, err, ErrTenantScopeManaged)
}

// TestTenantBackwardCompatSuperFlag verifies the new MultiTenancy option round-trips
// through the SuperFlag mechanism like all other options.
func TestTenantBackwardCompatSuperFlag(t *testing.T) {
	def := DefaultOptions("")
	require.True(t, optionsEqual(def, Options{}.FromSuperFlag(generateSuperFlag(def))),
		"new field broke the SuperFlag default round-trip")

	on := DefaultOptions("").WithMultiTenancy(true)
	regen := Options{}.FromSuperFlag(generateSuperFlag(on))
	require.Equal(t, true, regen.MultiTenancy)
	require.Equal(t, 0, regen.NamespaceOffset)
}

// TestTenantBackwardCompatPreExistingData verifies that legacy plain-key DBs (written
// without multi-tenancy) remain fully readable, and that enabling multi-tenancy later
// does not disturb pre-existing keys (it only gates new writes by tenant registration).
func TestTenantBackwardCompatPreExistingData(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-bwcompat")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Phase 1: legacy DB, write a plain key.
	db1, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	require.NoError(t, db1.Update(func(txn *Txn) error { return txn.Set([]byte("plain:k1"), []byte("v1")) }))
	require.NoError(t, db1.Close())

	// Phase 2: reopen with multi-tenancy OFF; legacy key still readable.
	db2, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	require.Nil(t, db2.Tenants())
	var got []byte
	require.NoError(t, db2.View(func(txn *Txn) error {
		it, err := txn.Get([]byte("plain:k1"))
		if err != nil {
			return err
		}
		return it.Value(func(v []byte) error { got = append(got, v...); return nil })
	}))
	require.Equal(t, []byte("v1"), got)
	require.NoError(t, db2.Close())

	// Phase 3: reopen WITH multi-tenancy on. Reads of the legacy key are unaffected
	// (enforcement is write-side only); new tenant-scoped writes work.
	db3, err := Open(getTestOptions(dir).WithMultiTenancy(true))
	require.NoError(t, err)
	defer db3.Close()
	require.NotNil(t, db3.Tenants())

	require.NoError(t, db3.View(func(txn *Txn) error {
		it, err := txn.Get([]byte("plain:k1"))
		if err != nil {
			return err
		}
		return it.Value(func(v []byte) error { got = v; return nil })
	}))
	require.Equal(t, []byte("v1"), got)

	ta, err := db3.Tenants().Create("acme")
	require.NoError(t, err)
	scope, err := db3.TenantScope(ta.ID)
	require.NoError(t, err)
	require.NoError(t, scope.Set([]byte("k"), []byte("tenant-v")))
	it, err := scope.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("tenant-v"), it)
}

// TestTenantBackwardCompatBanNamespace ensures the pre-existing BanNamespace API still
// works unchanged when multi-tenancy is enabled.
func TestTenantBackwardCompatBanNamespace(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-banlegacy")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	ta, err := db.Tenants().Create("acme")
	require.NoError(t, err)
	require.NoError(t, db.BanNamespace(ta.ID))
	require.Contains(t, db.BannedNamespaces(), ta.ID)

	key := append(y.U64ToBytes(ta.ID), []byte("k")...)
	require.ErrorIs(t, db.Update(func(txn *Txn) error { return txn.Set(key, []byte("v")) }), ErrBannedKey)

	require.NoError(t, db.Tenants().Unban(ta.ID))
	require.NotContains(t, db.BannedNamespaces(), ta.ID)
	require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set(key, []byte("v")) }))
}

// TestTenantBackwardCompatManagedTxns ensures the internal-access txn mode used by the
// registry does not interfere with managed (external-ts) transactions.
func TestTenantBackwardCompatManagedTxns(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-managed")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := OpenManaged(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	ta, err := db.Tenants().Create("acme")
	require.NoError(t, err)
	require.Equal(t, "acme", ta.Name)

	key := append(y.U64ToBytes(ta.ID), []byte("k")...)
	txn := db.NewTransactionAt(100, true)
	require.NoError(t, txn.Set(key, []byte("mv")))
	require.NoError(t, txn.CommitAt(100, nil))

	txn2 := db.NewTransactionAt(101, false)
	it, err := txn2.Get(key)
	require.NoError(t, err)
	var v []byte
	require.NoError(t, it.Value(func(val []byte) error { v = append(v, val...); return nil }))
	require.Equal(t, []byte("mv"), v)
	txn2.Discard()
}

// TestTenantCreateConcurrentSameName verifies that concurrent Create calls for the same
// name cannot both succeed: exactly one wins, the rest get ErrTenantExists (or a
// retriable ErrConflict that is handled internally). This guards against the TOCTOU race
// where two goroutines pass the name check and both persist.
func TestTenantCreateConcurrentSameName(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-concurrent")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	tm := db.Tenants()
	const n = 16
	var wg sync.WaitGroup
	var (
		mu       sync.Mutex
		created  []*Tenant
		existErr int
		otherErr []error
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ten, err := tm.Create("shared")
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				created = append(created, ten)
			} else if errors.Is(err, ErrTenantExists) {
				existErr++
			} else {
				otherErr = append(otherErr, err)
			}
		}()
	}
	wg.Wait()

	require.Empty(t, otherErr, "unexpected non-ErrTenantExists errors")
	require.Len(t, created, 1, "exactly one Create must succeed")
	require.Equal(t, n-1, existErr, "the rest must get ErrTenantExists")

	// The single winner is retrievable by name.
	got, err := tm.GetByName("shared")
	require.NoError(t, err)
	require.Equal(t, created[0].ID, got.ID)
}

// TestTenantCreateConcurrentDistinctNames verifies that concurrent creates of distinct
// names all succeed with distinct ids and no collisions.
func TestTenantCreateConcurrentDistinctNames(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-distinct")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	tm := db.Tenants()
	const n = 16
	var wg sync.WaitGroup
	wg.Add(n)
	results := make([]*Tenant, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			results[i], errs[i] = tm.Create(string(rune('a' + i)))
		}()
	}
	wg.Wait()

	seen := make(map[uint64]struct{}, n)
	for i, ten := range results {
		require.NoError(t, errs[i])
		require.NotNil(t, ten)
		_, dup := seen[ten.ID]
		require.False(t, dup, "duplicate tenant id %d", ten.ID)
		seen[ten.ID] = struct{}{}
	}
	require.Len(t, seen, n)
}

// TestTenantManagedTxnNoTSCollision verifies the registry does not reuse a timestamp
// that a user managed transaction then writes at: after registry activity, a user
// NewTransactionAt/CommitAt at the oracle's nextTs must not collide with registry data.
func TestTenantManagedTxnNoTSCollision(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-managed-ts")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := OpenManaged(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	// Perform several registry operations (each consumes a fresh ts).
	tm := db.Tenants()
	for i := 0; i < 5; i++ {
		_, err := tm.Create(string(rune('a' + i)))
		require.NoError(t, err)
	}

	// A user managed txn must get a distinct ts and write/read cleanly. Write under a
	// registered tenant's namespace so write-path enforcement accepts it.
	ta, err := tm.Create("z")
	require.NoError(t, err)
	userKey := append(y.U64ToBytes(ta.ID), []byte("userkey")...)
	ts := db.orc.nextTs()
	txn := db.NewTransactionAt(ts, true)
	require.NoError(t, txn.Set(userKey, []byte("userval")))
	require.NoError(t, txn.CommitAt(ts, nil))

	txn2 := db.NewTransactionAt(ts+1, false)
	it, err := txn2.Get(userKey)
	require.NoError(t, err)
	var v []byte
	require.NoError(t, it.Value(func(val []byte) error { v = append(v, val...); return nil }))
	require.Equal(t, []byte("userval"), v)
	txn2.Discard()

	// Registry read still works (was not clobbered by the user write).
	got, err := tm.GetByName("a")
	require.NoError(t, err)
	require.Equal(t, "a", got.Name)
}

// TestTenantScopeGetValue verifies that Get returns an owned value copy and that the
// returned bytes are valid after the internal read transaction closes.
func TestTenantScopeGetValue(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-get-value")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	ta, err := db.Tenants().Create("acme")
	require.NoError(t, err)
	s, err := db.TenantScope(ta.ID)
	require.NoError(t, err)

	require.NoError(t, s.Set([]byte("k"), []byte("v1")))

	val, err := s.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)

	// Overwrite the key through a fresh transaction. The previously returned value must
	// remain unchanged, proving Get returned an owned copy.
	require.NoError(t, s.Set([]byte("k"), []byte("v2")))
	require.Equal(t, []byte("v1"), val)

	// Re-reading returns the new value.
	val2, err := s.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), val2)
}

// TestMultiTenancyForcesNamespaceOffsetZero verifies that enabling multi-tenancy always
// resets NamespaceOffset to 0, because the tenant-scope iterator relies on the tenant id
// being a simple byte prefix.
func TestMultiTenancyForcesNamespaceOffsetZero(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-offset")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := DefaultOptions(dir).WithNamespaceOffset(4).WithMultiTenancy(true)
	require.Equal(t, 0, opt.NamespaceOffset)

	db, err := Open(opt)
	require.NoError(t, err)
	defer db.Close()

	ta, err := db.Tenants().Create("acme")
	require.NoError(t, err)
	tb, err := db.Tenants().Create("beta")
	require.NoError(t, err)

	sa, err := db.TenantScope(ta.ID)
	require.NoError(t, err)
	sb, err := db.TenantScope(tb.ID)
	require.NoError(t, err)

	require.NoError(t, sa.Set([]byte("user:1"), []byte("alice")))
	require.NoError(t, sb.Set([]byte("user:1"), []byte("bob")))

	aval, err := sa.Get([]byte("user:1"))
	require.NoError(t, err)
	require.Equal(t, "alice", string(aval))

	bval, err := sb.Get([]byte("user:1"))
	require.NoError(t, err)
	require.Equal(t, "bob", string(bval))

	itr, err := sa.NewIterator(DefaultIteratorOptions)
	require.NoError(t, err)
	defer itr.Close()
	itr.Rewind()
	require.True(t, itr.Valid())
	require.Equal(t, []byte("user:1"), itr.LogicalKey())
	itr.Next()
	require.False(t, itr.Valid())
}

// TestMultiTenancyNonZeroOffsetRejected verifies that explicitly setting a non-zero
// NamespaceOffset after enabling multi-tenancy is rejected at Open time, because the
// tenant-scope iterator requires the tenant id to be a simple byte prefix.
func TestMultiTenancyNonZeroOffsetRejected(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-offset-reject")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := DefaultOptions(dir).WithMultiTenancy(true).WithNamespaceOffset(4)
	_, err = Open(opt)
	require.Error(t, err)
	require.Contains(t, err.Error(), "MultiTenancy requires NamespaceOffset to be 0")
}

// TestTenantScopeEmptyLogicalKey verifies that an empty logical key (physical key equal
// to exactly the 8-byte tenant id) is accepted for a registered tenant. This guards the
// isAllowedTenant boundary check (must be "<", not "<=").
func TestTenantScopeEmptyLogicalKey(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tenant-emptykey")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(tenantTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	ta, err := db.Tenants().Create("acme")
	require.NoError(t, err)
	s, err := db.TenantScope(ta.ID)
	require.NoError(t, err)

	require.NoError(t, s.Set([]byte{}, []byte("root")))
	val, err := s.Get([]byte{})
	require.NoError(t, err)
	require.Equal(t, []byte("root"), val)

	// The raw 8-byte namespace key must also pass write enforcement directly.
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set(y.U64ToBytes(ta.ID), []byte("root2"))
	}))
}
