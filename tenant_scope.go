/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"errors"

	"github.com/dgraph-io/badger/v4/y"
)

// TenantScope is a tenant-bound view of a DB. It injects the tenant's 8-byte id into
// keys at Options.NamespaceOffset so callers use plain logical keys, and scopes reads
// (Get/Iterator) to the tenant's key range so tenants cannot observe each other's data.
// Obtain one via DB.TenantScope(id).
type TenantScope struct {
	db     *DB
	id     uint64
	offset int
	// idBytes caches the 8-byte big-endian id to avoid re-encoding on every operation.
	idBytes [8]byte
	// prefix is the physical byte prefix identifying this tenant's keys; used to scope
	// iterators. For NamespaceOffset==0 it is just the 8-byte id.
	prefix []byte
}

func newTenantScope(db *DB, id uint64) *TenantScope {
	s := &TenantScope{db: db, id: id, offset: db.opt.NamespaceOffset}
	copy(s.idBytes[:], y.U64ToBytes(id))
	s.prefix = make([]byte, s.offset+8)
	copy(s.prefix[s.offset:], s.idBytes[:])
	return s
}

// makeKey inserts the tenant id at the configured offset, producing the full DB key.
// Bytes [0:offset] of the user key are a caller-chosen leading prefix (passed through),
// and bytes [offset:] follow the 8-byte id.
func (s *TenantScope) makeKey(userKey []byte) ([]byte, error) {
	if len(userKey) < s.offset {
		return nil, errors.New("tenant scope key is shorter than NamespaceOffset")
	}
	out := make([]byte, len(userKey)+8)
	copy(out, userKey[:s.offset])
	copy(out[s.offset:], s.idBytes[:])
	copy(out[s.offset+8:], userKey[s.offset:])
	return out, nil
}

// stripPrefix removes the tenant prefix (bytes [0:offset+8]) from a physical key,
// returning the logical key as the caller passed it in.
func (s *TenantScope) stripPrefix(dbKey []byte) []byte {
	if len(dbKey) < s.offset+8 {
		return dbKey
	}
	out := make([]byte, len(dbKey)-8)
	copy(out, dbKey[:s.offset])
	copy(out[s.offset:], dbKey[s.offset+8:])
	return out
}

// ID returns the tenant id this scope is bound to.
func (s *TenantScope) ID() uint64 { return s.id }

// Set stores a key-value pair scoped to this tenant.
func (s *TenantScope) Set(key, val []byte) error {
	full, err := s.makeKey(key)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *Txn) error { return txn.Set(full, val) })
}

// Delete deletes a key scoped to this tenant.
func (s *TenantScope) Delete(key []byte) error {
	full, err := s.makeKey(key)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *Txn) error { return txn.Delete(full) })
}

// Get fetches a key scoped to this tenant and returns an owned copy of the value.
// The value is copied inside the read transaction so it remains valid after the
// transaction closes.
func (s *TenantScope) Get(key []byte) ([]byte, error) {
	full, err := s.makeKey(key)
	if err != nil {
		return nil, err
	}
	var val []byte
	err = s.db.View(func(txn *Txn) error {
		it, err := txn.Get(full)
		if err != nil {
			return err
		}
		return it.Value(func(v []byte) error {
			val = append([]byte(nil), v...)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}

// scopedIterOpts rewrites IteratorOptions.Prefix from a *logical* prefix (without the
// tenant id) to the physical prefix. An empty prefix means the whole tenant range.
func (s *TenantScope) scopedIterOpts(opts IteratorOptions) (IteratorOptions, error) {
	if len(opts.Prefix) == 0 {
		opts.Prefix = s.prefix
		return opts, nil
	}
	full, err := s.makeKey(opts.Prefix)
	if err != nil {
		return opts, err
	}
	opts.Prefix = full
	return opts, nil
}

// NewIterator returns an iterator scoped to this tenant's key range; it owns its
// underlying transaction, which Close releases. IteratorOptions.Prefix is interpreted
// as a *logical* prefix (without the tenant id) and rewritten to the physical prefix;
// empty means the whole tenant range.
func (s *TenantScope) NewIterator(opts IteratorOptions) (*TenantIterator, error) {
	opts, err := s.scopedIterOpts(opts)
	if err != nil {
		return nil, err
	}
	txn := s.db.NewTransaction(false)
	return &TenantIterator{Iterator: txn.NewIterator(opts), txn: txn, scope: s}, nil
}

// Update runs fn inside a read-write transaction scoped to this tenant. All key access
// through the TenantTxn is automatically namespaced, giving end users atomic multi-key
// operations (read-modify-write, conditional writes) with tenant isolation. The
// transaction commits when fn returns nil and is discarded on error.
func (s *TenantScope) Update(fn func(txn *TenantTxn) error) error {
	return s.db.Update(func(txn *Txn) error {
		return fn(&TenantTxn{txn: txn, scope: s})
	})
}

// View runs fn inside a read-only transaction scoped to this tenant.
func (s *TenantScope) View(fn func(txn *TenantTxn) error) error {
	return s.db.View(func(txn *Txn) error {
		return fn(&TenantTxn{txn: txn, scope: s})
	})
}

// TenantTxn is a tenant-scoped transaction handed to TenantScope.Update / View. Every key
// passed to its methods is a plain logical key; the tenant id is injected transparently.
// The transaction's lifetime is bounded by the enclosing Update/View callback.
type TenantTxn struct {
	txn   *Txn
	scope *TenantScope
}

// Set stores a key-value pair within the scoped transaction.
func (tt *TenantTxn) Set(key, val []byte) error {
	full, err := tt.scope.makeKey(key)
	if err != nil {
		return err
	}
	return tt.txn.Set(full, val)
}

// Delete removes a key within the scoped transaction.
func (tt *TenantTxn) Delete(key []byte) error {
	full, err := tt.scope.makeKey(key)
	if err != nil {
		return err
	}
	return tt.txn.Delete(full)
}

// Get fetches an item within the scoped transaction. The returned *Item is only valid for
// the lifetime of the enclosing Update/View callback; read its value via Item.Value inside
// the callback.
func (tt *TenantTxn) Get(key []byte) (*Item, error) {
	full, err := tt.scope.makeKey(key)
	if err != nil {
		return nil, err
	}
	return tt.txn.Get(full)
}

// NewIterator returns an iterator scoped to this tenant that shares the transaction; its
// Close releases only the iterator, not the transaction (owned by Update/View). Use
// TenantIterator.LogicalKey to read keys with the tenant prefix stripped.
func (tt *TenantTxn) NewIterator(opts IteratorOptions) (*TenantIterator, error) {
	opts, err := tt.scope.scopedIterOpts(opts)
	if err != nil {
		return nil, err
	}
	return &TenantIterator{Iterator: tt.txn.NewIterator(opts), txn: nil, scope: tt.scope}, nil
}

// NewWriteBatch returns a tenant-scoped WriteBatch for high-throughput bulk ingest. It
// wraps DB.NewWriteBatch and namespaces every key, giving conflict-free blind writes
// batched tightly across transactions. Call Flush at the end (or Cancel on abort).
func (s *TenantScope) NewWriteBatch() *TenantWriteBatch {
	return &TenantWriteBatch{wb: s.db.NewWriteBatch(), scope: s}
}

// TenantWriteBatch is a tenant-scoped WriteBatch. Every key is namespaced to the tenant
// transparently; otherwise it mirrors WriteBatch semantics (blind writes, no conflicts).
type TenantWriteBatch struct {
	wb    *WriteBatch
	scope *TenantScope
}

// Set queues a namespaced key-value write.
func (twb *TenantWriteBatch) Set(key, val []byte) error {
	full, err := twb.scope.makeKey(key)
	if err != nil {
		return err
	}
	return twb.wb.Set(full, val)
}

// Delete queues a namespaced delete.
func (twb *TenantWriteBatch) Delete(key []byte) error {
	full, err := twb.scope.makeKey(key)
	if err != nil {
		return err
	}
	return twb.wb.Delete(full)
}

// Flush commits any pending writes. It must be called to durably persist the batch.
func (twb *TenantWriteBatch) Flush() error { return twb.wb.Flush() }

// Cancel abandons the batch. Already-committed writes still persist.
func (twb *TenantWriteBatch) Cancel() { twb.wb.Cancel() }

// Error returns any error encountered so far.
func (twb *TenantWriteBatch) Error() error { return twb.wb.Error() }

// SetMaxPendingTxns bounds the number of in-flight batch transactions.
func (twb *TenantWriteBatch) SetMaxPendingTxns(max int) { twb.wb.SetMaxPendingTxns(max) }

// TenantIterator wraps an *Iterator with a tenant-scoped view and owns its transaction.
// Because the underlying *Item is reused by the iterator (it embeds a sync.WaitGroup and
// must not be copied), callers should use LogicalKey() rather than Item().Key().
type TenantIterator struct {
	*Iterator
	txn   *Txn
	scope *TenantScope
}

// LogicalKey returns the current item's key with the tenant prefix stripped.
func (ti *TenantIterator) LogicalKey() []byte {
	return ti.scope.stripPrefix(ti.Iterator.Item().Key())
}

// Close releases the iterator. If the iterator owns its transaction (created via
// TenantScope.NewIterator) that transaction is discarded too; iterators created from a
// TenantTxn share the caller's transaction and leave it untouched.
func (ti *TenantIterator) Close() {
	ti.Iterator.Close()
	if ti.txn != nil {
		ti.txn.Discard()
	}
}
