/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4/y"
)

// Tenant metadata lives under the reserved badgerPrefix, so isInternalKey skips it. Id
// records and the name index use disjoint sub-prefixes ("id:" and "name:") so the two key
// kinds are never distinguished by length.
var (
	tenantIDPrefix   = []byte("!badger!tenant!id:")
	tenantNamePrefix = []byte("!badger!tenant!name:")
)

// createMaxRetries bounds the number of times Create retries on a transaction conflict
// (two concurrent creates racing for the same name).
const createMaxRetries = 5

// tenantKey returns the id record key for a tenant: tenantIDPrefix + 8-byte big-endian id.
func tenantKey(id uint64) []byte {
	return append(append([]byte{}, tenantIDPrefix...), y.U64ToBytes(id)...)
}

// tenantNameKey returns the name-index key for a tenant: tenantNamePrefix + name.
func tenantNameKey(name string) []byte {
	return append(append([]byte{}, tenantNamePrefix...), []byte(name)...)
}

// tenantKeyID extracts the id from a key produced by tenantKey.
func tenantKeyID(key []byte) uint64 { return y.BytesToU64(key[len(tenantIDPrefix):]) }

// TenantManager administers tenants for a DB. Obtain it via DB.Tenants(); it is non-nil
// only when Options.MultiTenancy is enabled.
type TenantManager struct {
	db *DB

	// mu guards known and nextID. known caches registered tenant ids so the write-path
	// enforcement (DB.isAllowedTenant) can reject writes to unregistered namespaces
	// without hitting the LSM on every Set.
	mu     sync.RWMutex
	known  map[uint64]struct{}
	nextID uint64
}

func newTenantManager(db *DB) *TenantManager {
	return &TenantManager{db: db, known: make(map[uint64]struct{})}
}

// load primes the in-memory id cache and next-id counter from persisted tenant records.
// Called once during DB.Open when MultiTenancy is enabled.
func (m *TenantManager) load() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var maxID uint64
	err := m.db.View(func(txn *Txn) error {
		iopts := DefaultIteratorOptions
		iopts.Prefix = tenantIDPrefix
		iopts.PrefetchValues = false
		iopts.InternalAccess = true
		itr := txn.NewIterator(iopts)
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			key := itr.Item().Key()
			// Only id records live under tenantIDPrefix; guard length defensively.
			if len(key) != len(tenantIDPrefix)+8 {
				continue
			}
			id := tenantKeyID(key)
			m.known[id] = struct{}{}
			if id > maxID {
				maxID = id
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	m.nextID = maxID + 1
	if m.nextID == 0 {
		m.nextID = 1
	}
	return nil
}

// isKnown reports whether id is a registered tenant.
func (m *TenantManager) isKnown(id uint64) bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	_, ok := m.known[id]
	m.mu.RUnlock()
	return ok
}

// Create registers a new tenant with the given unique name and returns it. The id is
// auto-assigned (monotonic). Returns ErrTenantExists if name is already in use.
//
// Name uniqueness is enforced transactionally: the persist transaction reads the
// name-index key and writes it, relying on Badger's SSI conflict detection to reject
// concurrent creates of the same name. A short retry loop handles the rare conflict.
func (m *TenantManager) Create(name string) (*Tenant, error) {
	if m == nil || !m.db.opt.MultiTenancy {
		return nil, ErrMultiTenancyNotEnabled
	}
	if name == "" {
		return nil, errors.New("tenant name cannot be empty")
	}
	if m.db.opt.ReadOnly {
		return nil, errors.New("cannot create tenant in read-only mode")
	}

	var lastErr error
	for i := 0; i < createMaxRetries; i++ {
		// Allocate the id under mu; reserve it in known so concurrent creates get
		// distinct ids and so enforcement accepts the namespace immediately.
		m.mu.Lock()
		if m.nextID == 0 {
			m.nextID = 1
		}
		id := m.nextID
		m.known[id] = struct{}{}
		m.nextID = id + 1
		m.mu.Unlock()

		now := time.Now().UTC()
		t := &Tenant{ID: id, Name: name, CreatedAt: now, UpdatedAt: now}
		err := m.persistNew(t)
		if err == nil {
			return t, nil
		}
		// Roll back the reservation and decide whether to retry.
		m.mu.Lock()
		delete(m.known, id)
		m.mu.Unlock()
		if errors.Is(err, ErrTenantExists) {
			return nil, ErrTenantExists
		}
		if !errors.Is(err, ErrConflict) {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

// persistNew writes the tenant record and name index atomically, enforcing name
// uniqueness inside the same transaction. Returns ErrTenantExists if the name is
// already taken (read-then-write under SSI), or ErrConflict if a concurrent create
// raced and the caller should retry.
func (m *TenantManager) persistNew(t *Tenant) error {
	val, err := encodeTenant(t)
	if err != nil {
		return err
	}
	nameKey := tenantNameKey(t.Name)
	return m.dbInternalUpdate(func(txn *Txn) error {
		// Authoritative uniqueness check: a concurrent create that wrote this name
		// first will cause this Get to find it (or, under SSI, the commit will fail
		// with ErrConflict).
		if _, err := txn.Get(nameKey); err == nil {
			return ErrTenantExists
		} else if !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if err := txn.Set(tenantKey(t.ID), val); err != nil {
			return err
		}
		return txn.Set(nameKey, y.U64ToBytes(t.ID))
	})
}

// dbInternalUpdate runs fn in an internal read-write transaction whose keys may carry
// the reserved !badger! prefix. It supports both normal and managed (external-ts) DB
// modes so multi-tenancy does not break managed-txn users such as Dgraph.
func (m *TenantManager) dbInternalUpdate(fn func(txn *Txn) error) error {
	if m.db.opt.ReadOnly {
		return errors.New("cannot write tenant metadata in read-only mode")
	}
	if m.db.opt.managedTxns {
		// In managed mode timestamps are user-supplied, so the registry must consume
		// a fresh ts from the oracle's monotonic counter to avoid colliding with user
		// writes. incrementNextTs is atomic under the oracle lock.
		ts := m.db.orc.incrementNextTsAndGet()
		txn := m.db.NewTransactionAt(ts, true)
		txn.internalAccess = true
		defer txn.Discard()
		if err := fn(txn); err != nil {
			return err
		}
		return txn.CommitAt(ts, nil)
	}
	txn := m.db.newInternalTransaction(true)
	defer txn.Discard()
	if err := fn(txn); err != nil {
		return err
	}
	return txn.Commit()
}

// Get returns the tenant with the given id, or ErrTenantNotFound.
func (m *TenantManager) Get(id uint64) (*Tenant, error) {
	if m == nil || !m.db.opt.MultiTenancy {
		return nil, ErrMultiTenancyNotEnabled
	}
	if !m.isKnown(id) {
		return nil, ErrTenantNotFound
	}
	var t *Tenant
	err := m.db.View(func(txn *Txn) error {
		item, err := txn.Get(tenantKey(id))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			dec, err := decodeTenant(v)
			if err != nil {
				return err
			}
			t = dec
			return nil
		})
	})
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, ErrTenantNotFound
		}
		return nil, err
	}
	return t, nil
}

// GetByName returns the tenant with the given name, or ErrTenantNotFound.
func (m *TenantManager) GetByName(name string) (*Tenant, error) {
	if m == nil || !m.db.opt.MultiTenancy {
		return nil, ErrMultiTenancyNotEnabled
	}
	var id uint64
	err := m.db.View(func(txn *Txn) error {
		item, err := txn.Get(tenantNameKey(name))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			if len(v) != 8 {
				return fmt.Errorf("invalid tenant name index value")
			}
			id = y.BytesToU64(v)
			return nil
		})
	})
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, ErrTenantNotFound
		}
		return nil, err
	}
	return m.Get(id)
}

// List returns all registered tenants.
func (m *TenantManager) List() ([]*Tenant, error) {
	if m == nil || !m.db.opt.MultiTenancy {
		return nil, ErrMultiTenancyNotEnabled
	}
	var out []*Tenant
	err := m.db.View(func(txn *Txn) error {
		iopts := DefaultIteratorOptions
		iopts.Prefix = tenantIDPrefix
		iopts.PrefetchValues = true
		iopts.InternalAccess = true
		itr := txn.NewIterator(iopts)
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			item := itr.Item()
			// Only id records live under tenantIDPrefix; guard length defensively.
			if len(item.Key()) != len(tenantIDPrefix)+8 {
				continue
			}
			if err := item.Value(func(v []byte) error {
				t, err := decodeTenant(v)
				if err != nil {
					return err
				}
				out = append(out, t)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Ban blocks all reads/writes to a tenant's namespace by writing the banned-namespace
// key and updating the tenant record in one internal transaction. The in-memory banned
// set is updated after the txn commits so a crash leaves the durable ban as the source of
// truth (re-applied by initBannedNamespaces on restart).
func (m *TenantManager) Ban(id uint64) error {
	if m == nil || !m.db.opt.MultiTenancy {
		return ErrMultiTenancyNotEnabled
	}
	if m.db.opt.ReadOnly {
		return errors.New("cannot ban tenant in read-only mode")
	}
	t, err := m.Get(id)
	if err != nil {
		return err
	}
	t.UpdatedAt = time.Now().UTC()
	val, err := encodeTenant(t)
	if err != nil {
		return err
	}
	banKey := append(bannedNsKey, y.U64ToBytes(id)...)
	err = m.dbInternalUpdate(func(txn *Txn) error {
		if err := txn.Set(banKey, nil); err != nil {
			return err
		}
		return txn.Set(tenantKey(t.ID), val)
	})
	if err != nil {
		return err
	}
	m.db.bannedNamespaces.add(id)
	return nil
}

// Unban restores access to a previously-banned tenant's namespace. It tombstones the
// banned-namespace key and updates the tenant record in one internal transaction, then
// drops the id from the in-memory banned set. On restart initBannedNamespaces does not
// re-add it, as the iterator skips tombstoned keys (DefaultIteratorOptions.AllVersions is
// false).
func (m *TenantManager) Unban(id uint64) error {
	if m == nil || !m.db.opt.MultiTenancy {
		return ErrMultiTenancyNotEnabled
	}
	if m.db.opt.ReadOnly {
		return errors.New("cannot unban tenant in read-only mode")
	}
	t, err := m.Get(id)
	if err != nil {
		return err
	}
	t.UpdatedAt = time.Now().UTC()
	val, err := encodeTenant(t)
	if err != nil {
		return err
	}
	banKey := append(bannedNsKey, y.U64ToBytes(id)...)
	err = m.dbInternalUpdate(func(txn *Txn) error {
		// Delete the persisted banned-namespace key. The tombstone hides it from
		// initBannedNamespaces on restart.
		if err := txn.Delete(banKey); err != nil {
			return err
		}
		return txn.Set(tenantKey(t.ID), val)
	})
	if err != nil {
		return err
	}
	m.db.bannedNamespaces.remove(id)
	return nil
}

// Delete deregisters a tenant: it atomically writes the banned-namespace key (stopping
// further writes), deletes the tenant record, and deletes the name index — all in one
// internal transaction. Existing user keys are left for offline cleanup. On success the
// id is dropped from the in-memory known cache.
func (m *TenantManager) Delete(id uint64) error {
	if m == nil || !m.db.opt.MultiTenancy {
		return ErrMultiTenancyNotEnabled
	}
	if m.db.opt.ReadOnly {
		return errors.New("cannot delete tenant in read-only mode")
	}
	t, err := m.Get(id)
	if err != nil {
		return err
	}
	banKey := append(bannedNsKey, y.U64ToBytes(id)...)
	err = m.dbInternalUpdate(func(txn *Txn) error {
		// Write the banned-namespace key so the namespace stays banned even after the
		// tenant record is gone (the ban is the durable guard against further writes).
		if err := txn.Set(banKey, nil); err != nil {
			return err
		}
		if err := txn.Delete(tenantNameKey(t.Name)); err != nil {
			return err
		}
		return txn.Delete(tenantKey(id))
	})
	if err != nil {
		return err
	}
	m.db.bannedNamespaces.add(id)
	m.mu.Lock()
	delete(m.known, id)
	m.mu.Unlock()
	return nil
}

// Purge deregisters a tenant and physically removes all of its keys, reclaiming disk
// space. It bans the namespace first (so no new writes land mid-purge), drops every key
// under the tenant's 8-byte namespace prefix via DB.DropPrefix, then removes the registry
// metadata. Like Delete, the namespace is left banned to prevent resurrection.
//
// Purge is idempotent under crash: if it fails after the ban or drop, retrying it
// converges (the drop becomes a no-op once data is gone, and Delete removes the metadata).
// DropPrefix is a heavy, blocking operation (it flushes memtables and rewrites levels), so
// Purge should be used deliberately, not on a hot path.
func (m *TenantManager) Purge(id uint64) error {
	if m == nil || !m.db.opt.MultiTenancy {
		return ErrMultiTenancyNotEnabled
	}
	if m.db.opt.ReadOnly {
		return errors.New("cannot purge tenant in read-only mode")
	}
	if _, err := m.Get(id); err != nil {
		return err
	}
	// Ban before dropping so concurrent writes to the namespace are rejected while the
	// physical delete is in flight.
	if err := m.Ban(id); err != nil {
		return err
	}
	if err := m.db.DropPrefix(y.U64ToBytes(id)); err != nil {
		return err
	}
	return m.Delete(id)
}
