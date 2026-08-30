# Multi-Tenancy in Badger

Badger can host many logical tenants inside a single `DB` instance, keeping each tenant's
keys strongly isolated from the others. This is useful when one process serves data for
multiple customers, workspaces, or environments and you want them separated without running
a separate database per tenant.

Multi-tenancy is **off by default**, so existing databases and code are unaffected.

## Overview

Each tenant owns a unique 8-byte namespace id that Badger prefixes onto its keys. A small
registry (persisted inside the same DB) tracks each tenant's id, name, and timestamps.
Badger exposes two things once multi-tenancy is enabled:

- **`DB.Tenants()`** — a `TenantManager` for the tenant lifecycle (create, look up, list,
  ban, delete, purge).
- **`DB.TenantScope(id)`** — a `TenantScope` that transparently namespaces every key, so
  your application works with plain logical keys and never sees another tenant's data.

When multi-tenancy is enabled, any write to a namespace that is not a registered tenant is
rejected with `ErrUnknownTenant`.

## Enabling Multi-Tenancy

Enable it through options when opening the database:

```go
db, err := badger.Open(badger.DefaultOptions("/tmp/badger").WithMultiTenancy(true))
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

Enabling multi-tenancy sets `NamespaceOffset` to `0`; the tenant id occupies the first 8
bytes of every key.

## Managing Tenants

Obtain the tenant manager with `DB.Tenants()`. It returns `nil` if multi-tenancy is
disabled.

```go
tm := db.Tenants()

// Create a tenant. Names are unique; ids are assigned automatically.
acme, err := tm.Create("acme")
if err != nil {
    log.Fatal(err)
}

// Look up by id or name.
t, err := tm.Get(acme.ID)
t, err = tm.GetByName("acme")

// List every registered tenant.
tenants, err := tm.List()
```

`Create` returns `ErrTenantExists` if the name is already taken, and `Get` / `GetByName`
return `ErrTenantNotFound` for unknown tenants. Tenant records persist and are reloaded
automatically when the database is reopened.

## Reading and Writing Tenant Data

Use a `TenantScope` for all tenant data access. Keys you pass are logical keys — the tenant
id is added for you.

```go
scope, err := db.TenantScope(acme.ID)
if err != nil {
    log.Fatal(err)
}

// One-shot operations.
if err := scope.Set([]byte("user:1"), []byte("alice")); err != nil {
    log.Fatal(err)
}

val, err := scope.Get([]byte("user:1")) // val is an owned copy, safe to keep
if err != nil {
    log.Fatal(err)
}

if err := scope.Delete([]byte("user:1")); err != nil {
    log.Fatal(err)
}
```

### Iterating

`NewIterator` scopes iteration to the tenant. Use `LogicalKey` to read keys without the
namespace prefix.

```go
it, err := scope.NewIterator(badger.DefaultIteratorOptions)
if err != nil {
    log.Fatal(err)
}
defer it.Close()

for it.Rewind(); it.Valid(); it.Next() {
    fmt.Printf("%s\n", it.LogicalKey())
}
```

### Transactions

For atomic multi-key work (read-modify-write, conditional updates), use `Update` for
read-write transactions or `View` for read-only ones. The callback receives a `TenantTxn`
whose keys are namespaced.

```go
err = scope.Update(func(txn *badger.TenantTxn) error {
    if err := txn.Set([]byte("a"), []byte("1")); err != nil {
        return err
    }
    return txn.Set([]byte("b"), []byte("2"))
})
```

Returning an error from the callback rolls back all of the transaction's writes.

### Bulk Ingest

For high-throughput loading, `NewWriteBatch` returns a tenant-scoped write batch
(conflict-free blind writes batched across transactions). Call `Flush` at the end.

```go
wb := scope.NewWriteBatch()
for i := 0; i < 100000; i++ {
    if err := wb.Set(key(i), value(i)); err != nil {
        log.Fatal(err)
    }
}
if err := wb.Flush(); err != nil {
    log.Fatal(err)
}
```

## Suspending, Deleting, and Purging Tenants

```go
tm.Ban(acme.ID)    // block all reads and writes to the tenant
tm.Unban(acme.ID)  // restore access

tm.Delete(acme.ID) // deregister the tenant; its data remains for offline cleanup
tm.Purge(acme.ID)  // deregister the tenant and drop all of its data to reclaim disk space
```

`Ban` immediately rejects further access to the tenant's keys with `ErrBannedKey`.
`Delete` removes the tenant from the registry but leaves its keys on disk. `Purge` removes
the registry entry *and* physically drops every key in the tenant's namespace; it is a
heavier, blocking operation, so use it deliberately rather than on a hot path. After
`Delete` or `Purge`, the namespace stays banned so it cannot be silently resurrected.

## How Isolation Works

Isolation is enforced on the write path: with multi-tenancy enabled, writes must target a
registered tenant's namespace, otherwise they return `ErrUnknownTenant`. Reads are isolated
by using a `TenantScope`, which confines every read and iteration to the tenant's key range.
Always go through `TenantScope` (or `TenantTxn`) for tenant data — it guarantees isolation
by construction.

## Managed Mode

If you open the database with `OpenManaged`, the tenant registry (`DB.Tenants()`) is fully
supported. `DB.TenantScope` is **not** available in managed mode and returns
`ErrTenantScopeManaged`, because scope operations assign timestamps internally, which
conflicts with the user-supplied timestamps managed mode requires. In managed mode,
namespace keys yourself (the tenant id is the leading 8 bytes) using `NewTransactionAt`
while still using the registry to track tenants.

## Errors

| Error | Meaning |
| --- | --- |
| `ErrMultiTenancyNotEnabled` | A multi-tenancy API was used while the feature is disabled. |
| `ErrUnknownTenant` | A write targeted a namespace that is not a registered tenant. |
| `ErrTenantNotFound` | The requested tenant id or name does not exist. |
| `ErrTenantExists` | `Create` was called with a name that is already in use. |
| `ErrTenantScopeManaged` | `TenantScope` was requested on a managed-mode database. |
