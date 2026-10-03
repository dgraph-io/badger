/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"encoding/json"
	"time"
)

// Tenant is a logical tenant sharing a single Badger DB instance. Each tenant owns a
// unique 8-byte namespace id embedded in its user keys at Options.NamespaceOffset, so
// tenants' keyspaces are physically disjoint.
type Tenant struct {
	ID        uint64    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func encodeTenant(t *Tenant) ([]byte, error) { return json.Marshal(t) }

func decodeTenant(b []byte) (*Tenant, error) {
	var t Tenant
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, err
	}
	return &t, nil
}
