// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"math"
	"strconv"
)

// A TenantID is a unique ID associated with a tenant in a multi-tenant cluster.
// Each tenant is granted exclusive access to a portion of the keyspace and a
// collection of SQL tables in that keyspace which comprise a "logical" cluster.
//
// The type is intentionally opaque to require deliberate use.
type TenantID struct{ id uint64 }

// SystemTenantID is the ID associated with the system's internal tenant in a
// multi-tenant cluster and the only tenant in a single-tenant cluster.
//
// The system tenant differs from all other tenants in four important ways:
// 1. the system tenant's keyspace is not prefixed with a tenant specifier.
// 2. the system tenant is created by default during cluster initialization.
// 3. the system tenant is always present and can never be destroyed.
// 4. the system tenant has the ability to create and destroy other tenants.
var SystemTenantID = MakeTenantID(1)

// MinTenantID is the minimum ID of a (non-system) tenant in a multi-tenant
// cluster.
var MinTenantID = MakeTenantID(2)

// MaxTenantID is the maximum ID of a (non-system) tenant in a multi-tenant
// cluster.
var MaxTenantID = MakeTenantID(math.MaxUint64)

// MakeTenantID constructs a new TenantID from the provided uint64.
func MakeTenantID(id uint64) TenantID {
	checkValid(id)
	return TenantID{id}
}

// ToUint64 returns the TenantID as a uint64.
func (t TenantID) ToUint64() uint64 {
	checkValid(t.id)
	return t.id
}

// String implements the fmt.Stringer interface.
func (t TenantID) String() string {
	switch t {
	case TenantID{}:
		return "invalid"
	case SystemTenantID:
		return "system"
	default:
		return strconv.FormatUint(t.id, 10)
	}
}

// Protects against zero value.
func checkValid(id uint64) {
	if id == 0 {
		panic("invalid tenant ID 0")
	}
}
