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
type TenantID uint64

const (
	// SystemTenantID is the ID associated with the system's internal tenant in
	// a multi-tenant cluster and the only tenant in a single-tenant cluster.
	//
	// The system tenant differs from all other tenants in three important ways:
	// 1. the system tenant's keyspace is not prefixed with a tenant specifier.
	// 2. the system tenant is created by default during cluster initialization.
	// 2. the system tenant had the ability to create and destroy other tenants.
	SystemTenantID TenantID = 1

	// MinTenantID is the minimum ID of the a (non-system) tenant in a
	// multi-tenant cluster.
	MinTenantID TenantID = 2

	// MaxTenantID is the maximum ID of a tenant in a multi-tenant cluster.
	MaxTenantID TenantID = math.MaxUint64
)

// String implements the fmt.Stringer interface.
func (t TenantID) String() string {
	if t == SystemTenantID {
		return "system"
	}
	return strconv.FormatUint(uint64(t), 10)
}
