// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package errorutil

// TenantSQLDeprecatedWrapper is a helper to annotate uses of components that
// are in the progress of being phased out due to work towards multi-tenancy.
type TenantSQLDeprecatedWrapper struct {
	v      interface{}
	tenant bool
}

// MakeTenantSQLDeprecatedWrapper wraps an arbitrary object that is not going
// to be available in SQL tenant servers. The boolean indicates whether the
// object is part of a SQL tenant server, which informs the behavior of the
// Optional() method.
//
// Calls to Deprecated() indicate outstanding work items, i.e. dependencies that
// have yet to be removed in order to enable multi-tenancy.
//
// Calls to Optional() indicate non-essential functionality that simply will not
// be available in a SQL tenant server for the time being. Such calls may also
// point at work items that may obviate the use of the wrapper in the future.
func MakeTenantSQLDeprecatedWrapper(v interface{}, tenant bool) TenantSQLDeprecatedWrapper {
	return TenantSQLDeprecatedWrapper{v: v, tenant: tenant}
}

// Deprecated returns the unwrapped object. It takes an issue number that should
// contain a work item resulting in the removal of the Deprecated() call (i.e.
// removes the dependency on the wrapped object).
func (w TenantSQLDeprecatedWrapper) Deprecated(issueNo int) interface{} {
	return w.v
}

// Optional returns an error if the wrapper is in SQL tenant mode. This should
// be called by functionality that is selectively disabled for tenants. For
// example, crdb_internal.gossip_liveness certainly can't work on a tenant
// since Gossip is not available, and yet there is no desire to remove this
// virtual table for single-tenant deployments.
//
// A list of related issues can optionally be provided, in case there is a
// desire to make the functionality available to tenants in the future.
func (w TenantSQLDeprecatedWrapper) Optional(issueNos ...int) (interface{}, error) {
	if w.tenant {
		return nil, UnsupportedWithMultiTenancy(issueNos...)
	}
	return w.v, nil
}
