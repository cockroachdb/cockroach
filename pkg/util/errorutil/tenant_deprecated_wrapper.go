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
// It is usually usually used under a layer of abstraction that is aware of
// the wrapped object's type.
//
// Deprecated objects are broadly objects that reach deeply into the KV layer
// and which will be inaccessible from a SQL tenant server. Their uses in SQL
// fall into two categories:
//
// - functionality essential for multi-tenancy, i.e. a use which will
//   have to be removed before we can start SQL tenant servers.
// - non-essential functionality, which will be disabled when run in
//   a SQL tenant server. It may or may not be a long-term goal to remove
//   this usage; this is determined on a case-by-case basis.
//
// As work towards multi-tenancy is taking place, semi-dedicated SQL tenant
// servers are supported. These are essentially SQL tenant servers that get
// to reach into the KV layer as needed while the first category above is
// being whittled down.
//
// This wrapper aids that process by offering two methods corresponding to
// the categories above:
//
// Deprecated() trades in a reference to Github issue (tracking the removal of
// an essential usage) for the wrapped object; OptionalErr() returns the wrapped
// object only if the wrapper was set up to allow this.
//
// Note that the wrapped object will in fact always have to be present as long
// as calls to Deprecated() exist. However, when running semi-dedicated SQL
// tenants, the wrapper should be set up with exposed=false so that it can
// pretend that the object is in fact not available.
//
// Finally, once all Deprecated() calls have been removed, it is possible to
// treat the wrapper as a pure option type, i.e. wrap a nil value with
// exposed=false.
type TenantSQLDeprecatedWrapper struct {
	v       interface{}
	exposed bool
}

// MakeTenantSQLDeprecatedWrapper wraps an arbitrary object. When the 'exposed'
// parameter is set to true, Optional() will return the object.
func MakeTenantSQLDeprecatedWrapper(v interface{}, exposed bool) TenantSQLDeprecatedWrapper {
	return TenantSQLDeprecatedWrapper{v: v, exposed: exposed}
}

// Optional returns the wrapped object if it is available (meaning that the
// wrapper was set up to make it available). This should be called by
// functionality that relies on the wrapped object but can be disabled when this
// is desired.
//
// Optional functionality should be used sparingly as it increases the
// maintenance and testing burden. It is preferable to use OptionalErr()
// (and return the error) where possible.
func (w TenantSQLDeprecatedWrapper) Optional() (interface{}, bool) {
	if !w.exposed {
		return nil, false
	}
	return w.v, true
}

// OptionalErr calls Optional and returns an error (referring to the optionally
// supplied Github issues) if the wrapped object is not available.
func (w TenantSQLDeprecatedWrapper) OptionalErr(issue int) (interface{}, error) {
	v, ok := w.Optional()
	if !ok {
		return nil, UnsupportedWithMultiTenancy(issue)
	}
	return v, nil
}
