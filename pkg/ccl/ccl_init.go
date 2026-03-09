// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ccl

// We import each of the CCL packages that use init hooks below, so a single
// import of this package enables building a binary with CCL features.

import (
	_ "github.com/cockroachdb/cockroach/pkg/backup"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/gssapiccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/workloadccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/workloadccl/cliccl" // registers fixtures command
	// TODO(ssd): Many test packages require this implicitly but have failed to
	// import it, instead depending on the implicit dependency tree of pkg/ccl to
	// include it.
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"
	_ "github.com/cockroachdb/cockroach/pkg/crosscluster/logical"
	_ "github.com/cockroachdb/cockroach/pkg/crosscluster/physical"
	_ "github.com/cockroachdb/cockroach/pkg/crosscluster/producer"
	_ "github.com/cockroachdb/cockroach/pkg/security/jwtauth"
	_ "github.com/cockroachdb/cockroach/pkg/security/ldapauth"
	_ "github.com/cockroachdb/cockroach/pkg/security/oidcauth"
)

// TestingEnableEnterprise is a no-op. It was deprecated when the core license
// was removed. We no longer distinguish between features enabled only for
// enterprise. All features are enabled, and if a license policy is violated,
// we throttle connections. Callers can safely remove any reference to this
// function.
//
// Deprecated
func TestingEnableEnterprise() func() {
	return func() {}
}

// TestingDisableEnterprise is a no-op. See TestingEnableEnterprise.
//
// Deprecated
func TestingDisableEnterprise() func() {
	return func() {}
}
