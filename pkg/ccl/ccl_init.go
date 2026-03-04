// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ccl

// We import each of the CCL packages that use init hooks below, so a single
// import of this package enables building a binary with CCL features.

import (
	_ "github.com/cockroachdb/cockroach/pkg/backup"
	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/gssapiccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/jwtauthccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/ldapccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/oidcccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/workloadccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/workloadccl/cliccl" // registers fixtures command
	// TODO(ssd): Many test packages require this implicitly but have failed to
	// import it, instead depending on the implicit dependency tree of pkg/ccl to
	// include it.
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"
	_ "github.com/cockroachdb/cockroach/pkg/crosscluster/logical"
	_ "github.com/cockroachdb/cockroach/pkg/crosscluster/physical"
	_ "github.com/cockroachdb/cockroach/pkg/crosscluster/producer"
	"github.com/cockroachdb/cockroach/pkg/server/license"
)

func init() {
	// Set up license-related hooks from OSS to CCL code.
	base.LicenseType = license.GetLicenseType
	base.GetLicenseTTL = license.GetLicenseTTL
}

// TestingEnableEnterprise allows overriding the license check in tests.
func TestingEnableEnterprise() func() {
	return utilccl.TestingEnableEnterprise()
}

// TestingDisableEnterprise allows re-enabling the license check in tests.
func TestingDisableEnterprise() func() {
	return utilccl.TestingDisableEnterprise()
}
