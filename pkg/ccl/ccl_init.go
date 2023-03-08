// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package ccl

// We import each of the CCL packages that use init hooks below, so a single
// import of this package enables building a binary with CCL features.

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/buildccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/cliccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/gssapiccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/jwtauthccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/oidcccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamingest"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamproducer"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/workloadccl"
	"github.com/cockroachdb/cockroach/pkg/server"
)

func init() {
	// Let CheckEnterpriseEnabled know that all the ccl code is linked in.
	utilccl.AllCCLCodeImported = true

	// Set up license-related hooks from OSS to CCL. The implementation of the
	// functions we bind is in utilccl, but license checks only work once
	// utilccl.AllCCLCodeImported is set, above; that's why this hookup is done in
	// this `ccl` pkg.
	base.CheckEnterpriseEnabled = utilccl.CheckEnterpriseEnabled
	base.LicenseType = utilccl.GetLicenseType
	base.UpdateMetricOnLicenseChange = utilccl.UpdateMetricOnLicenseChange
	server.ApplyTenantLicense = utilccl.ApplyTenantLicense
}

// TestingEnableEnterprise allows overriding the license check in tests.
func TestingEnableEnterprise() func() {
	return utilccl.TestingEnableEnterprise()
}

// TestingDisableEnterprise allows re-enabling the license check in tests.
func TestingDisableEnterprise() func() {
	return utilccl.TestingDisableEnterprise()
}
