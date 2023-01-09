// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package serverccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

// TestServerStartupGuardrails ensures that a SQL server will fail to start if its binary
// version (TBV) is less than the tenant's logical version (TLV). The following will be used
// to reference the different versions hereafter:
//
//	SBV: Storage Binary Version
//	SLV: Storage Logical Version
//	TBV: Tenant Binary Version
//	TLV: Tenant Logical Version
func TestServerStartupGuardrails(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := func(major, minor int32) roachpb.Version {
		return roachpb.Version{Major: major, Minor: minor}
	}

	tests := []struct {
		storageBinaryVersion                        roachpb.Version
		storageBinaryMinSupportedVersion            roachpb.Version
		tenantSecondServerBinaryVersion             roachpb.Version
		tenantSecondServerBinaryMinSupportedVersion roachpb.Version
		expErrMatch                                 string // empty if expecting a nil error
	}{
		// First test case ensures that a tenant server can start if the server binary version is
		// not too low for the tenant logical version.
		{
			storageBinaryVersion:                        clusterversion.ByKey(clusterversion.BinaryVersionKey),
			storageBinaryMinSupportedVersion:            v(21+clusterversion.DevOffset, 1),
			tenantSecondServerBinaryVersion:             clusterversion.ByKey(clusterversion.BinaryVersionKey),
			tenantSecondServerBinaryMinSupportedVersion: v(21+clusterversion.DevOffset, 1),
			expErrMatch: "",
		},
		// Second test case ensures that a tenant server is prevented from starting if its binary
		// version is too low for the current tenant logical version.
		{
			storageBinaryVersion:                        clusterversion.ByKey(clusterversion.BinaryVersionKey),
			storageBinaryMinSupportedVersion:            v(21+clusterversion.DevOffset, 1),
			tenantSecondServerBinaryVersion:             v(21+clusterversion.DevOffset, 1),
			tenantSecondServerBinaryMinSupportedVersion: v(20+clusterversion.DevOffset, 2),
			expErrMatch: fmt.Sprintf("preventing SQL server from starting because its binary version is too low for the tenant active version: "+
				"server binary version = %v, tenant active version = %v",
				v(21+clusterversion.DevOffset, 1).PrettyPrint(), clusterversion.ByKey(clusterversion.BinaryVersionKey).PrettyPrint()),
		},
	}

	for i, test := range tests {
		storageSettings := cluster.MakeTestingClusterSettingsWithVersions(
			test.storageBinaryVersion,
			test.storageBinaryMinSupportedVersion,
			false, /* initializeVersion */
		)

		// The active version of this server will be equal to its binary version. We ensure this is
		// true with an assertion below. This is needed because in some test cases we want to ensure
		// the active version of this server is greater than the binary version of the tenant. By knowing
		// that the Storage Binary Version is higher than the Tenant Binary Version and the Storage Logical
		// Version is equal to the Storage Binary Version we can be sure that the Storage Logical Version
		// is higher than the Tenant Binary Version.
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
			// Disable the default test tenant, since we create one explicitly
			// below.
			DisableDefaultTestTenant: true,
			Settings:                 storageSettings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
				},
			},
		})

		// Ensure that the Storage Logical Version is equal to the Storage Binary Version.
		assert.True(t, s.ClusterSettings().Version.ActiveVersion(context.Background()).Version == test.storageBinaryVersion,
			"invalid test state: SLV not equal to SBV")

		tenantID := serverutils.TestTenantID()
		// The TLV will be equal to the SBV because in this case we already confirmed that the SBV
		// is equal to the SLV. Note that if the SLV is less than the SBV, the TLV will be equal
		// to the storage minimum supported version. The storage minimum supported version referenced
		// here depends on this binary actual minimum supported version as set in `sql.tenantCreationMinSupportedVersionKey`
		// and is not the override value we use above. The override value for the storage minimum supported version
		// is only passed to let this test possible with versions lower than the actual min supported version.
		tenantFirstServer, err := s.StartTenant(context.Background(),
			base.TestTenantArgs{
				Settings: cluster.MakeTestingClusterSettings(),
				TenantID: tenantID,
				TestingKnobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						DisableAutomaticVersionUpgrade: make(chan struct{}),
					},
				},
			})
		if err != nil {
			t.Fatal(err)
		}

		// Attempt to start a second server with version overrides.
		tenantSecondServerSettings := cluster.MakeTestingClusterSettingsWithVersions(
			test.tenantSecondServerBinaryVersion,
			test.tenantSecondServerBinaryMinSupportedVersion,
			true, /* initializeVersion */
		)
		tenantSecondServer, err := s.StartTenant(context.Background(),
			base.TestTenantArgs{
				Settings: tenantSecondServerSettings,
				TenantID: tenantID,
				TestingKnobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						DisableAutomaticVersionUpgrade: make(chan struct{}),
					},
				},
			})

		if !testutils.IsError(err, test.expErrMatch) {
			t.Fatalf("test %d: got error %s, wanted error matching '%s'", i, err, test.expErrMatch)
		}

		tenantFirstServer.Stopper().Stop(context.Background())
		// Only attempt to stop the second server if it was started successfully.
		if err == nil {
			tenantSecondServer.Stopper().Stop(context.Background())
		}
		s.Stopper().Stop(context.Background())
	}
}
