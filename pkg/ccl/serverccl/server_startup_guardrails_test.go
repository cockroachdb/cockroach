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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestServerStartupGuardrails ensures that a SQL server will fail to start if
// its binary version (TBV) is less than the tenant's logical version (TLV).
func TestServerStartupGuardrails(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We need to conditionally apply the DevOffset for the version
	// returned by this function to work both on master (where the dev
	// offset applies) and on release branches (where it doesn't).
	v := func(major, minor int32) roachpb.Version {
		binaryVersion := clusterversion.ByKey(clusterversion.BinaryVersionKey)
		var offset int32
		if binaryVersion.Major > clusterversion.DevOffset {
			offset = clusterversion.DevOffset
		}
		return roachpb.Version{Major: offset + major, Minor: minor}
	}

	tests := []struct {
		storageBinaryVersion             roachpb.Version
		storageBinaryMinSupportedVersion roachpb.Version
		tenantBinaryVersion              roachpb.Version
		tenantBinaryMinSupportedVersion  roachpb.Version
		TenantLogicalVersionKey          clusterversion.Key
		expErrMatch                      string // empty if expecting a nil error
	}{
		// First test case ensures that a tenant server can start if the server binary
		// version is not too low for the tenant logical version.
		{
			storageBinaryVersion:             v(22, 2),
			storageBinaryMinSupportedVersion: v(22, 1),
			tenantBinaryVersion:              v(22, 2),
			tenantBinaryMinSupportedVersion:  v(22, 2),
			TenantLogicalVersionKey:          clusterversion.V22_2,
			expErrMatch:                      "",
		},
		// Second test case ensures that a tenant server is prevented from starting if
		// its binary version is too low for the current tenant logical version.
		{
			storageBinaryVersion:             v(22, 2),
			storageBinaryMinSupportedVersion: v(22, 1),
			tenantBinaryVersion:              v(22, 1),
			tenantBinaryMinSupportedVersion:  v(21, 2),
			TenantLogicalVersionKey:          clusterversion.V22_2,
			expErrMatch: fmt.Sprintf("preventing SQL server from starting because its binary version is too low for the tenant active version: "+
				"server binary version = %v, tenant active version = %v", v(22, 1), v(22, 2)),
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			defer log.Scope(t).Close(t)

			storageSettings := cluster.MakeTestingClusterSettingsWithVersions(
				test.storageBinaryVersion,
				test.storageBinaryMinSupportedVersion,
				false, /* initializeVersion */
			)

			s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
				// Disable the default test tenant, since we create one explicitly
				// below.
				DisableDefaultTestTenant: true,
				Settings:                 storageSettings,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						BinaryVersionOverride:          test.storageBinaryVersion,
						BootstrapVersionKeyOverride:    clusterversion.V22_2,
						DisableAutomaticVersionUpgrade: make(chan struct{}),
					},
					SQLEvalContext: &eval.TestingKnobs{
						TenantLogicalVersionKeyOverride: test.TenantLogicalVersionKey,
					},
				},
			})
			defer s.Stopper().Stop(context.Background())

			tenantSettings := cluster.MakeTestingClusterSettingsWithVersions(
				test.tenantBinaryVersion,
				test.tenantBinaryMinSupportedVersion,
				true, /* initializeVersion */
			)

			// The tenant will be created with an active version equal to the version
			// corresponding to TenantLogicalVersionKey. Tenant creation is expected
			// to succeed for all test cases but server creation is expected to succeed
			// only if tenantBinaryVersion is at least equal to the version corresponding
			// to TenantLogicalVersionKey.
			_, err := s.StartTenant(context.Background(),
				base.TestTenantArgs{
					Settings: tenantSettings,
					TenantID: serverutils.TestTenantID(),
					TestingKnobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							BinaryVersionOverride:          test.tenantBinaryVersion,
							DisableAutomaticVersionUpgrade: make(chan struct{}),
						},
					},
				})

			if !testutils.IsError(err, test.expErrMatch) {
				t.Fatalf("test %d: got error %s, wanted error matching '%s'", i, err, test.expErrMatch)
			}
		})
	}
}
