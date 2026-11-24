// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	// The tests below will use the minimum supported version as the logical
	// version.
	logicalVersionKey := clusterversion.MinSupported
	logicalVersion := logicalVersionKey.Version()

	prev := func(v roachpb.Version) roachpb.Version {
		t.Helper()
		if v.Minor < 1 || v.Minor > 4 || v.Patch != 0 || v.Internal != 0 {
			t.Fatalf("invalid version %v", v)
		}
		if v.Minor > 1 {
			v.Minor--
		} else {
			v.Major--
			v.Minor = 2
		}
		return v
	}
	minusOne := prev(logicalVersion)
	minusTwo := prev(minusOne)

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
			storageBinaryVersion:             logicalVersion,
			storageBinaryMinSupportedVersion: minusOne,
			tenantBinaryVersion:              logicalVersion,
			tenantBinaryMinSupportedVersion:  logicalVersion,
			TenantLogicalVersionKey:          logicalVersionKey,
			expErrMatch:                      "",
		},
		// Second test case ensures that a tenant server is prevented from starting if
		// its binary version is too low for the current tenant logical version.
		{
			storageBinaryVersion:             logicalVersion,
			storageBinaryMinSupportedVersion: minusOne,
			tenantBinaryVersion:              minusOne,
			tenantBinaryMinSupportedVersion:  minusTwo,
			TenantLogicalVersionKey:          logicalVersionKey,
			expErrMatch: fmt.Sprintf("preventing SQL server from starting because its binary version is too low for the tenant active version: "+
				"server binary version = %v, tenant active version = %v", minusOne, logicalVersion),
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

			s := serverutils.StartServerOnly(t, base.TestServerArgs{
				DefaultTestTenant: base.TestControlsTenantsExplicitly,
				Settings:          storageSettings,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						ClusterVersionOverride:         test.storageBinaryVersion,
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
			_, err := s.TenantController().StartTenant(context.Background(),
				base.TestTenantArgs{
					Settings: tenantSettings,
					TenantID: serverutils.TestTenantID(),
					TestingKnobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							ClusterVersionOverride:         test.tenantBinaryVersion,
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
