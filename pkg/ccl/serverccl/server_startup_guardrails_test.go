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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

// TestServerStartupGuardrails ensures that a SQL server will fail to start if its binary
// version (TBV) is less than the tenant's logical version (TLV).
func TestServerStartupGuardrails(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := func(major, minor int32) roachpb.Version {
		return roachpb.Version{Major: major, Minor: minor}
	}

	var tests = []struct {
		storageBinaryVersion             roachpb.Version
		storageBinaryMinSupportedVersion roachpb.Version
		tenantBinaryVersion              roachpb.Version
		tenantBinaryMinSupportedVersion  roachpb.Version
		expErrMatch                      string // empty if expecting a nil error
	}{
		{
			storageBinaryVersion:             v(21, 2),
			storageBinaryMinSupportedVersion: v(21, 1),
			tenantBinaryVersion:              v(21, 2),
			tenantBinaryMinSupportedVersion:  v(21, 1),
			expErrMatch:                      "",
		},
		{
			storageBinaryVersion:             v(21, 2),
			storageBinaryMinSupportedVersion: v(20, 2),
			tenantBinaryVersion:              v(21, 1),
			tenantBinaryMinSupportedVersion:  v(20, 2),
			expErrMatch: "preventing SQL server from starting because its binary version is too low for the tenant active version: " +
				"server binary version = 21.1, tenant active version = 21.2",
		},
	}

	for i, test := range tests {
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
					DisableAutomaticVersionUpgrade: make(chan struct{}),
				},
			},
		})

		// Ensure that SLV is equal to SBV.
		assert.True(t, s.ClusterSettings().Version.ActiveVersion(context.Background()).Version == test.storageBinaryVersion,
			"invalid test state: SLV not equal to SBV")

		tenantSettings := cluster.MakeTestingClusterSettingsWithVersions(
			test.tenantBinaryVersion,
			test.tenantBinaryMinSupportedVersion,
			true, /* initializeVersion */
		)

		tenantServer, err := s.StartTenant(context.Background(),
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

		// Only attempt to stop the tenant if it was started successfully.
		if err == nil {
			tenantServer.Stopper().Stop(context.Background())
		}
		s.Stopper().Stop(context.Background())
	}
}
