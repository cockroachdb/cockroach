// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bootstrap

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestGetInitialValuesCheckForOverrides(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test normal bootstrap using current binary's initial values.
	tc1InitialValuesOpts := InitialValuesOpts{
		DefaultZoneConfig:       zonepb.DefaultZoneConfigRef(),
		DefaultSystemZoneConfig: zonepb.DefaultSystemZoneConfigRef(),
		Codec:                   keys.SystemSQLCodec,
	}
	tc1ExpectedKVs, tc1ExpectedSplits := MakeMetadataSchema(
		keys.SystemSQLCodec, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef()).GetInitialValues()

	tc1ActualKVs, tc1ActualSplits, err := tc1InitialValuesOpts.GetInitialValuesCheckForOverrides()
	require.NoError(t, err)
	require.Equal(t, tc1ExpectedKVs, tc1ActualKVs)
	require.Equal(t, tc1ExpectedSplits, tc1ActualSplits)

	// Test system tenant override.
	tc2InitialValuesOpts := InitialValuesOpts{
		DefaultZoneConfig:       zonepb.DefaultZoneConfigRef(),
		DefaultSystemZoneConfig: zonepb.DefaultSystemZoneConfigRef(),
		OverrideKey:             clusterversion.V22_2,
		Codec:                   keys.SystemSQLCodec,
	}
	fn, err := GetInitialValuesFn(tc2InitialValuesOpts.OverrideKey, SystemTenant)
	require.NoError(t, err)
	tc2ExpectedKVs, tc2ExpectedSplits, _ := fn(
		tc2InitialValuesOpts.Codec, tc2InitialValuesOpts.DefaultZoneConfig, tc2InitialValuesOpts.DefaultSystemZoneConfig,
	)
	tc2ActualKVs, tc2ActualSplits, err := tc2InitialValuesOpts.GetInitialValuesCheckForOverrides()
	require.NoError(t, err)
	require.Equal(t, tc2ExpectedKVs, tc2ActualKVs)
	require.Equal(t, tc2ExpectedSplits, tc2ActualSplits)

	// Test secondary tenant override.
	tc3InitialValuesOpts := InitialValuesOpts{
		DefaultZoneConfig:       zonepb.DefaultZoneConfigRef(),
		DefaultSystemZoneConfig: zonepb.DefaultZoneConfigRef(),
		OverrideKey:             clusterversion.V22_2,
		Codec:                   keys.MakeSQLCodec(roachpb.TenantID{InternalValue: 123}),
	}
	fn, err = GetInitialValuesFn(tc3InitialValuesOpts.OverrideKey, SecondaryTenant)
	require.NoError(t, err)
	tc3ExpectedKVs, tc3ExpectedSplits, _ := fn(
		tc3InitialValuesOpts.Codec, tc3InitialValuesOpts.DefaultZoneConfig, tc3InitialValuesOpts.DefaultSystemZoneConfig,
	)
	tc3ActualKVs, tc3ActualSplits, err := tc3InitialValuesOpts.GetInitialValuesCheckForOverrides()
	require.NoError(t, err)
	require.Equal(t, tc3ExpectedKVs, tc3ActualKVs)
	require.Equal(t, tc3ExpectedSplits, tc3ActualSplits)
}

func TestInitialValuesToString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			codec := keys.SystemSQLCodec
			switch d.Cmd {
			case "system":
				break

			case "tenant":
				const dummyTenantID = 12345
				codec = keys.MakeSQLCodec(roachpb.MustMakeTenantID(dummyTenantID))

			default:
				t.Fatalf("unexpected command %q", d.Cmd)
			}
			var expectedHash string
			d.ScanArgs(t, "hash", &expectedHash)
			ms := MakeMetadataSchema(codec, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef())
			result := InitialValuesToString(ms)
			h := sha256.Sum256([]byte(result))
			if actualHash := hex.EncodeToString(h[:]); expectedHash != actualHash {
				t.Errorf(`Unexpected hash value %s for %s.
If you're seeing this error message, this means that the bootstrapped system
schema has changed. Assuming that this is expected:
- If this occurred during development on the main branch, rewrite the expected
  test output and the hash value and move on.
- If this occurred during development of a patch for a release branch, make
  very sure that the underlying change really is expected and is backward-
  compatible and is absolutely necessary. If that's the case, then there are
  hardcoded literals in the main development branch as well as any subsequent
  release branches that need to be updated also.`, actualHash, d.Cmd)
			}
			return result
		})
	})
}

func TestRoundTripInitialValuesStringRepresentation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("system", func(t *testing.T) {
		roundTripInitialValuesStringRepresentation(t, 0 /* tenantID */)
	})
	t.Run("tenant", func(t *testing.T) {
		const dummyTenantID = 54321
		roundTripInitialValuesStringRepresentation(t, dummyTenantID)
	})
	t.Run("tenants", func(t *testing.T) {
		const dummyTenantID1, dummyTenantID2 = 54321, 12345
		require.Equal(t,
			InitialValuesToString(makeMetadataSchema(dummyTenantID1)),
			InitialValuesToString(makeMetadataSchema(dummyTenantID2)),
		)
	})
}

func roundTripInitialValuesStringRepresentation(t *testing.T, tenantID uint64) {
	ms := makeMetadataSchema(tenantID)
	expectedKVs, expectedSplits := ms.GetInitialValues()
	actualKVs, actualSplits, err := InitialValuesFromString(ms.codec, InitialValuesToString(ms))
	require.NoError(t, err)
	require.Len(t, actualKVs, len(expectedKVs))
	require.Len(t, actualSplits, len(expectedSplits))
	for i, actualKV := range actualKVs {
		expectedKV := expectedKVs[i]
		require.EqualValues(t, expectedKV, actualKV)
	}
	for i, actualSplit := range actualSplits {
		expectedSplit := expectedSplits[i]
		require.EqualValues(t, expectedSplit, actualSplit)
	}
}

func makeMetadataSchema(tenantID uint64) MetadataSchema {
	codec := keys.SystemSQLCodec
	if tenantID > 0 {
		codec = keys.MakeSQLCodec(roachpb.MustMakeTenantID(tenantID))
	}
	return MakeMetadataSchema(codec, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef())
}
