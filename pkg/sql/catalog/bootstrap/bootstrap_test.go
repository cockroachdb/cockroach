// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bootstrap

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestSupportedReleases checks that the GenerateInitialValues method
// of InitialValuesOpts works for all currently-supported releases.
//
// If this test fails because a new release has come into existence that is
// not yet supported by that method, consider adding new hard-coded values
// for this release.
// These can be obtained from the test output file for the data-driven
// TestInitialValuesToString test in the corresponding release branch.
func TestSupportedReleases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	expected := make(map[roachpb.Version]struct{})
	earliest := clusterversion.ByKey(clusterversion.BinaryMinSupportedVersionKey)
	latest := clusterversion.ByKey(clusterversion.BinaryVersionKey)
	var incumbent roachpb.Version
	for _, v := range clusterversion.ListBetween(earliest, latest) {
		if v.Major != incumbent.Major || v.Minor != incumbent.Minor {
			incumbent = roachpb.Version{
				Major: v.Major,
				Minor: v.Minor,
			}
			expected[incumbent] = struct{}{}
		}
	}
	expected[latest] = struct{}{}
	actual := make(map[roachpb.Version]struct{})
	for k := range initialValuesFactoryByKey {
		actual[clusterversion.ByKey(k)] = struct{}{}
		opts := InitialValuesOpts{
			DefaultZoneConfig:       zonepb.DefaultZoneConfigRef(),
			DefaultSystemZoneConfig: zonepb.DefaultZoneConfigRef(),
			OverrideKey:             k,
			Codec:                   keys.SystemSQLCodec,
		}
		_, _, err := opts.GenerateInitialValues()
		require.NoErrorf(t, err, "error generating initial values for system codec in version %s", k)
		opts.Codec = keys.MakeSQLCodec(roachpb.TenantID{InternalValue: 123})
		_, _, err = opts.GenerateInitialValues()
		require.NoErrorf(t, err, "error generating initial values for non-system codec in version %s", k)
	}
	require.Truef(t, reflect.DeepEqual(actual, expected),
		"expected supported releases %v, actual %v\n"+
			"see comments in test definition if this message appears",
		expected, actual)
}

func TestInitialValuesToString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var tenantID uint64
			switch d.Cmd {
			case "system":
			case "tenant":
				tenantID = 12345
			default:
				t.Fatalf("unexpected command %q", d.Cmd)
			}
			var expectedHash string
			d.ScanArgs(t, "hash", &expectedHash)
			initialValues, actualHash := getAndHashInitialValuesToString(tenantID)
			if expectedHash != actualHash {
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

			return initialValues
		})
	})
}

func getAndHashInitialValuesToString(tenantID uint64) (initialValues string, hash string) {
	ms := makeMetadataSchema(tenantID)
	initialValues = InitialValuesToString(ms)
	h := sha256.Sum256([]byte(initialValues))
	hash = hex.EncodeToString(h[:])
	return initialValues, hash
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

// TestSystemDatabaseSchemaBootstrapVersionBumped serves as a reminder to bump
// systemschema.SystemDatabaseSchemaBootstrapVersion whenever a new upgrade
// creates or modifies the schema of system tables. We unfortunately cannot
// programmatically determine if an upgrade should bump the version so by
// adding a test failure when the initial values change, the programmer and
// code reviewers are reminded to manually check whether the version should
// be bumped.
func TestSystemDatabaseSchemaBootstrapVersionBumped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// If you need to update this value (i.e. failed this test), check whether
	// you need to bump systemschema.SystemDatabaseSchemaBootstrapVersion too.
	const prevSystemHash = "14095ff89cf466b8603b3c5e5b4fca5bb04cab37eddcd1702023b8151781c0a8"
	_, curSystemHash := getAndHashInitialValuesToString(0 /* tenantID */)

	if prevSystemHash != curSystemHash {
		t.Fatalf(
			`Check whether you need to bump systemschema.SystemDatabaseSchemaBootstrapVersion
and then update prevSystemHash to %q.
The current value of SystemDatabaseSchemaBootstrapVersion is %s.`,
			curSystemHash,
			systemschema.SystemDatabaseSchemaBootstrapVersion,
		)
	}
}
