// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func newCoverageTestStorage(t *testing.T) cloud.ExternalStorage {
	t.Helper()
	es := nodelocal.TestingMakeNodelocalStorage(
		t.TempDir(), cluster.MakeTestingClusterSettings(), cloudpb.ExternalStorage{},
	)
	t.Cleanup(func() { _ = es.Close() })
	return es
}

func tsAt(seconds int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: seconds * int64(time.Second)}
}

// TestCoverageRoundTrip writes a Coverage object and reads it back
// via ReadCoverage at its known path, confirming the framing and
// proto layout round-trip end-to-end.
func TestCoverageRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	es := newCoverageTestStorage(t)

	want := revlogpb.Coverage{
		EffectiveFrom: tsAt(100),
		Scope:         "database:foo",
		Spans: []roachpb.Span{
			{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
			{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
		},
	}
	require.NoError(t, revlog.WriteCoverage(ctx, es, want))

	got, err := revlog.ReadCoverage(ctx, es, revlog.CoveragePath(want.EffectiveFrom))
	require.NoError(t, err)
	require.Equal(t, want.EffectiveFrom, got.EffectiveFrom)
	require.Equal(t, want.Scope, got.Scope)
	require.Equal(t, want.Spans, got.Spans)
}

// TestCoverageAt picks the largest-HLC <= asOf entry across a
// staircase of three coverage epochs.
func TestCoverageAt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	es := newCoverageTestStorage(t)

	// Three epochs at HLC 100, 200, 300.
	for _, ts := range []hlc.Timestamp{tsAt(100), tsAt(200), tsAt(300)} {
		require.NoError(t, revlog.WriteCoverage(ctx, es, revlogpb.Coverage{
			EffectiveFrom: ts,
			Scope:         "test",
		}))
	}

	for _, tc := range []struct {
		asOf      hlc.Timestamp
		wantFound bool
		wantHLC   hlc.Timestamp
	}{
		{asOf: tsAt(50), wantFound: false}, // before everything
		{asOf: tsAt(100), wantFound: true, wantHLC: tsAt(100)},
		{asOf: tsAt(150), wantFound: true, wantHLC: tsAt(100)},
		{asOf: tsAt(200), wantFound: true, wantHLC: tsAt(200)},
		{asOf: tsAt(250), wantFound: true, wantHLC: tsAt(200)},
		{asOf: tsAt(300), wantFound: true, wantHLC: tsAt(300)},
		{asOf: tsAt(1000), wantFound: true, wantHLC: tsAt(300)}, // past the latest
	} {
		got, found, err := revlog.CoverageAt(ctx, es, tc.asOf)
		require.NoError(t, err, "asOf %s", tc.asOf)
		require.Equal(t, tc.wantFound, found, "asOf %s", tc.asOf)
		if found {
			require.Equal(t, tc.wantHLC, got.EffectiveFrom, "asOf %s", tc.asOf)
		}
	}
}

// TestCoverageAtEmpty: with no coverage written, CoverageAt
// returns (zero, false, nil) for any asOf rather than erroring.
func TestCoverageAtEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	es := newCoverageTestStorage(t)

	_, found, err := revlog.CoverageAt(ctx, es, tsAt(100))
	require.NoError(t, err)
	require.False(t, found)
}
