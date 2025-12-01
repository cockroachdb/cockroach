// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfiller

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func TestSSTManifestTenantPrefixConversion(t *testing.T) {
	tenantCodec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(5))
	start := tenantCodec.IndexPrefix(42, 7)
	end := start.PrefixEnd()
	rowSample := append(roachpb.Key(nil), start...)
	rowSample = append(rowSample, []byte("sample")...)
	ts := hlc.Timestamp{WallTime: 1, Logical: 2}
	input := []jobspb.IndexBackfillSSTManifest{{
		URI:            "nodelocal://1/.index-backfill/42/1.sst",
		Span:           &roachpb.Span{Key: start, EndKey: end},
		FileSize:       1234,
		RowSample:      rowSample,
		WriteTimestamp: &ts,
	}}

	stripped, err := stripTenantPrefixFromSSTManifests(tenantCodec, input)
	require.NoError(t, err)
	require.Len(t, stripped, 1)
	require.False(t, bytes.HasPrefix(stripped[0].Span.Key, tenantCodec.TenantPrefix()))
	require.False(t, bytes.HasPrefix(stripped[0].Span.EndKey, tenantCodec.TenantPrefix()))
	require.False(t, bytes.HasPrefix(stripped[0].RowSample, tenantCodec.TenantPrefix()))

	withPrefix := addTenantPrefixToSSTManifests(tenantCodec, stripped)
	require.Equal(t, input, withPrefix)
}
