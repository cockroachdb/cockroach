// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package instancestorage

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

const (
	tableID  = 46
	tenantID = 1337
)

func TestRowCodec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tenantID, err := roachpb.MakeTenantID(tenantID)
	require.NoError(t, err)
	codec := keys.MakeSQLCodec(tenantID)

	testutils.RunTrueAndFalse(t, "useRegionalByRow", func(t *testing.T, useRegionalByRow bool) {
		testEncoder(t, useRegionalByRow, makeRowCodec(codec, systemschema.SQLInstancesTable(), useRegionalByRow), tenantID)
	})
}

func testEncoder(t *testing.T, useRegionalByRow bool, codec rowCodec, expectedID roachpb.TenantID) {
	region := []byte{103} /* 103 is an arbitrary value */
	if !useRegionalByRow {
		// region is always enum.One if the system database is not configured
		// for multi-region.
		region = enum.One
	}

	t.Run("IndexPrefix", func(t *testing.T) {
		prefix := codec.makeIndexPrefix()

		rem, decodedID, err := keys.DecodeTenantPrefix(prefix)
		require.NoError(t, err)
		require.Equal(t, expectedID, decodedID)

		_, decodedTableID, indexID, err := keys.DecodeTableIDIndexID(rem)
		require.NoError(t, err)
		require.Equal(t, decodedTableID, uint32(tableID))

		if useRegionalByRow {
			require.Equal(t, indexID, uint32(2))
		} else {
			require.Equal(t, indexID, uint32(1))
		}
	})

	t.Run("RegionPrefix", func(t *testing.T) {
		prefix := codec.makeRegionPrefix(region)

		rem, decodedID, err := keys.DecodeTenantPrefix(prefix)
		require.NoError(t, err)
		require.Equal(t, expectedID, decodedID)

		rem, decodedTableID, indexID, err := keys.DecodeTableIDIndexID(rem)
		require.NoError(t, err)
		require.Equal(t, decodedTableID, uint32(tableID))

		if useRegionalByRow {
			require.Equal(t, indexID, uint32(2))
			_, decodedRegion, err := encoding.DecodeBytesAscending(rem, nil)
			require.Equal(t, region, decodedRegion)
			require.NoError(t, err)
		} else {
			require.Equal(t, indexID, uint32(1))
		}
	})

	t.Run("RoundTripKey", func(t *testing.T) {
		id := base.SQLInstanceID(42)
		key := codec.encodeKey(region, id)

		decodedRegion, decodedID, err := codec.decodeKey(key)

		require.NoError(t, err)
		require.Equal(t, decodedID, id)
		require.Equal(t, region, decodedRegion)
	})
}
