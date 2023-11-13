// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slstorage

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestKeyEncoder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sqlCodec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(1337))
	codec := &rbrEncoder{sqlCodec.IndexPrefix(42, 2)}

	t.Run("Prefix", func(t *testing.T) {
		prefix := codec.indexPrefix()

		rem, tenant, err := keys.DecodeTenantPrefix(prefix)
		require.NoError(t, err)
		require.Equal(t, tenant, roachpb.MustMakeTenantID(1337))

		rem, tableID, indexID, err := keys.DecodeTableIDIndexID(rem)
		require.NoError(t, err)
		require.Equal(t, tableID, uint32(42))
		require.Len(t, rem, 0)

		require.Equal(t, indexID, uint32(2))
	})

	t.Run("RoundTrip", func(t *testing.T) {
		id, err := MakeSessionID(enum.One, uuid.MakeV4())
		require.NoError(t, err)

		key, err := codec.encode(id)
		require.NoError(t, err)
		require.True(t, bytes.HasPrefix(key, codec.indexPrefix()))

		decodedID, err := codec.decode(key)
		require.NoError(t, err)
		require.Equal(t, id, decodedID)
	})
}
