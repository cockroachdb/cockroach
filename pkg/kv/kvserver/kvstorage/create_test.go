// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCreateUninitializedReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	storage.DisableMetamorphicSimpleValueEncoding(t) // for deterministic output

	id := roachpb.FullReplicaID{RangeID: 123, ReplicaID: 3}

	runWithEngines(t, func(t *testing.T, e Engines) {
		ctx := context.Background()
		out := testMutateSep(t, "create", e, func(rw ReadWriter, w *wag.Writer) {
			require.NoError(t, CreateUninitializedReplica(
				ctx, rw.State, RaftRO(e.LogEngine()), w, 1 /* storeID */, id,
			))
		})
		echotestRequire(t, out)
	})
}
