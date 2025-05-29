// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestWitnessReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	}
	tc := testcluster.StartTestCluster(t, 3, args)
	defer tc.Stopper().Stop(ctx)

	t0 := tc.Servers[0].Clock().Now()

	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Target(1))

	{
		s3 := roachpb.StoreID(3)
		kvserver.StoreWitnessID.Store(&s3)
	}

	require.NoError(t, tc.Servers[0].DB().Put(ctx, k, "before-snap"))
	desc := tc.AddVotersOrFatal(t, k, tc.Target(2))
	require.NoError(t, tc.Servers[0].DB().Put(ctx, k, "after-snap"))
	time.Sleep(100 * time.Millisecond)

	t1 := tc.Servers[0].Clock().Now()

	for _, s := range tc.Servers {
		eng := s.Engines()[0]

		var sstFile bytes.Buffer
		opts := storage.MVCCExportOptions{
			StartKey:           storage.MVCCKey{Key: desc.StartKey.AsRawKey()},
			EndKey:             desc.EndKey.AsRawKey(),
			StartTS:            t0,
			EndTS:              t1,
			ExportAllRevisions: true,
		}
		// TODO(tbg): should dump entire replicated state here to validate equality
		// outside of user span.
		_, _, err := storage.MVCCExportToSST(ctx, s.ClusterSettings(), eng, opts, &sstFile)
		require.NoError(t, err)

		var buf redact.StringBuilder
		require.NoError(t, storageutils.ReportSSTEntries(&buf, fmt.Sprintf("s%d", s.GetFirstStoreID()), sstFile.Bytes()))
		t.Log(buf)
	}
}
