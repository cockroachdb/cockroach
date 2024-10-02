// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvnemesis

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

var (
	k1  = tk(1)
	k2  = tk(2)
	k3  = tk(3)
	k4  = tk(4)
	k5  = tk(5)
	k6  = tk(6)
	k7  = tk(7)
	k8  = tk(8)
	k9  = tk(9)
	k10 = tk(10)
	k11 = tk(11)
)

func TestOperationsFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var sstValueHeader enginepb.MVCCValueHeader
	sstValueHeader.KVNemesisSeq.Set(1)
	sstSpan := roachpb.Span{Key: roachpb.Key(k1), EndKey: roachpb.Key(k4)}
	sstTS := hlc.Timestamp{WallTime: 1}
	sstFile := &storage.MemObject{}
	{
		st := cluster.MakeTestingClusterSettings()
		w := storage.MakeIngestionSSTWriter(ctx, st, sstFile)
		defer w.Close()

		require.NoError(t, w.PutMVCC(storage.MVCCKey{Key: roachpb.Key(k1), Timestamp: sstTS},
			storage.MVCCValue{MVCCValueHeader: sstValueHeader, Value: roachpb.MakeValueFromString("v1")}))
		require.NoError(t, w.PutMVCC(storage.MVCCKey{Key: roachpb.Key(k2), Timestamp: sstTS},
			storage.MVCCValue{MVCCValueHeader: sstValueHeader}))
		require.NoError(t, w.PutMVCCRangeKey(
			storage.MVCCRangeKey{StartKey: roachpb.Key(k3), EndKey: roachpb.Key(k4), Timestamp: sstTS},
			storage.MVCCValue{MVCCValueHeader: sstValueHeader}))
		require.NoError(t, w.Finish())
	}

	tests := []struct {
		step Step
	}{
		{step: step(get(k1))},
		{step: step(del(k1, 1))},
		{step: step(batch(get(k2), reverseScanForUpdate(k3, k5), get(k6)))},
		{
			step: step(
				closureTxn(ClosureTxnType_Commit,
					isolation.Serializable,
					batch(get(k7), get(k8), del(k9, 1)),
					delRange(k10, k11, 2),
					put(k11, 3),
				)),
		},
		{
			step: step(addSSTable(sstFile.Data(), sstSpan, sstTS, sstValueHeader.KVNemesisSeq.Get(), true)),
		},
		{
			step: step(
				closureTxn(ClosureTxnType_Commit,
					isolation.Serializable,
					createSavepoint(0), put(k9, 3), releaseSavepoint(0),
					get(k8),
					createSavepoint(4), del(k9, 1), rollbackSavepoint(4),
				)),
		},
		{step: step(barrier(k1, k2, false /* withLAI */))},
		{step: step(barrier(k3, k4, true /* withLAI */))},
	}

	w := echotest.NewWalker(t, datapathutils.TestDataPath(t, t.Name()))
	for i, test := range tests {
		name := fmt.Sprint(i)
		t.Run(name, w.Run(t, name, func(t *testing.T) string {
			var actual strings.Builder
			test.step.format(&actual, formatCtx{indent: "···"})
			return strings.TrimLeft(actual.String(), "\n")
		}))
	}
}
