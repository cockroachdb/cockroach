// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvevent_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

func makeKVs(t *testing.T, count int) []roachpb.KeyValue {
	kv := func(tableID uint32, k, v string, ts hlc.Timestamp) roachpb.KeyValue {
		vDatum := tree.DString(k)
		key, err := rowenc.EncodeTableKey(keys.SystemSQLCodec.TablePrefix(tableID), &vDatum, encoding.Ascending)
		require.NoError(t, err)

		return roachpb.KeyValue{
			Key: key,
			Value: roachpb.Value{
				RawBytes:  []byte(v),
				Timestamp: ts,
			},
		}
	}

	ret := make([]roachpb.KeyValue, count)
	for i := 0; i < count; i++ {
		ret[i] = kv(42, "a", fmt.Sprintf("b-%d", count), hlc.Timestamp{WallTime: int64(count + 1)})
	}
	return ret
}

func TestBlockingBuffer(t *testing.T) {
	metrics := kvevent.MakeMetrics(time.Minute)
	settings := cluster.MakeTestingClusterSettings()
	mm := mon.NewUnlimitedMonitor(
		context.Background(), "test", mon.MemoryResource,
		nil /* curCount */, nil /* maxHist */, math.MaxInt64, settings,
	)

	buf := kvevent.NewMemBuffer(mm.MakeBoundAccount(), &metrics)
	kvCount := rand.Intn(20) + 1
	kvs := makeKVs(t, kvCount)
	ctx := context.Background()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for _, kv := range kvs {
			err := buf.AddKV(ctx, kv, roachpb.Value{}, hlc.Timestamp{})
			require.NoError(t, err)
		}
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		for i := 0; i < kvCount; i++ {
			_, err := buf.Get(ctx)
			require.NoError(t, err)
		}
		wg.Done()
	}()
	wg.Wait()
}
