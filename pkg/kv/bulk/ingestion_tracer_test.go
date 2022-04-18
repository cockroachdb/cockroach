// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sstutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestIngestionTracer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	cs := cluster.MakeTestingClusterSettings()
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	tr := s.TracerI().(*tracing.Tracer)

	// Jobs such as RESTORE and IMPORT run with a RecordingStructured recording
	// type, and so we want to ensure the ingestionTracer works as expected for
	// them.
	recordingType := tracing.RecordingStructured
	ctx, sp := tr.StartSpanCtx(ctx, "TestIngestionTracer",
		tracing.WithRecording(recordingType))
	defer sp.Finish()
	defer s.Stopper().Stop(ctx)

	b := &SSTBatcher{
		db: kvDB,
	}

	sendAddSST := func(ingestAsWrites bool, key string) (int, *roachpb.BulkRequestStats) {
		sst, start, end := sstutil.MakeSST(t, cs, []sstutil.KV{{key, 1, "3"}})
		iter, err := storage.NewMemSSTIterator(sst, true)
		require.NoError(t, err)
		defer iter.Close()

		sstStats, err := storage.ComputeStatsForRange(iter, start, end, timeutil.Now().UnixNano())
		require.NoError(t, err)
		_, _, stats, pErr := addSSTableWithTracing(ctx, &sstSpan{
			start:    start,
			end:      end,
			sstBytes: sst,
			stats:    sstStats,
		}, b, ingestAsWrites, recordingType)
		require.Nil(t, pErr)
		return len(sst), stats
	}

	sendAdminSplit := func(key roachpb.Key) *roachpb.BulkRequestStats {
		_, stats, err := adminSplitWithTracing(ctx, b.db, key, hlc.MaxTimestamp, recordingType)
		require.NoError(t, err)
		return stats
	}

	sendAdminScatter := func(key string) *roachpb.BulkRequestStats {
		_, _, stats, err := adminScatterWithTracing(ctx, b.db, roachpb.Key(key), 0, /* maxSize */
			recordingType)
		require.NoError(t, err)
		return stats
	}

	// Issue a couple of AddSSTable requests.
	sst1Sz, addSST1Stats := sendAddSST(false /* ingestAsWrites */, "bc")
	sst2Sz, addSST2Stats := sendAddSST(true /* ingestAsWrites */, "ef")

	// Now an Admin{Split,Scatter}.
	adminSplitRec := sendAdminSplit(roachpb.Key("b"))
	adminScatterRec := sendAdminScatter("b")

	i := newIngestionTracer("foo", sp)

	// Notify the tracer of the requests.
	i.notify(0 /* duration */, adminSplitRec)
	i.notify(0 /* duration */, adminScatterRec)
	i.notify(0 /* duration */, addSST1Stats)
	i.notify(0 /* duration */, addSST2Stats)

	checkTracerState := func(i *ingestionTracer) {
		// Check the state of the tracer.
		require.Equal(t, "foo", i.name)

		require.Equal(t, 2, i.addSSTTag.mu.numRequests)
		require.Equal(t, 1, i.addSSTTag.mu.numIngestAsWrites)
		require.Equal(t, int64(sst1Sz+sst2Sz), i.addSSTTag.mu.dataSize)

		require.Equal(t, 1, i.splitTag.mu.numRequests)
		require.Equal(t, 1, i.scatterTag.mu.numRequests)
	}
	checkTracerState(i)

	// Let's initialize a new tracer but with the same name, and ensure it
	// subsumes the old tag values.
	i2 := newIngestionTracer("foo", sp)
	checkTracerState(i2)
}
