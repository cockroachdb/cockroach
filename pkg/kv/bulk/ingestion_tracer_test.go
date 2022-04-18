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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sstutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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

	sendAddSST := func(ingestAsWrites bool, key string) (int, tracing.Recording) {
		sst, start, end := sstutil.MakeSST(t, cs, []sstutil.KV{{key, 1, "3"}})
		req := &roachpb.AddSSTableRequest{
			RequestHeader:  roachpb.RequestHeader{Key: start, EndKey: end},
			Data:           sst,
			IngestAsWrites: ingestAsWrites,
		}

		_, _, rec, pErr := SendWrappedWithTracing(ctx, kvDB.NonTransactionalSender(), roachpb.Header{},
			roachpb.AdmissionHeader{}, req, recordingType)
		require.Nil(t, pErr)
		return len(sst), rec
	}

	sendAdminSplit := func(key string) tracing.Recording {
		rec, err := adminSplitWithTracing(ctx, kvDB, key, hlc.MaxTimestamp, recordingType)
		require.NoError(t, err)
		return rec
	}

	sendAdminScatter := func(key string) tracing.Recording {
		_, rec, err := adminScatterWithTracing(ctx, kvDB, roachpb.Key(key), 0, /* maxSize */
			recordingType)
		require.NoError(t, err)
		return rec
	}

	// Issue a couple of AddSSTable requests.
	sst1Sz, addSSTRec1 := sendAddSST(false /* ingestAsWrites */, "bc")
	sst2Sz, addSSTRec2 := sendAddSST(true /* ingestAsWrites */, "ef")

	// Now an Admin{Split,Scatter}.
	adminSplitRec := sendAdminSplit("b")
	adminScatterRec := sendAdminScatter("b")

	i, err := newIngestionTracer("foo", sp)
	require.NoError(t, err)

	// Notify the tracer of the requests.
	i.notifyAdminSplit(ctx, adminSplitRec)
	i.notifyAdminScatter(ctx, adminScatterRec)
	i.notifyAddSSTable(ctx, addSSTRec1)
	i.notifyAddSSTable(ctx, addSSTRec2)

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
	i2, err := newIngestionTracer("foo", sp)
	require.NoError(t, err)

	checkTracerState(i2)
}
