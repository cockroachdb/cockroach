// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	gogotypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

// TestMetaSinkRoundTrip verifies that a Flush handed to metaSink
// arrives intact on the channel and survives the
// marshal/decode pair used to bridge a Producer's TickSink across
// the DistSQL boundary into the gateway-side TickManager.
func TestMetaSinkRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	ch := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress, 1)
	sink := newMetaSink(ch)

	tickEnd := hlc.Timestamp{WallTime: int64(120 * time.Second)}
	want := &revlogpb.Flush{
		Files: []revlogpb.FlushedFile{
			{
				TickEnd: tickEnd,
				File:    revlogpb.File{FileID: 11, FlushOrder: 3},
			},
		},
		Checkpoints: []kvpb.RangeFeedCheckpoint{
			{
				Span:       roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
				ResolvedTS: hlc.Timestamp{WallTime: 119},
			},
		},
	}
	require.NoError(t, sink.Flush(ctx, want))

	prog := <-ch
	require.Equal(t, flushTypeURL, prog.ProgressDetails.TypeUrl)

	got, err := DecodeFlush(prog.ProgressDetails)
	require.NoError(t, err)
	require.Len(t, got.Files, 1)
	require.Equal(t, want.Files[0].TickEnd, got.Files[0].TickEnd)
	require.Equal(t, want.Files[0].File, got.Files[0].File)
	require.Len(t, got.Checkpoints, 1)
	require.Equal(t, want.Checkpoints[0].Span, got.Checkpoints[0].Span)
	require.Equal(t, want.Checkpoints[0].ResolvedTS, got.Checkpoints[0].ResolvedTS)
}

// TestMetaSinkBlocksUntilCtxDone verifies Flush returns ctx.Err()
// when the receiver isn't draining and the context is cancelled —
// rather than blocking forever or silently dropping the message.
func TestMetaSinkBlocksUntilCtxDone(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ch := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress) // unbuffered, no reader
	sink := newMetaSink(ch)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- sink.Flush(ctx, &revlogpb.Flush{})
	}()

	// Verify the call hasn't returned (sender is parked).
	select {
	case err := <-errCh:
		t.Fatalf("Flush returned before receiver drained or ctx cancelled: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Flush did not return after ctx cancel")
	}
}

// TestDecodeFlushRejectsGarbage verifies DecodeFlush surfaces a
// wrapped error rather than silently returning a zero-value Flush
// when handed bytes that aren't a valid revlogpb.Flush.
func TestDecodeFlushRejectsGarbage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	_, err := DecodeFlush(gogotypes.Any{
		TypeUrl: flushTypeURL,
		Value:   []byte{0xff, 0x12, 0x34},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "decoding Flush")
}
