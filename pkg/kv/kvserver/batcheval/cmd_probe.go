// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func declareKeysProbe(
	_ ImmutableRangeState,
	_ *kvpb.Header,
	_ kvpb.Request,
	_ *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	// Declare no keys. This means that we're not even serializing with splits
	// (i.e. a probe could be directed at a key that will become the right-hand
	// side of the split, and the split races ahead of the probe though the probe
	// will still execute on the left-hand side). This is acceptable; we want the
	// probe to bypass as much of the above-raft machinery as possible so that it
	// gives us a signal on the replication layer alone.
	return nil
}

func init() {
	RegisterReadWriteCommand(kvpb.Probe, declareKeysProbe, Probe)
}

// Probe causes an effectless round-trip through the replication layer,
// i.e. it is a write that does not change any kv pair. It declares a
// write on the targeted key (but no lock).
func Probe(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	return result.Result{
		Replicated: kvserverpb.ReplicatedEvalResult{
			IsProbe: true,
		},
	}, nil
}
