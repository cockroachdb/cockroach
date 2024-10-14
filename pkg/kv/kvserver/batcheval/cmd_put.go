// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func init() {
	RegisterReadWriteCommand(kvpb.Put, declareKeysPut, Put)
}

func declareKeysPut(
	rs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	lockSpans *lockspanset.LockSpanSet,
	maxOffset time.Duration,
) error {
	args := req.(*kvpb.PutRequest)
	if args.Inline {
		return DefaultDeclareKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	} else {
		return DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	}
}

// Put sets the value for a specified key.
func Put(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.PutRequest)
	h := cArgs.Header

	var ts hlc.Timestamp
	if !args.Inline {
		ts = h.Timestamp
	}

	opts := storage.MVCCWriteOptions{
		Txn:                            h.Txn,
		LocalTimestamp:                 cArgs.Now,
		Stats:                          cArgs.Stats,
		ReplayWriteTimestampProtection: h.AmbiguousReplayProtection,
		OmitInRangefeeds:               cArgs.OmitInRangefeeds,
		OriginID:                       h.WriteOptions.GetOriginID(),
		OriginTimestamp:                h.WriteOptions.GetOriginTimestamp(),
		MaxLockConflicts:               storage.MaxConflictsPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV),
		TargetLockConflictBytes:        storage.TargetBytesPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV),
		Category:                       fs.BatchEvalReadCategory,
	}

	var err error
	var acq roachpb.LockAcquisition
	if args.Blind {
		acq, err = storage.MVCCBlindPut(ctx, readWriter, args.Key, ts, args.Value, opts)
	} else {
		acq, err = storage.MVCCPut(ctx, readWriter, args.Key, ts, args.Value, opts)
	}
	if err != nil {
		return result.Result{}, err
	}
	return result.WithAcquiredLocks(acq), nil
}
