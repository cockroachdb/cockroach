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
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(kvpb.ConditionalPut, declareKeysConditionalPut, ConditionalPut)
}

func declareKeysConditionalPut(
	rs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	lockSpans *lockspanset.LockSpanSet,
	maxOffset time.Duration,
) error {
	args := req.(*kvpb.ConditionalPutRequest)
	if args.Inline {
		return DefaultDeclareKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	} else {
		return DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	}
}

// ConditionalPut sets the value for a specified key only if
// the expected value matches. If not, the return value contains
// the actual value.
func ConditionalPut(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.ConditionalPutRequest)
	h := cArgs.Header

	var ts hlc.Timestamp
	if !args.Inline {
		ts = h.Timestamp
	}

	if err := args.Validate(); err != nil {
		return result.Result{}, err
	}

	originTimestampForValueHeader := h.WriteOptions.GetOriginTimestamp()
	if args.OriginTimestamp.IsSet() {
		originTimestampForValueHeader = args.OriginTimestamp
	}
	if args.OriginTimestamp.IsSet() && h.WriteOptions.GetOriginTimestamp().IsSet() {
		return result.Result{}, errors.AssertionFailedf("OriginTimestamp cannot be passed via CPut arg and in request header")
	}

	opts := storage.ConditionalPutWriteOptions{
		MVCCWriteOptions: storage.MVCCWriteOptions{
			Txn:                            h.Txn,
			LocalTimestamp:                 cArgs.Now,
			Stats:                          cArgs.Stats,
			ReplayWriteTimestampProtection: h.AmbiguousReplayProtection,
			OmitInRangefeeds:               cArgs.OmitInRangefeeds,
			OriginID:                       h.WriteOptions.GetOriginID(),
			OriginTimestamp:                originTimestampForValueHeader,
			MaxLockConflicts:               storage.MaxConflictsPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV),
			TargetLockConflictBytes:        storage.TargetBytesPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV),
			Category:                       fs.BatchEvalReadCategory,
		},
		AllowIfDoesNotExist:         storage.CPutMissingBehavior(args.AllowIfDoesNotExist),
		OriginTimestamp:             args.OriginTimestamp,
		ShouldWinOriginTimestampTie: args.ShouldWinOriginTimestampTie,
	}

	var err error
	var acq roachpb.LockAcquisition
	if args.Blind {
		acq, err = storage.MVCCBlindConditionalPut(
			ctx, readWriter, args.Key, ts, args.Value, args.ExpBytes, opts)
	} else {
		acq, err = storage.MVCCConditionalPut(
			ctx, readWriter, args.Key, ts, args.Value, args.ExpBytes, opts)
	}
	if err != nil {
		return result.Result{}, err
	}
	return result.WithAcquiredLocks(acq), nil
}
