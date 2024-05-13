// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/poison"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanlatch"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/redact"
)

// latchManagerImpl implements the latchManager interface.
type latchManagerImpl struct {
	m spanlatch.Manager
}

func (m *latchManagerImpl) Acquire(ctx context.Context, req Request) (latchGuard, *Error) {
	lg, err := m.m.Acquire(ctx, req.LatchSpans, req.PoisonPolicy, req.BaFmt, req.Txn)
	if err != nil {
		return nil, kvpb.NewError(err)
	}
	return lg, nil
}

func (m *latchManagerImpl) AcquireOptimistic(req Request) latchGuard {
	lg := m.m.AcquireOptimistic(req.LatchSpans, req.PoisonPolicy, req.BaFmt, req.Txn)
	return lg
}

func (m *latchManagerImpl) CheckOptimisticNoConflicts(lg latchGuard, spans *spanset.SpanSet) bool {
	return m.m.CheckOptimisticNoConflicts(lg.(*spanlatch.Guard), spans)
}

func (m *latchManagerImpl) WaitUntilAcquired(
	ctx context.Context, lg latchGuard,
) (latchGuard, *Error) {
	lg, err := m.m.WaitUntilAcquired(ctx, lg.(*spanlatch.Guard))
	if err != nil {
		return nil, kvpb.NewError(err)
	}
	return lg, nil
}

func (m *latchManagerImpl) WaitFor(
	ctx context.Context,
	ss *spanset.SpanSet,
	pp poison.Policy,
	baFmt redact.SafeFormatter,
	txn *roachpb.Transaction,
) *Error {
	err := m.m.WaitFor(ctx, ss, pp, baFmt, txn)
	if err != nil {
		return kvpb.NewError(err)
	}
	return nil
}

func (m *latchManagerImpl) Poison(lg latchGuard) {
	m.m.Poison(lg.(*spanlatch.Guard))
}

func (m *latchManagerImpl) Release(ctx context.Context, lg latchGuard) {
	m.m.Release(ctx, lg.(*spanlatch.Guard))
}

func (m *latchManagerImpl) Metrics() LatchMetrics {
	return m.m.Metrics()
}
