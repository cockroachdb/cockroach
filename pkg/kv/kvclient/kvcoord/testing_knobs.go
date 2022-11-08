// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ClientTestingKnobs contains testing options that dictate the behavior
// of the key-value client.
type ClientTestingKnobs struct {
	// The RPC dispatcher. Defaults to grpc but can be changed here for
	// testing purposes.
	TransportFactory TransportFactory

	// DontConsiderConnHealth, if set, makes the GRPCTransport not take into
	// consideration the connection health when deciding the ordering for
	// replicas. When not set, replicas on nodes with unhealthy connections are
	// deprioritized.
	DontConsiderConnHealth bool

	// The maximum number of times a txn will attempt to refresh its
	// spans for a single transactional batch.
	// 0 means use a default. -1 means disable refresh.
	MaxTxnRefreshAttempts int

	// CondenseRefreshSpansFilter, if set, is called when the span refresher is
	// considering condensing the refresh spans. If it returns false, condensing
	// will not be attempted and the span refresher will behave as if condensing
	// failed to save enough memory.
	CondenseRefreshSpansFilter func() bool

	// LatencyFunc, if set, overrides RPCContext.RemoteClocks.Latency as the
	// function used by the DistSender to order replicas for follower reads.
	LatencyFunc LatencyFunc

	// If set, the DistSender will try the replicas in the order they appear in
	// the descriptor, instead of trying to reorder them by latency. The knob
	// only applies to requests sent with the LEASEHOLDER routing policy.
	DontReorderReplicas bool

	// DisableCommitSanityCheck allows "setting" the DisableCommitSanityCheck to
	// true without actually overriding the variable.
	DisableCommitSanityCheck bool

	// CommitWaitFilter allows tests to instrument the beginning of a transaction
	// commit wait sleep.
	CommitWaitFilter func()

	// OnRangeSpanningNonTxnalBatch is invoked whenever DistSender attempts to split
	// a non-transactional batch across a range boundary. The method may inject an
	// error which, if non-nil, becomes the result of the batch. Otherwise, execution
	// continues.
	OnRangeSpanningNonTxnalBatch func(ba *roachpb.BatchRequest) *roachpb.Error
}

var _ base.ModuleTestingKnobs = &ClientTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ClientTestingKnobs) ModuleTestingKnobs() {}
