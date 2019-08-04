// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"golang.org/x/time/rate"
)

// Limiters is the collection of per-store limits used during cmd evaluation.
type Limiters struct {
	BulkIOWriteRate              *rate.Limiter
	ConcurrentImportRequests     limit.ConcurrentRequestLimiter
	ConcurrentExportRequests     limit.ConcurrentRequestLimiter
	AddSSTableRequestRate        *rate.Limiter
	ConcurrentAddSSTableRequests limit.ConcurrentRequestLimiter
	// concurrentRangefeedIters is a semaphore used to limit the number of
	// rangefeeds in the "catch-up" state across the store. The "catch-up" state
	// is a temporary state at the beginning of a rangefeed which is expensive
	// because it uses an engine iterator.
	ConcurrentRangefeedIters limit.ConcurrentRequestLimiter
}

// EvalContext is the interface through which command evaluation accesses the
// underlying state.
type EvalContext interface {
	fmt.Stringer
	ClusterSettings() *cluster.Settings
	EvalKnobs() storagebase.BatchEvalTestingKnobs

	Engine() engine.Engine
	Clock() *hlc.Clock
	DB() *client.DB
	AbortSpan() *abortspan.AbortSpan
	GetTxnWaitQueue() *txnwait.Queue
	GetLimiters() *Limiters

	NodeID() roachpb.NodeID
	StoreID() roachpb.StoreID
	GetRangeID() roachpb.RangeID
	GetNodeLocality() roachpb.Locality

	IsFirstRange() bool
	GetFirstIndex() (uint64, error)
	GetTerm(uint64) (uint64, error)
	GetLeaseAppliedIndex() uint64

	Desc() *roachpb.RangeDescriptor
	ContainsKey(key roachpb.Key) bool

	// CanCreateTxnRecord determines whether a transaction record can be created
	// for the provided transaction information. See Replica.CanCreateTxnRecord
	// for details about its arguments, return values, and preconditions.
	CanCreateTxnRecord(
		txnID uuid.UUID, txnKey []byte, txnMinTS hlc.Timestamp,
	) (ok bool, minCommitTS hlc.Timestamp, reason roachpb.TransactionAbortedReason)

	// GetMVCCStats returns a snapshot of the MVCC stats for the range.
	// If called from a command that declares a read/write span on the
	// entire range, the stats will be consistent with the data that is
	// visible to the batch. Otherwise, it may return inconsistent
	// results due to concurrent writes.
	GetMVCCStats() enginepb.MVCCStats

	// GetSplitQPS returns the queries/s request rate for this range.
	//
	// NOTE: This should not be used when the load based splitting cluster
	// setting is disabled.
	GetSplitQPS() float64

	GetGCThreshold() hlc.Timestamp
	GetLastReplicaGCTimestamp(context.Context) (hlc.Timestamp, error)
	GetLease() (roachpb.Lease, roachpb.Lease)
}
