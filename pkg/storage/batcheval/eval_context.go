// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package batcheval

import (
	"context"
	"fmt"

	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/time/rate"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
)

// Limiters is the collection of per-store limits used during cmd evaluation.
type Limiters struct {
	BulkIOWriteRate   *rate.Limiter
	ConcurrentImports limit.ConcurrentRequestLimiter
	ConcurrentExports limit.ConcurrentRequestLimiter
}

// EvalContext is the interface through which command evaluation accesses the
// underlying state.
type EvalContext interface {
	fmt.Stringer
	ClusterSettings() *cluster.Settings
	EvalKnobs() TestingKnobs

	// TODO(tschottdorf): available through ClusterSettings().
	Tracer() opentracing.Tracer

	Engine() engine.Engine
	Clock() *hlc.Clock
	DB() *client.DB
	AbortSpan() *abortspan.AbortSpan
	GetTxnWaitQueue() *txnwait.Queue
	GetLimiters() *Limiters

	NodeID() roachpb.NodeID
	StoreID() roachpb.StoreID
	GetRangeID() roachpb.RangeID

	IsFirstRange() bool
	GetFirstIndex() (uint64, error)
	GetTerm(uint64) (uint64, error)

	Desc() *roachpb.RangeDescriptor
	ContainsKey(key roachpb.Key) bool

	GetMVCCStats() enginepb.MVCCStats
	GetGCThreshold() hlc.Timestamp
	GetTxnSpanGCThreshold() hlc.Timestamp
	GetLastReplicaGCTimestamp(context.Context) (hlc.Timestamp, error)
	GetLease() (roachpb.Lease, *roachpb.Lease)
}
