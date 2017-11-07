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

package storage

import (
	"fmt"

	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// todoSpanSet is a placeholder value for callsites that need to pass a properly
// populated SpanSet (with according protection by the command queue) but fail
// to do so at the time of writing.
//
// See https://github.com/cockroachdb/cockroach/issues/19851.
//
// Do not introduce new uses of this.
var todoSpanSet = &SpanSet{}

// NewReplicaEvalContext returns a ReplicaEvalContext to use for command
// evaluation. The supplied SpanSet will be ignored except for race builds, in
// which case state access is asserted against it. A SpanSet must always be
// passed.
func NewReplicaEvalContext(r *Replica, ss *SpanSet) ReplicaEvalContext {
	if ss == nil {
		log.Fatalf(r.AnnotateCtx(context.Background()), "can't create a ReplicaEvalContext with assertions but no SpanSet")
	}
	if util.RaceEnabled {
		return &SpanSetReplicaEvalContext{
			i:  r,
			ss: *ss,
		}
	}
	return r
}

// ReplicaEvalContext is the interface through which command evaluation accesses
// the in-memory state of a Replica.
type ReplicaEvalContext interface {
	fmt.Stringer
	ClusterSettings() *cluster.Settings
	StoreTestingKnobs() StoreTestingKnobs

	// TODO(tschottdorf): available through ClusterSettings().
	Tracer() opentracing.Tracer

	Engine() engine.Engine
	DB() *client.DB
	AbortSpan() *abortspan.AbortSpan
	GetTxnWaitQueue() *txnwait.Queue

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
