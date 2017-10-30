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
	"github.com/cockroachdb/cockroach/pkg/storage/pushtxnq"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// ReplicaI is the glue between a ReplicaEvalContext and a *Replica
// to avoid a direct dependency.
type ReplicaI interface {
	fmt.Stringer
	ClusterSettings() *cluster.Settings
	StoreTestingKnobs() StoreTestingKnobs

	// TODO(tschottdorf): available through ClusterSettings().
	Tracer() opentracing.Tracer

	Engine() engine.Engine
	DB() *client.DB
	AbortSpan() *abortspan.AbortSpan
	GetPushTxnQueue() *pushtxnq.PushTxnQueue

	NodeID() roachpb.NodeID
	StoreID() roachpb.StoreID
	GetRangeID() roachpb.RangeID

	IsFirstRange() bool
	GetFirstIndex() (uint64, error)
	GetTerm(uint64) (uint64, error)

	Desc() *roachpb.RangeDescriptor

	GetMVCCStats() enginepb.MVCCStats
	GetGCThreshold() hlc.Timestamp
	GetTxnSpanGCThreshold() hlc.Timestamp
	GetLastReplicaGCTimestamp(context.Context) (hlc.Timestamp, error)
	GetLease() (roachpb.Lease, *roachpb.Lease)
}
