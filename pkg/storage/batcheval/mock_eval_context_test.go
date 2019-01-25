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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type MockEvalCtx struct {
	clusterSettings *cluster.Settings
	desc            *roachpb.RangeDescriptor
	clock           *hlc.Clock
	stats           enginepb.MVCCStats
	qps             float64
	abortSpan       *abortspan.AbortSpan
	gcThreshold     hlc.Timestamp
	MockEngine      engine.Engine
}

func (m *MockEvalCtx) String() string {
	return "mock"
}
func (m *MockEvalCtx) ClusterSettings() *cluster.Settings {
	return m.clusterSettings
}
func (m *MockEvalCtx) EvalKnobs() storagebase.BatchEvalTestingKnobs {
	panic("unimplemented")
}
func (m *MockEvalCtx) Engine() engine.Engine {
	return m.MockEngine
}
func (m *MockEvalCtx) Clock() *hlc.Clock {
	return m.clock
}
func (m *MockEvalCtx) DB() *client.DB {
	panic("unimplemented")
}
func (m *MockEvalCtx) GetLimiters() *Limiters {
	panic("unimplemented")
}
func (m *MockEvalCtx) AbortSpan() *abortspan.AbortSpan {
	return m.abortSpan
}
func (m *MockEvalCtx) GetTxnWaitQueue() *txnwait.Queue {
	panic("unimplemented")
}
func (m *MockEvalCtx) NodeID() roachpb.NodeID {
	panic("unimplemented")
}
func (m *MockEvalCtx) StoreID() roachpb.StoreID {
	panic("unimplemented")
}
func (m *MockEvalCtx) GetRangeID() roachpb.RangeID {
	return m.desc.RangeID
}
func (m *MockEvalCtx) IsFirstRange() bool {
	panic("unimplemented")
}
func (m *MockEvalCtx) GetFirstIndex() (uint64, error) {
	panic("unimplemented")
}
func (m *MockEvalCtx) GetTerm(uint64) (uint64, error) {
	panic("unimplemented")
}
func (m *MockEvalCtx) GetLeaseAppliedIndex() uint64 {
	panic("unimplemented")
}
func (m *MockEvalCtx) Desc() *roachpb.RangeDescriptor {
	return m.desc
}
func (m *MockEvalCtx) ContainsKey(key roachpb.Key) bool {
	panic("unimplemented")
}
func (m *MockEvalCtx) GetMVCCStats() enginepb.MVCCStats {
	return m.stats
}
func (m *MockEvalCtx) GetSplitQPS() float64 {
	return m.qps
}
func (m *MockEvalCtx) CanCreateTxnRecord(
	uuid.UUID, []byte, hlc.Timestamp,
) (bool, hlc.Timestamp, roachpb.TransactionAbortedReason) {
	panic("unimplemented")
}
func (m *MockEvalCtx) GetGCThreshold() hlc.Timestamp {
	return m.gcThreshold
}
func (m *MockEvalCtx) GetTxnSpanGCThreshold() hlc.Timestamp {
	panic("unimplemented")
}
func (m *MockEvalCtx) GetLastReplicaGCTimestamp(context.Context) (hlc.Timestamp, error) {
	panic("unimplemented")
}
func (m *MockEvalCtx) GetLease() (roachpb.Lease, roachpb.Lease) {
	panic("unimplemented")
}
