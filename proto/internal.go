// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

package proto

import (
	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
)

const (
	// InternalRangeLookup looks up range descriptors, containing the
	// locations of replicas for the range containing the specified key.
	InternalRangeLookup = "InternalRangeLookup"
	// InternalHeartbeatTxn sends a periodic heartbeat to extant
	// transaction rows to indicate the client is still alive and
	// the transaction should not be considered abandoned.
	InternalHeartbeatTxn = "InternalHeartbeatTxn"
	// InternalGC garbage collects values based on expired timestamps
	// for a list of keys in a range. This method is called by the
	// range leader after a snapshot scan. The call goes through Raft,
	// so all range replicas GC the exact same values.
	InternalGC = "InternalGC"
	// InternalPushTxn attempts to resolve read or write conflicts between
	// transactions. Both the pusher (args.Txn) and the pushee
	// (args.PushTxn) are supplied. However, args.Key should be set to the
	// transaction ID of the pushee, as it must be directed to the range
	// containing the pushee's transaction record in order to consult the
	// most up to date txn state. If the conflict resolution can be
	// resolved in favor of the pusher, returns success; otherwise returns
	// an error code either indicating the pusher must retry or abort and
	// restart the transaction.
	InternalPushTxn = "InternalPushTxn"
	// InternalResolveIntent resolves existing write intents for a key or
	// key range.
	InternalResolveIntent = "InternalResolveIntent"
	// InternalMerge merges a given value into the specified key. Merge is a
	// high-performance operation provided by underlying data storage for values
	// which are accumulated over several writes. Because it is not
	// transactional, Merge is currently not made available to external clients.
	//
	// The logic used to merge values of different types is described in more
	// detail by the "Merge" method of engine.Engine.
	InternalMerge = "InternalMerge"
	// InternalTruncateLog discards a prefix of the raft log.
	InternalTruncateLog = "InternalTruncateLog"
	// InternalLeaderLease requests a leader lease for a replica.
	InternalLeaderLease = "InternalLeaderLease"
)

// ToValue generates a Value message which contains an encoded copy of this
// TimeSeriesData in its "bytes" field. The returned Value will also have its
// "tag" string set to the TIME_SERIES constant.
func (ts *InternalTimeSeriesData) ToValue() (*Value, error) {
	b, err := gogoproto.Marshal(ts)
	if err != nil {
		return nil, err
	}
	return &Value{
		Bytes: b,
		Tag:   gogoproto.String(_CR_TS.String()),
	}, nil
}

// InternalTimeSeriesDataFromValue attempts to extract an InternalTimeSeriesData
// message from the "bytes" field of the given value.
func InternalTimeSeriesDataFromValue(value *Value) (*InternalTimeSeriesData, error) {
	if value.GetTag() != _CR_TS.String() {
		return nil, util.Errorf("value is not tagged as containing TimeSeriesData: %v", value)
	}
	var ts InternalTimeSeriesData
	err := gogoproto.Unmarshal(value.Bytes, &ts)
	if err != nil {
		return nil, util.Errorf("TimeSeriesData could not be unmarshalled from value: %v %s", value, err)
	}
	return &ts, nil
}
