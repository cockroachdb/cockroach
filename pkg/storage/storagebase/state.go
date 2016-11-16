// Copyright 2016 The Cockroach Authors.
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

package storagebase

import "github.com/cockroachdb/cockroach/pkg/util/hlc"

// IsFrozen returns true if the underlying ReplicaState indicates that the
// Replica is frozen.
func (s ReplicaState) IsFrozen() bool {
	return s.Frozen == ReplicaState_FROZEN
}

// GetLastProcessed returns the last processed time corresponding to the
// specified queue. If no last processed time is recorded, returns the
// low water timestamp.
func (qs *QueueState) GetLastProcessed(name string) hlc.Timestamp {
	if qs.LastProcessed == nil {
		return hlc.ZeroTimestamp
	}
	if ts, ok := qs.LastProcessed[name]; ok {
		return ts
	}
	return hlc.ZeroTimestamp
}

// SetLastProcessed updates the last processed time for the specified queue.
func (qs *QueueState) SetLastProcessed(name string, ts hlc.Timestamp) {
	if qs.LastProcessed == nil {
		qs.LastProcessed = map[string]hlc.Timestamp{}
	}
	qs.LastProcessed[name] = ts
}
