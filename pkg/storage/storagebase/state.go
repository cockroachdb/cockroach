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

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// ResponseWithError is a tuple of a BatchResponse and an error. It is used to
// pass around a BatchResponse with its associated error where that
// entanglement is necessary (e.g. channels, methods that need to return
// another error in addition to this one).
type ResponseWithError struct {
	Reply *roachpb.BatchResponse
	Err   *roachpb.Error
	Debug interface{}
}

// IsFrozen returns true if the underlying ReplicaState indicates that the
// Replica is frozen.
func (s ReplicaState) IsFrozen() bool {
	return s.Frozen == ReplicaState_FROZEN
}
