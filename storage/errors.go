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
// implied.  See the License for the specific language governing
// permissions and limitations under the L:loicense. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"encoding/gob"
	"fmt"

	"github.com/cockroachdb/cockroach/storage/engine"
)

// A NotLeaderError indicates that the current range is not the
// leader. If the leader is known, its Replica is set in the error.
type NotLeaderError struct {
	leader Replica
}

// Error formats error.
func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("range not leader; leader is %+v", e.leader)
}

// RangeNotFoundError indicates that a command was sent to a range which
// is not hosted on this store.
type RangeNotFoundError struct {
	RangeID int64
}

// NewRangeNotFoundError initializes a new RangeNotFoundError.
func NewRangeNotFoundError(rid int64) *RangeNotFoundError {
	return &RangeNotFoundError{
		RangeID: rid,
	}
}

// Error formats error.
func (e *RangeNotFoundError) Error() string {
	return fmt.Sprintf("range %d was not found at requested store.", e.RangeID)
}

// CanRetry indicates whether or not this RangeNotFoundError can be retried.
func (e *RangeNotFoundError) CanRetry() bool {
	return true
}

// RangeKeyMismatchError indicates that a command was sent to a range which did
// not contain the key(s) specified by the command.
type RangeKeyMismatchError struct {
	RequestStartKey engine.Key
	RequestEndKey   engine.Key
	Range           RangeMetadata
}

// NewRangeKeyMismatchError initializes a new RangeKeyMismatchError.
func NewRangeKeyMismatchError(start, end engine.Key,
	metadata RangeMetadata) *RangeKeyMismatchError {
	return &RangeKeyMismatchError{
		RequestStartKey: start,
		RequestEndKey:   end,
		Range:           metadata,
	}
}

// Error formats error.
func (e *RangeKeyMismatchError) Error() string {
	return fmt.Sprintf("key range %q-%q outside of bounds of range %d: %q-%q",
		string(e.RequestStartKey), string(e.RequestEndKey),
		string(e.Range.RangeID),
		string(e.Range.StartKey), string(e.Range.EndKey))
}

// CanRetry indicates whether or not this RangeKeyMismatchError can be retried.
func (e *RangeKeyMismatchError) CanRetry() bool {
	return true
}

// Init registers storage error types with Gob.
func init() {
	gob.Register(&NotLeaderError{})
	gob.Register(&RangeNotFoundError{})
	gob.Register(&RangeKeyMismatchError{})
}
