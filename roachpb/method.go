// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package roachpb

// Method is the enumerated type for methods.
type Method int

//go:generate stringer -type=Method
const (
	// Get fetches the value for a key from the KV map, respecting a
	// possibly historical timestamp. If the timestamp is 0, returns
	// the most recent value.
	Get Method = iota
	// Put sets the value for a key at the specified timestamp. If the
	// timestamp is 0, the value is set with the current time as timestamp.
	Put
	// ConditionalPut sets the value for a key if the existing value
	// matches the value specified in the request. Specifying a null value
	// for existing means the value must not yet exist.
	ConditionalPut
	// Increment increments the value at the specified key. Once called
	// for a key, Put & ConditionalPut will return errors; only
	// Increment will continue to be a valid command. The value must be
	// deleted before it can be reset using Put.
	Increment
	// Delete removes the value for the specified key.
	Delete
	// DeleteRange removes all values for keys which fall between
	// args.RequestHeader.Key and args.RequestHeader.EndKey, with
	// the latter endpoint excluded.
	DeleteRange
	// Scan fetches the values for all keys which fall between
	// args.RequestHeader.Key and args.RequestHeader.EndKey, with
	// the latter endpoint excluded.
	Scan
	// ReverseScan fetches the values for all keys which fall between
	// args.RequestHeader.Key and args.RequestHeader.EndKey, with
	// the latter endpoint excluded.
	ReverseScan
	// BeginTransaction writes a new transaction record, marking the
	// beginning of the write-portion of a transaction. It is sent
	// exclusively by the coordinating node along with the first
	// transactional write and neither sent nor received by the client
	// itself.
	BeginTransaction
	// EndTransaction either commits or aborts an ongoing transaction.
	EndTransaction
	// AdminSplit is called to coordinate a split of a range.
	AdminSplit
	// AdminMerge is called to coordinate a merge of two adjacent ranges.
	AdminMerge
	// HeartbeatTxn sends a periodic heartbeat to extant
	// transaction rows to indicate the client is still alive and
	// the transaction should not be considered abandoned.
	HeartbeatTxn
	// GC garbage collects values based on expired timestamps
	// for a list of keys in a range. This method is called by the
	// range leader after a snapshot scan. The call goes through Raft,
	// so all range replicas GC the exact same values.
	GC
	// PushTxn attempts to resolve read or write conflicts between
	// transactions. Both the pusher (args.Txn) and the pushee
	// (args.PushTxn) are supplied. However, args.Key should be set to the
	// transaction ID of the pushee, as it must be directed to the range
	// containing the pushee's transaction record in order to consult the
	// most up to date txn state. If the conflict resolution can be
	// resolved in favor of the pusher, returns success; otherwise returns
	// an error code either indicating the pusher must retry or abort and
	// restart the transaction.
	PushTxn
	// RangeLookup looks up range descriptors, containing the
	// locations of replicas for the range containing the specified key.
	RangeLookup
	// ResolveIntent resolves existing write intents for a key.
	ResolveIntent
	// ResolveIntentRange resolves existing write intents for a key range.
	ResolveIntentRange
	// Noop is a no-op.
	Noop
	// Merge merges a given value into the specified key. Merge is a
	// high-performance operation provided by underlying data storage for values
	// which are accumulated over several writes. Because it is not
	// transactional, Merge is currently not made available to external clients.
	//
	// The logic used to merge values of different types is described in more
	// detail by the "Merge" method of engine.Engine.
	Merge
	// TruncateLog discards a prefix of the raft log.
	TruncateLog
	// LeaderLease requests a leader lease for a replica.
	LeaderLease
	// ComputeChecksum starts a checksum computation over a replica snapshot.
	ComputeChecksum
	// VerifyChecksum verifies the checksum computed through an earlier
	// ComputeChecksum.
	VerifyChecksum
	// CheckConsistency verifies the consistency of all ranges falling within a
	// key span.
	CheckConsistency
)
