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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package proto

// Method is the enumerated type for methods.
type Method int

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
	// EndTransaction either commits or aborts an ongoing transaction.
	EndTransaction
	// ReapQueue scans and deletes messages from a recipient message
	// queue. ReapQueueRequest invocations must be part of an extant
	// transaction or they fail. Returns the reaped queue messsages, up to
	// the requested maximum. If fewer than the maximum were returned,
	// then the queue is empty.
	ReapQueue
	// EnqueueUpdate enqueues an update for eventual execution.
	EnqueueUpdate
	// EnqueueMessage enqueues a message for delivery to an inbox.
	EnqueueMessage
	// Batch executes a set of commands in parallel.
	Batch
	// CreateTable is called to create a new structured data table.
	CreateTable
	// AdminSplit is called to coordinate a split of a range.
	AdminSplit
	// AdminMerge is called to coordinate a merge of two adjacent ranges.
	AdminMerge
	// InternalRangeLookup looks up range descriptors, containing the
	// locations of replicas for the range containing the specified key.
	InternalRangeLookup
	// InternalHeartbeatTxn sends a periodic heartbeat to extant
	// transaction rows to indicate the client is still alive and
	// the transaction should not be considered abandoned.
	InternalHeartbeatTxn
	// InternalGC garbage collects values based on expired timestamps
	// for a list of keys in a range. This method is called by the
	// range leader after a snapshot scan. The call goes through Raft,
	// so all range replicas GC the exact same values.
	InternalGC
	// InternalPushTxn attempts to resolve read or write conflicts between
	// transactions. Both the pusher (args.Txn) and the pushee
	// (args.PushTxn) are supplied. However, args.Key should be set to the
	// transaction ID of the pushee, as it must be directed to the range
	// containing the pushee's transaction record in order to consult the
	// most up to date txn state. If the conflict resolution can be
	// resolved in favor of the pusher, returns success; otherwise returns
	// an error code either indicating the pusher must retry or abort and
	// restart the transaction.
	InternalPushTxn
	// InternalResolveIntent resolves existing write intents for a key.
	InternalResolveIntent
	// InternalResolveIntentRange resolves existing write intents for a key range.
	InternalResolveIntentRange
	// InternalMerge merges a given value into the specified key. Merge is a
	// high-performance operation provided by underlying data storage for values
	// which are accumulated over several writes. Because it is not
	// transactional, Merge is currently not made available to external clients.
	//
	// The logic used to merge values of different types is described in more
	// detail by the "Merge" method of engine.Engine.
	InternalMerge
	// InternalTruncateLog discards a prefix of the raft log.
	InternalTruncateLog
	// InternalLeaderLease requests a leader lease for a replica.
	InternalLeaderLease
	// InternalBatch implements batch processing of commands. This is a
	// superset of the Batch method.
	InternalBatch
)
