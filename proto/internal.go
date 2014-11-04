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

package proto

const (
	// InternalRangeLookup looks up range descriptors, containing the
	// locations of replicas for the range containing the specified key.
	InternalRangeLookup = "InternalRangeLookup"
	// InternalHeartbeatTxn sends a periodic heartbeat to extant
	// transaction rows to indicate the client is still alive and
	// the transaction should not be considered abandoned.
	InternalHeartbeatTxn = "InternalHeartbeatTxn"
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
	// InternalSnapshotCopy scans the key range specified by start key through
	// end key up to some maximum number of results from the given snapshot_id.
	// It will create a snapshot if snapshot_id is empty.
	InternalSnapshotCopy = "InternalSnapshotCopy"
)
