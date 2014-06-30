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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

// Key defines the key in the key-value datastore.
type Key []byte

// Value specifies the value at a key. Multiple values at the same key
// are supported based on timestamp. Values which have been overwritten
// have an associated expiration, after which they will be permanently
// deleted.
type Value struct {
	// Bytes is the byte string value.
	Bytes []byte
	// Timestamp of value in nanoseconds since epoch.
	Timestamp int64
	// Expiration in nanoseconds.
	Expiration int64
}

// KeyValue is a pair of Key and Value for returned Key/Value pairs
// from ScanRequest/ScanResponse. It embeds a Key and a Value.
type KeyValue struct {
	Key
	Value
}

// RequestHeader is supplied with every storage node request.
type RequestHeader struct {
	// Timestamp specifies time at which read or writes should be
	// performed. In nanoseconds since the epoch. Defaults to current
	// wall time.
	Timestamp int64

	// The following values are set internally and should not be set
	// manually.

	// Replica specifies the destination for the request. See config.go.
	Replica Replica
	// MaxTimestamp is the maximum wall time seen by the client to
	// date. This should be supplied with successive transactions for
	// linearalizability for this client. In nanoseconds since the
	// epoch.
	MaxTimestamp int64
	// TxID is set non-empty if a transaction is underway. Empty string
	// to start a new transaction.
	TxID string
}

// ResponseHeader is returned with every storage node response.
type ResponseHeader struct {
	// Error is non-nil if an error occurred.
	Error error
	// TxID is non-empty if a transaction is underway.
	TxID string
}

// A ContainsRequest is arguments to the Contains() method.
type ContainsRequest struct {
	RequestHeader
	Key Key
}

// A ContainsResponse is the return value of the Contains() method.
type ContainsResponse struct {
	ResponseHeader
	Exists bool
}

// A GetRequest is arguments to the Get() method.
type GetRequest struct {
	RequestHeader
	Key Key
}

// A GetResponse is the return value from the Get() method.
// If the key doesn't exist, returns nil for Value.Bytes.
type GetResponse struct {
	ResponseHeader
	Value Value
}

// A PutRequest is arguments to the Put() method.
// Conditional puts are supported if ExpValue is set.
// - Returns true and sets value if ExpValue equals existing value.
// - If key doesn't exist and ExpValue is empty, sets value.
// - Otherwise, returns error.
type PutRequest struct {
	RequestHeader
	Key      Key    // must be non-empty
	Value    Value  // The value to put
	ExpValue *Value // ExpValue.Bytes empty to test for non-existence
}

// A PutResponse is the return value form the Put() method.
type PutResponse struct {
	ResponseHeader
	ActualValue *Value // ActualValue.Bytes set if conditional put failed
}

// An IncrementRequest is arguments to the Increment() method. It
// increments the value for key, interpreting the existing value as a
// varint64.
type IncrementRequest struct {
	RequestHeader
	Key       Key
	Increment int64
}

// An IncrementResponse is the return value from the Increment
// method. The new value after increment is specified in NewValue. If
// the value could not be decoded as specified, Error will be set.
type IncrementResponse struct {
	ResponseHeader
	NewValue int64
}

// A DeleteRequest is arguments to the Delete() method.
type DeleteRequest struct {
	RequestHeader
	Key Key
}

// A DeleteResponse is the return value from the Delete() method.
type DeleteResponse struct {
	ResponseHeader
}

// A DeleteRangeRequest is arguments to the DeleteRange method. It
// specifies the range of keys to delete.
type DeleteRangeRequest struct {
	RequestHeader
	StartKey Key // Empty to start at first key
	EndKey   Key // Non-inclusive; if empty, deletes all
}

// A DeleteRangeResponse is the return value from the DeleteRange()
// method.
type DeleteRangeResponse struct {
	ResponseHeader
	NumDeleted uint64
}

// A ScanRequest is arguments to the Scan() method. It specifies the
// start and end keys for the scan and the maximum number of results.
type ScanRequest struct {
	RequestHeader
	StartKey   Key   // Empty to start at first key
	EndKey     Key   // Optional max key; empty to ignore
	MaxResults int64 // Must be > 0
}

// A ScanResponse is the return value from the Scan() method.
type ScanResponse struct {
	ResponseHeader
	Rows []KeyValue // Empty if no rows were scanned
}

// An EndTransactionRequest is arguments to the EndTransaction() method.
// It specifies whether to commit or roll back an extant transaction.
// It also lists the keys involved in the transaction so their write
// intents may be aborted or committed.
type EndTransactionRequest struct {
	RequestHeader
	Commit bool  // False to abort and rollback
	Keys   []Key // Write-intent keys to commit or abort
}

// An EndTransactionResponse is the return value from the
// EndTransaction() method. It specifies the commit timestamp for the
// final transaction (all writes will have this timestamp). It further
// specifies the commit wait, which is the remaining time the client
// MUST wait before signalling completion of the transaction to another
// distributed node to maintain consistency.
type EndTransactionResponse struct {
	ResponseHeader
	CommitTimestamp int64 // Unix nanos (us)
	CommitWait      int64 // Remaining with (us)
}

// An AccumulateTSRequest is arguments to the AccumulateTS() method.
// It specifies the key at which to accumulate TS values, and the
// time series counts for this discrete time interval.
type AccumulateTSRequest struct {
	RequestHeader
	Key    Key
	Counts []int64 // One per discrete subtime period (e.g. one/minute or one/second)
}

// An AccumulateTSResponse is the return value from the AccumulateTS()
// method.
type AccumulateTSResponse struct {
	ResponseHeader
}

// A ReapQueueRequest is arguments to the ReapQueue() method. It
// specifies the recipient inbox key to which messages are waiting
// to be reapted and also the maximum number of results to return.
type ReapQueueRequest struct {
	RequestHeader
	Inbox      Key   // Recipient inbox key
	MaxResults int64 // Maximum results to return; must be > 0
}

// A ReapQueueResponse is the return value from the ReapQueue() method.
type ReapQueueResponse struct {
	ResponseHeader
	Messages []Value
}

// An EnqueueUpdateRequest is arguments to the EnqueueUpdate() method.
// It specifies the update to enqueue for asynchronous execution.
// Update is an instance of one of the following messages: PutRequest,
// IncrementRequest, DeleteRequest, DeleteRangeRequest, or
// AccountingRequest.
type EnqueueUpdateRequest struct {
	RequestHeader
	Update interface{}
}

// An EnqueueUpdateResponse is the return value from the
// EnqueueUpdate() method.
type EnqueueUpdateResponse struct {
	ResponseHeader
}

// An EnqueueMessageRequest is arguments to the EnqueueMessage() method.
// It specifies the recipient inbox key and the message (an arbitrary
// byte slice value).
type EnqueueMessageRequest struct {
	RequestHeader
	Inbox   Key   // Recipient key
	Message Value // Message value to delivery to inbox
}

// An EnqueueMessageResponse is the return value from the
// EnqueueMessage() method.
type EnqueueMessageResponse struct {
	ResponseHeader
}

// An InternalRangeLookupRequest is arguments to the InternalRangeLookup()
// method. It specifies the key for range lookup, which is a system key prefixed
// by KeyMeta1Prefix or KeyMeta2Prefix to the user key.
type InternalRangeLookupRequest struct {
	RequestHeader
	Key Key
}

// An InternalRangeLookupResponse is the return value from the
// InternalRangeLookup() method. It returns the metadata for the
// range where the key resides. When looking up 1-level metadata,
// it returns the info for the range containing the 2-level metadata
// for the key. And when looking up 2-level metadata, it returns the
// info for the range possibly containing the actual key and its value.
type InternalRangeLookupResponse struct {
	ResponseHeader
	EndKey Key // The key in datastore whose value is the Range object.
	Range  RangeDescriptor
}
