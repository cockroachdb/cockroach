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

import (
	"github.com/cockroachdb/cockroach/hlc"
	"github.com/cockroachdb/cockroach/storage/engine"
)

// ClientCmdID provides a unique ID for client commands. Clients which
// provide ClientCmdID gain operation idempotence. In other words,
// clients can submit the same command multiple times and always
// receive the same response. This is common on retries over flaky
// networks. However, the system imposes a limit on how long
// idempotence is provided. Retries over an hour old are not
// guaranteed idempotence and may be executed more than once with
// potentially different results.
//
// ClientCmdID contains the client's timestamp and a client-generated
// random number. The client Timestamp is specified in unix
// nanoseconds and is used for some uniqueness but also to provide a
// rough ordering of requests, useful for data locality on the
// server. The Random is specified for additional uniqueness.
// NOTE: An accurate time signal IS NOT required for correctness.
type ClientCmdID struct {
	WallTime int64 // Nanoseconds since Unix epoch
	Random   int64
}

// IsEmpty returns true if the client command ID has zero values.
func (ccid ClientCmdID) IsEmpty() bool {
	return ccid.WallTime == 0 && ccid.Random == 0
}

// Request is an interface providing access to all requests'
// header structs.
type Request interface {
	Header() *RequestHeader
}

// Response is an interface providing access to all responses' header
// structs.
type Response interface {
	Header() *ResponseHeader
}

// RequestHeader is supplied with every storage node request.
type RequestHeader struct {
	// Timestamp specifies time at which read or writes should be
	// performed. If the timestamp is set to zero value, its value
	// is initialized to the wall time of the receiving node.
	Timestamp hlc.HLTimestamp
	// CmdID is optionally specified for request idempotence
	// (i.e. replay protection).
	CmdID ClientCmdID

	// The following values are set internally and should not be set
	// manually.

	// The key for request. If the request operates on a range, this
	// represents the starting key for the range.
	Key engine.Key
	// End key is empty if request spans only a single key.
	EndKey engine.Key
	// User is the originating user. Used to lookup priority when
	// scheduling queued operations at target node.
	User string
	// Replica specifies the destination for the request. See config.go.
	Replica Replica
	// TxID is set non-empty if a transaction is underway. Empty string
	// to start a new transaction.
	TxID string
}

// Header implements the Request interface by returning itself.
func (rh *RequestHeader) Header() *RequestHeader {
	return rh
}

// ResponseHeader is returned with every storage node response.
type ResponseHeader struct {
	// Error is non-nil if an error occurred.
	Error error
	// Timestamp specifies time at which read or write actually was
	// performed. In the case of both reads and writes, if the supplied
	// timestamp is 0, the node servicing the request will use its
	// timestamp for the operation and its value will be set
	// here. Additionally, in the case of writes, this value may be
	// increased from the timestamp passed with the RequestHeader when
	// the key being written was either read or updated more recently.
	Timestamp hlc.HLTimestamp
	// TxID is non-empty if a transaction is underway.
	TxID string
}

// Header implements the Response interface by returning itself.
func (rh *ResponseHeader) Header() *ResponseHeader {
	return rh
}

// A ContainsRequest is arguments to the Contains() method.
type ContainsRequest struct {
	RequestHeader
}

// A ContainsResponse is the return value of the Contains() method.
type ContainsResponse struct {
	ResponseHeader
	Exists bool
}

// A GetRequest is arguments to the Get() method.
type GetRequest struct {
	RequestHeader
}

// A GetResponse is the return value from the Get() method.
// If the key doesn't exist, returns nil for Value.Bytes.
type GetResponse struct {
	ResponseHeader
	Value engine.Value
}

// A PutRequest is arguments to the Put() method.
type PutRequest struct {
	RequestHeader
	Value engine.Value // The value to put
}

// A PutResponse is the return value from the Put() method.
type PutResponse struct {
	ResponseHeader
}

// A ConditionalPutRequest is arguments to the ConditionalPut()
// method.
// - Returns true and sets value if ExpValue equals existing value.
// - If key doesn't exist and ExpValue is empty, sets value.
// - Otherwise, returns error.
type ConditionalPutRequest struct {
	RequestHeader
	Value    engine.Value // The value to put
	ExpValue engine.Value // ExpValue.Bytes empty to test for non-existence
}

// A ConditionalPutResponse is the return value from the
// ConditionalPut() method.
type ConditionalPutResponse struct {
	ResponseHeader
	ActualValue *engine.Value // ActualValue.Bytes set if conditional put failed
}

// An IncrementRequest is arguments to the Increment() method.
// It increments the value for key, interpreting the existing value as a
// varint64.
type IncrementRequest struct {
	RequestHeader
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
}

// A DeleteResponse is the return value from the Delete() method.
type DeleteResponse struct {
	ResponseHeader
}

// A DeleteRangeRequest is arguments to the DeleteRange method. It
// specifies the range of keys to delete.
type DeleteRangeRequest struct {
	RequestHeader
	// If 0, *all* entries between Key (inclusive) and EndKey (exclusive) are deleted.
	MaxEntriesToDelete int64 // Must be >= 0
}

// A DeleteRangeResponse is the return value from the DeleteRange()
// method.
type DeleteRangeResponse struct {
	ResponseHeader
	NumDeleted int64 // Number of entries removed
}

// A ScanRequest is arguments to the Scan() method. It specifies the
// start and end keys for the scan and the maximum number of results.
type ScanRequest struct {
	RequestHeader
	MaxResults int64 // Must be > 0
}

// A ScanResponse is the return value from the Scan() method.
type ScanResponse struct {
	ResponseHeader
	Rows []engine.KeyValue // Empty if no rows were scanned
}

// An EndTransactionRequest is arguments to the EndTransaction() method.
// It specifies whether to commit or roll back an extant transaction.
type EndTransactionRequest struct {
	RequestHeader
	Commit bool // False to abort and rollback
}

// An EndTransactionResponse is the return value from the
// EndTransaction() method. It specifies the commit timestamp for the
// final transaction (all writes will have this timestamp). It further
// specifies the commit wait, which is the remaining time the client
// MUST wait before signalling completion of the transaction to another
// distributed node to maintain consistency.
type EndTransactionResponse struct {
	ResponseHeader
	CommitTimestamp hlc.HLTimestamp
	CommitWait      int64 // Remaining with (us)
}

// An AccumulateTSRequest is arguments to the AccumulateTS() method.
// It specifies the key at which to accumulate TS values, and the
// time series counts for this discrete time interval.
type AccumulateTSRequest struct {
	RequestHeader
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
	MaxResults int64 // Maximum results to return; must be > 0
}

// A ReapQueueResponse is the return value from the ReapQueue() method.
type ReapQueueResponse struct {
	ResponseHeader
	Messages []engine.Value
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
	Message engine.Value // Message value to delivery to inbox
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
}

// An InternalRangeLookupResponse is the return value from the
// InternalRangeLookup() method. It returns the metadata for the
// range where the key resides. When looking up 1-level metadata,
// it returns the info for the range containing the 2-level metadata
// for the key. And when looking up 2-level metadata, it returns the
// info for the range possibly containing the actual key and its value.
type InternalRangeLookupResponse struct {
	ResponseHeader
	EndKey engine.Key // The key in datastore whose value is the Range object.
	Range  RangeDescriptor
}
