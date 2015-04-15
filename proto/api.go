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

import (
	"log"

	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
)

// TODO(spencer): change these string constants into a type.
const (
	// Contains determines whether the KV map contains the specified key.
	Contains = "Contains"
	// Get fetches the value for a key from the KV map, respecting a
	// possibly historical timestamp. If the timestamp is 0, returns
	// the most recent value.
	Get = "Get"
	// Put sets the value for a key at the specified timestamp. If the
	// timestamp is 0, the value is set with the current time as timestamp.
	Put = "Put"
	// ConditionalPut sets the value for a key if the existing value
	// matches the value specified in the request. Specifying a null value
	// for existing means the value must not yet exist.
	ConditionalPut = "ConditionalPut"
	// Increment increments the value at the specified key. Once called
	// for a key, Put & ConditionalPut will return errors; only
	// Increment will continue to be a valid command. The value must be
	// deleted before it can be reset using Put.
	Increment = "Increment"
	// Delete removes the value for the specified key.
	Delete = "Delete"
	// DeleteRange removes all values for keys which fall between
	// args.RequestHeader.Key and args.RequestHeader.EndKey, with
	// the latter endpoint excluded.
	DeleteRange = "DeleteRange"
	// Scan fetches the values for all keys which fall between
	// args.RequestHeader.Key and args.RequestHeader.EndKey, with
	// the latter endpoint excluded.
	Scan = "Scan"
	// EndTransaction either commits or aborts an ongoing transaction.
	EndTransaction = "EndTransaction"
	// ReapQueue scans and deletes messages from a recipient message
	// queue. ReapQueueRequest invocations must be part of an extant
	// transaction or they fail. Returns the reaped queue messsages, up to
	// the requested maximum. If fewer than the maximum were returned,
	// then the queue is empty.
	ReapQueue = "ReapQueue"
	// EnqueueUpdate enqueues an update for eventual execution.
	EnqueueUpdate = "EnqueueUpdate"
	// EnqueueMessage enqueues a message for delivery to an inbox.
	EnqueueMessage = "EnqueueMessage"
	// Batch executes a set of commands in parallel.
	Batch = "Batch"
	// AdminSplit is called to coordinate a split of a range.
	AdminSplit = "AdminSplit"
	// AdminMerge is called to coordinate a merge of two adjacent ranges.
	AdminMerge = "AdminMerge"
)

type stringSet map[string]struct{}

func (s stringSet) keys() []string {
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	return keys
}

// A RaftID is a unique ID associated to a Raft consensus group.
type RaftID int64

// TODO(spencer): replaces these individual maps with a bitmask or
//   equivalent listing each method's attributes.

// AllMethods specifies the complete set of methods.
var AllMethods = stringSet{
	Contains:              {},
	Get:                   {},
	Put:                   {},
	ConditionalPut:        {},
	Increment:             {},
	Delete:                {},
	DeleteRange:           {},
	Scan:                  {},
	EndTransaction:        {},
	ReapQueue:             {},
	EnqueueUpdate:         {},
	EnqueueMessage:        {},
	AdminSplit:            {},
	AdminMerge:            {},
	Batch:                 {},
	InternalHeartbeatTxn:  {},
	InternalGC:            {},
	InternalPushTxn:       {},
	InternalResolveIntent: {},
	InternalMerge:         {},
	InternalTruncateLog:   {},
}

// PublicMethods specifies the set of methods accessible via the
// public key-value API.
var PublicMethods = stringSet{
	Contains:       {},
	Get:            {},
	Put:            {},
	ConditionalPut: {},
	Increment:      {},
	Delete:         {},
	DeleteRange:    {},
	Scan:           {},
	EndTransaction: {},
	ReapQueue:      {},
	EnqueueUpdate:  {},
	EnqueueMessage: {},
	Batch:          {},
	AdminSplit:     {},
	AdminMerge:     {},
}

// InternalMethods specifies the set of methods accessible only
// via the internal node RPC API.
var InternalMethods = stringSet{
	InternalHeartbeatTxn:  {},
	InternalGC:            {},
	InternalPushTxn:       {},
	InternalResolveIntent: {},
	InternalMerge:         {},
	InternalTruncateLog:   {},
}

// ReadMethods specifies the set of methods which read and return data.
var ReadMethods = stringSet{
	Contains:            {},
	Get:                 {},
	ConditionalPut:      {},
	Increment:           {},
	Scan:                {},
	ReapQueue:           {},
	InternalRangeLookup: {},
}

// WriteMethods specifies the set of methods which write data.
var WriteMethods = stringSet{
	Put:                   {},
	ConditionalPut:        {},
	Increment:             {},
	Delete:                {},
	DeleteRange:           {},
	EndTransaction:        {},
	ReapQueue:             {},
	EnqueueUpdate:         {},
	EnqueueMessage:        {},
	Batch:                 {},
	InternalHeartbeatTxn:  {},
	InternalGC:            {},
	InternalPushTxn:       {},
	InternalResolveIntent: {},
	InternalMerge:         {},
	InternalTruncateLog:   {},
}

// TxnMethods specifies the set of methods which leave key intents
// during transactions.
var TxnMethods = stringSet{
	Put:            {},
	ConditionalPut: {},
	Increment:      {},
	Delete:         {},
	DeleteRange:    {},
	ReapQueue:      {},
	EnqueueUpdate:  {},
	EnqueueMessage: {},
}

// adminMethods specifies the set of methods which are neither
// read-only nor read-write commands but instead execute directly on
// the Raft leader.
var adminMethods = stringSet{
	AdminSplit: {},
	AdminMerge: {},
}

// NeedReadPerm returns true if the specified method requires read permissions.
func NeedReadPerm(method string) bool {
	_, ok := ReadMethods[method]
	return ok
}

// NeedWritePerm returns true if the specified method requires write permissions.
func NeedWritePerm(method string) bool {
	_, ok := WriteMethods[method]
	return ok
}

// NeedAdminPerm returns true if the specified method requires admin permissions.
func NeedAdminPerm(method string) bool {
	_, ok := adminMethods[method]
	return ok
}

// IsPublic returns true if the specified method is in the public
// key-value API.
func IsPublic(method string) bool {
	_, ok := PublicMethods[method]
	return ok
}

// IsInternal returns true if the specified method is only available
// via the internal node RPC API.
func IsInternal(method string) bool {
	_, ok := InternalMethods[method]
	return ok
}

// IsReadOnly returns true if the specified method only requires read
// permissions.
func IsReadOnly(method string) bool {
	return NeedReadPerm(method) && !NeedWritePerm(method)
}

// IsReadWrite returns true if the specified method requires write
// permissions.
func IsReadWrite(method string) bool {
	return NeedWritePerm(method)
}

// IsAdmin returns true if the specified method requires admin
// permissions.
func IsAdmin(method string) bool {
	return NeedAdminPerm(method)
}

// IsTransactional returns true if the specified method can be part of
// a transaction.
func IsTransactional(method string) bool {
	_, ok := TxnMethods[method]
	return ok
}

// GetArgs returns a GetRequest object initialized to get the
// value at key.
func GetArgs(key Key) *GetRequest {
	return &GetRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
	}
}

// IncrementArgs returns an IncrementRequest object initialized to
// increment the value at key by increment.
func IncrementArgs(key Key, increment int64) *IncrementRequest {
	return &IncrementRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Increment: increment,
	}
}

// PutArgs returns a PutRequest object initialized to put value
// as a byte slice at key.
func PutArgs(key Key, valueBytes []byte) *PutRequest {
	value := Value{Bytes: valueBytes}
	value.InitChecksum(key)
	return &PutRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Value: value,
	}
}

// DeleteArgs returns a DeleteRequest object initialized to delete
// the value at key.
func DeleteArgs(key Key) *DeleteRequest {
	return &DeleteRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
	}
}

// DeleteRangeArgs returns a DeleteRangeRequest object initialized to delete
// the values in the given key range (excluding the endpoint).
func DeleteRangeArgs(startKey, endKey Key) *DeleteRangeRequest {
	return &DeleteRangeRequest{
		RequestHeader: RequestHeader{
			Key:    startKey,
			EndKey: endKey,
		},
	}
}

// ScanArgs returns a ScanRequest object initialized to scan
// from start to end keys with max results.
func ScanArgs(key, endKey Key, maxResults int64) *ScanRequest {
	return &ScanRequest{
		RequestHeader: RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
		MaxResults: maxResults,
	}
}

// IsEmpty returns true if the client command ID has zero values.
func (ccid ClientCmdID) IsEmpty() bool {
	return ccid.WallTime == 0 && ccid.Random == 0
}

// Request is an interface for RPC requests.
type Request interface {
	gogoproto.Message
	// Header returns the request header.
	Header() *RequestHeader
	// Method returns the request method name.
	Method() string
	// CreateReply creates a new response object.
	CreateReply() Response
}

// Response is an interface for RPC responses.
type Response interface {
	gogoproto.Message
	// Header returns the response header.
	Header() *ResponseHeader
	// Verify verifies response integrity, as applicable.
	Verify(req Request) error
}

// Combinable is implemented by response types whose corresponding
// requests may cross range boundaries, such as Scan or DeleteRange.
// Combine() allows responses from individual ranges to be aggregated
// into a single one.
// It is not expected that Combine() perform any error checking; this
// should be done by the caller instead.
type Combinable interface {
	Combine(Response)
}

// Combine is used by range-spanning Response types (e.g. Scan or DeleteRange)
// to merge their headers.
func (rh *ResponseHeader) Combine(otherRH *ResponseHeader) {
	if rh != nil {
		if ts := otherRH.GetTimestamp(); rh.Timestamp.Less(ts) {
			rh.Timestamp = ts
		}
		if rh.Txn != nil && otherRH.GetTxn() == nil {
			rh.Txn = nil
		}
	}
}

// Combine implements the Combinable interface for ScanResponse.
func (sr *ScanResponse) Combine(c Response) {
	otherSR := c.(*ScanResponse)
	if sr != nil {
		sr.Rows = append(sr.Rows, otherSR.GetRows()...)
		sr.Header().Combine(otherSR.Header())
	}
}

// Combine implements the Combinable interface for DeleteRangeResponse.
func (dr *DeleteRangeResponse) Combine(c Response) {
	otherDR := c.(*DeleteRangeResponse)
	if dr != nil {
		dr.NumDeleted += otherDR.GetNumDeleted()
		dr.Header().Combine(otherDR.Header())
	}
}

// Combine implements the Combinable interface for
// InternalResolveIntentResponse.
func (rr *InternalResolveIntentResponse) Combine(c Response) {
	otherRR := c.(*InternalResolveIntentResponse)
	if rr != nil {
		rr.Header().Combine(otherRR.Header())
	}
}

// Header implements the Request interface for RequestHeader.
func (rh *RequestHeader) Header() *RequestHeader {
	return rh
}

// Header implements the Response interface for ResponseHeader.
func (rh *ResponseHeader) Header() *ResponseHeader {
	return rh
}

// Verify implements the Response interface for ResopnseHeader with a
// default noop. Individual response types should override this method
// if they contain checksummed data which can be verified.
func (rh *ResponseHeader) Verify(req Request) error {
	return nil
}

// GoError returns the non-nil error from the proto.Error union.
func (rh *ResponseHeader) GoError() error {
	if rh.Error == nil {
		return nil
	}
	if rh.Error.Detail == nil {
		return rh.Error
	}
	errVal := rh.Error.Detail.GetValue()
	if errVal == nil {
		// Unknown error detail; return the generic error.
		return rh.Error
	}
	err := errVal.(error)
	// Make sure that the flags in the generic portion of the error
	// match the methods of the specific error type.
	if rh.Error.Retryable {
		if r, ok := err.(util.Retryable); !ok || !r.CanRetry() {
			log.Fatalf("inconsistent error proto; expected %T to be retryable", err)
		}
	}
	if r, ok := err.(TransactionRestartError); ok {
		if r.CanRestartTransaction() != rh.Error.TransactionRestart {
			log.Fatalf("inconsistent error proto; expected %T to have restart mode %v",
				err, rh.Error.TransactionRestart)
		}
	} else {
		// Error type doesn't implement TransactionRestartError, so expect it to have the default.
		if rh.Error.TransactionRestart != TransactionRestart_ABORT {
			log.Fatalf("inconsistent error proto; expected %T to have restart mode ABORT", err)
		}
	}
	return err
}

// SetGoError converts the specified type into either one of the proto-
// defined error types or into a Error for all other Go errors.
func (rh *ResponseHeader) SetGoError(err error) {
	if err == nil {
		rh.Error = nil
		return
	}
	rh.Error = &Error{}
	rh.Error.Message = err.Error()
	if r, ok := err.(util.Retryable); ok {
		rh.Error.Retryable = r.CanRetry()
	}
	if r, ok := err.(TransactionRestartError); ok {
		rh.Error.TransactionRestart = r.CanRestartTransaction()
	}
	// If the specific error type exists in the detail union, set it.
	detail := &ErrorDetail{}
	if detail.SetValue(err) {
		rh.Error.Detail = detail
	}
}

// Verify verifies the integrity of the get response value.
func (gr *GetResponse) Verify(req Request) error {
	if gr.Value != nil {
		return gr.Value.Verify(req.Header().Key)
	}
	return nil
}

// Verify verifies the integrity of every value returned in the scan.
func (sr *ScanResponse) Verify(req Request) error {
	for _, kv := range sr.Rows {
		if err := kv.Value.Verify(kv.Key); err != nil {
			return err
		}
	}
	return nil
}

// Add adds a request to the batch request. The batch inherits
// the key range of the first request added to it.
//
// TODO(spencer): batches should include a list of key ranges
//   representing the constituent requests.
func (br *BatchRequest) Add(args Request) {
	union := RequestUnion{}
	union.SetValue(args)
	if br.Key == nil {
		br.Key = args.Header().Key
		br.EndKey = args.Header().EndKey
	}
	br.Requests = append(br.Requests, union)
}

// Add adds a response to the batch response.
func (br *BatchResponse) Add(reply Response) {
	union := ResponseUnion{}
	union.SetValue(reply)
	br.Responses = append(br.Responses, union)
}

// Bounded is implemented by request types which have a bounded number of
// result rows, such as Scan.
type Bounded interface {
	GetBound() int64
	SetBound(bound int64)
}

// GetBound returns the MaxResults field in ScanRequest.
func (sr *ScanRequest) GetBound() int64 {
	return sr.GetMaxResults()
}

// SetBound sets the MaxResults field in ScanRequest.
func (sr *ScanRequest) SetBound(bound int64) {
	sr.MaxResults = bound
}

// Countable is implemented by response types which have a number of
// result rows, such as Scan.
type Countable interface {
	Count() int64
}

// Count returns the number of rows in ScanResponse.
func (sr *ScanResponse) Count() int64 {
	return int64(len(sr.Rows))
}

// Method implements the Request interface.
func (*ContainsRequest) Method() string { return Contains }

// Method implements the Request interface.
func (*GetRequest) Method() string { return Get }

// Method implements the Request interface.
func (*PutRequest) Method() string { return Put }

// Method implements the Request interface.
func (*ConditionalPutRequest) Method() string { return ConditionalPut }

// Method implements the Request interface.
func (*IncrementRequest) Method() string { return Increment }

// Method implements the Request interface.
func (*DeleteRequest) Method() string { return Delete }

// Method implements the Request interface.
func (*DeleteRangeRequest) Method() string { return DeleteRange }

// Method implements the Request interface.
func (*ScanRequest) Method() string { return Scan }

// Method implements the Request interface.
func (*EndTransactionRequest) Method() string { return EndTransaction }

// Method implements the Request interface.
func (*ReapQueueRequest) Method() string { return ReapQueue }

// Method implements the Request interface.
func (*EnqueueUpdateRequest) Method() string { return EnqueueUpdate }

// Method implements the Request interface.
func (*EnqueueMessageRequest) Method() string { return EnqueueMessage }

// Method implements the Request interface.
func (*BatchRequest) Method() string { return Batch }

// Method implements the Request interface.
func (*AdminSplitRequest) Method() string { return AdminSplit }

// Method implements the Request interface.
func (*AdminMergeRequest) Method() string { return AdminMerge }

// Method implements the Request interface.
func (*InternalHeartbeatTxnRequest) Method() string { return InternalHeartbeatTxn }

// Method implements the Request interface.
func (*InternalGCRequest) Method() string { return InternalGC }

// Method implements the Request interface.
func (*InternalPushTxnRequest) Method() string { return InternalPushTxn }

// Method implements the Request interface.
func (*InternalRangeLookupRequest) Method() string { return InternalRangeLookup }

// Method implements the Request interface.
func (*InternalResolveIntentRequest) Method() string { return InternalResolveIntent }

// Method implements the Request interface.
func (*InternalMergeRequest) Method() string { return InternalMerge }

// Method implements the Request interface.
func (*InternalLeaderLeaseRequest) Method() string { return InternalLeaderLease }

// Method implements the Request interface.
func (*InternalTruncateLogRequest) Method() string { return InternalTruncateLog }

// CreateReply implements the Request interface.
func (*ContainsRequest) CreateReply() Response { return &ContainsResponse{} }

// CreateReply implements the Request interface.
func (*GetRequest) CreateReply() Response { return &GetResponse{} }

// CreateReply implements the Request interface.
func (*PutRequest) CreateReply() Response { return &PutResponse{} }

// CreateReply implements the Request interface.
func (*ConditionalPutRequest) CreateReply() Response { return &ConditionalPutResponse{} }

// CreateReply implements the Request interface.
func (*IncrementRequest) CreateReply() Response { return &IncrementResponse{} }

// CreateReply implements the Request interface.
func (*DeleteRequest) CreateReply() Response { return &DeleteResponse{} }

// CreateReply implements the Request interface.
func (*DeleteRangeRequest) CreateReply() Response { return &DeleteRangeResponse{} }

// CreateReply implements the Request interface.
func (*ScanRequest) CreateReply() Response { return &ScanResponse{} }

// CreateReply implements the Request interface.
func (*EndTransactionRequest) CreateReply() Response { return &EndTransactionResponse{} }

// CreateReply implements the Request interface.
func (*ReapQueueRequest) CreateReply() Response { return &ReapQueueResponse{} }

// CreateReply implements the Request interface.
func (*EnqueueUpdateRequest) CreateReply() Response { return &EnqueueUpdateResponse{} }

// CreateReply implements the Request interface.
func (*EnqueueMessageRequest) CreateReply() Response { return &EnqueueMessageResponse{} }

// CreateReply implements the Request interface.
func (*BatchRequest) CreateReply() Response { return &BatchResponse{} }

// CreateReply implements the Request interface.
func (*AdminSplitRequest) CreateReply() Response { return &AdminSplitResponse{} }

// CreateReply implements the Request interface.
func (*AdminMergeRequest) CreateReply() Response { return &AdminMergeResponse{} }

// CreateReply implements the Request interface.
func (*InternalHeartbeatTxnRequest) CreateReply() Response { return &InternalHeartbeatTxnResponse{} }

// CreateReply implements the Request interface.
func (*InternalGCRequest) CreateReply() Response { return &InternalGCResponse{} }

// CreateReply implements the Request interface.
func (*InternalPushTxnRequest) CreateReply() Response { return &InternalPushTxnResponse{} }

// CreateReply implements the Request interface.
func (*InternalRangeLookupRequest) CreateReply() Response { return &InternalRangeLookupResponse{} }

// CreateReply implements the Request interface.
func (*InternalResolveIntentRequest) CreateReply() Response { return &InternalResolveIntentResponse{} }

// CreateReply implements the Request interface.
func (*InternalMergeRequest) CreateReply() Response { return &InternalMergeResponse{} }

// CreateReply implements the Request interface.
func (*InternalTruncateLogRequest) CreateReply() Response { return &InternalTruncateLogResponse{} }

// CreateReply implements the Request interface.
func (*InternalLeaderLeaseRequest) CreateReply() Response { return &InternalLeaderLeaseResponse{} }
