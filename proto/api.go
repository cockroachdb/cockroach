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
	// for a key, Put & Get will return errors; only Increment will
	// continue to be a valid command. The value must be deleted before
	// it can be reset using Put.
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
)

type stringSet map[string]struct{}

func (s stringSet) keys() []string {
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	return keys
}

// TODO(spencer): replaces these individual maps with a bitmask or
//   equivalent listing each method's attributes.

// AllMethods specifies the complete set of methods.
var AllMethods = stringSet{
	Contains:              struct{}{},
	Get:                   struct{}{},
	Put:                   struct{}{},
	ConditionalPut:        struct{}{},
	Increment:             struct{}{},
	Delete:                struct{}{},
	DeleteRange:           struct{}{},
	Scan:                  struct{}{},
	EndTransaction:        struct{}{},
	ReapQueue:             struct{}{},
	EnqueueUpdate:         struct{}{},
	EnqueueMessage:        struct{}{},
	AdminSplit:            struct{}{},
	Batch:                 struct{}{},
	InternalHeartbeatTxn:  struct{}{},
	InternalPushTxn:       struct{}{},
	InternalResolveIntent: struct{}{},
	InternalSnapshotCopy:  struct{}{},
	InternalMerge:         struct{}{},
}

// PublicMethods specifies the set of methods accessible via the
// public key-value API.
var PublicMethods = stringSet{
	Contains:       struct{}{},
	Get:            struct{}{},
	Put:            struct{}{},
	ConditionalPut: struct{}{},
	Increment:      struct{}{},
	Delete:         struct{}{},
	DeleteRange:    struct{}{},
	Scan:           struct{}{},
	EndTransaction: struct{}{},
	ReapQueue:      struct{}{},
	EnqueueUpdate:  struct{}{},
	EnqueueMessage: struct{}{},
	Batch:          struct{}{},
	AdminSplit:     struct{}{},
}

// InternalMethods specifies the set of methods accessible only
// via the internal node RPC API.
var InternalMethods = stringSet{
	InternalHeartbeatTxn:  struct{}{},
	InternalPushTxn:       struct{}{},
	InternalResolveIntent: struct{}{},
	InternalSnapshotCopy:  struct{}{},
	InternalMerge:         struct{}{},
}

// ReadMethods specifies the set of methods which read and return data.
var ReadMethods = stringSet{
	Contains:             struct{}{},
	Get:                  struct{}{},
	ConditionalPut:       struct{}{},
	Increment:            struct{}{},
	Scan:                 struct{}{},
	ReapQueue:            struct{}{},
	InternalRangeLookup:  struct{}{},
	InternalSnapshotCopy: struct{}{},
}

// WriteMethods specifies the set of methods which write data.
var WriteMethods = stringSet{
	Put:                   struct{}{},
	ConditionalPut:        struct{}{},
	Increment:             struct{}{},
	Delete:                struct{}{},
	DeleteRange:           struct{}{},
	EndTransaction:        struct{}{},
	ReapQueue:             struct{}{},
	EnqueueUpdate:         struct{}{},
	EnqueueMessage:        struct{}{},
	Batch:                 struct{}{},
	InternalHeartbeatTxn:  struct{}{},
	InternalPushTxn:       struct{}{},
	InternalResolveIntent: struct{}{},
	InternalMerge:         struct{}{},
}

// TxnMethods specifies the set of methods which leave key intents
// during transactions.
var TxnMethods = stringSet{
	Put:            struct{}{},
	ConditionalPut: struct{}{},
	Increment:      struct{}{},
	Delete:         struct{}{},
	DeleteRange:    struct{}{},
	ReapQueue:      struct{}{},
	EnqueueUpdate:  struct{}{},
	EnqueueMessage: struct{}{},
}

// adminMethods specifies the set of methods which are neither
// read-only nor read-write commands but instead execute directly on
// the Raft leader.
var adminMethods = stringSet{
	AdminSplit: struct{}{},
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

// MethodForRequest returns the method name corresponding to the type
// of the request.
func MethodForRequest(req Request) (string, error) {
	switch req.(type) {
	case *ContainsRequest:
		return Contains, nil
	case *GetRequest:
		return Get, nil
	case *PutRequest:
		return Put, nil
	case *ConditionalPutRequest:
		return ConditionalPut, nil
	case *IncrementRequest:
		return Increment, nil
	case *DeleteRequest:
		return Delete, nil
	case *DeleteRangeRequest:
		return DeleteRange, nil
	case *ScanRequest:
		return Scan, nil
	case *EndTransactionRequest:
		return EndTransaction, nil
	case *ReapQueueRequest:
		return ReapQueue, nil
	case *EnqueueUpdateRequest:
		return EnqueueUpdate, nil
	case *EnqueueMessageRequest:
		return EnqueueMessage, nil
	case *BatchRequest:
		return Batch, nil
	case *AdminSplitRequest:
		return AdminSplit, nil
	case *InternalHeartbeatTxnRequest:
		return InternalHeartbeatTxn, nil
	case *InternalPushTxnRequest:
		return InternalPushTxn, nil
	case *InternalResolveIntentRequest:
		return InternalResolveIntent, nil
	case *InternalSnapshotCopyRequest:
		return InternalSnapshotCopy, nil
	case *InternalMergeRequest:
		return InternalMerge, nil
	}
	return "", util.Errorf("unhandled request %T", req)
}

// CreateArgsAndReply returns allocated request and response pairs
// according to the specified method.
func CreateArgsAndReply(method string) (args Request, reply Response, err error) {
	if args, err = CreateArgs(method); err != nil {
		return
	}
	reply, err = CreateReply(method)
	return
}

// CreateArgs returns an allocated request according to the specified method.
func CreateArgs(method string) (Request, error) {
	switch method {
	case Contains:
		return &ContainsRequest{}, nil
	case Get:
		return &GetRequest{}, nil
	case Put:
		return &PutRequest{}, nil
	case ConditionalPut:
		return &ConditionalPutRequest{}, nil
	case Increment:
		return &IncrementRequest{}, nil
	case Delete:
		return &DeleteRequest{}, nil
	case DeleteRange:
		return &DeleteRangeRequest{}, nil
	case Scan:
		return &ScanRequest{}, nil
	case EndTransaction:
		return &EndTransactionRequest{}, nil
	case ReapQueue:
		return &ReapQueueRequest{}, nil
	case EnqueueUpdate:
		return &EnqueueUpdateRequest{}, nil
	case EnqueueMessage:
		return &EnqueueMessageRequest{}, nil
	case Batch:
		return &BatchRequest{}, nil
	case AdminSplit:
		return &AdminSplitRequest{}, nil
	case InternalHeartbeatTxn:
		return &InternalHeartbeatTxnRequest{}, nil
	case InternalPushTxn:
		return &InternalPushTxnRequest{}, nil
	case InternalResolveIntent:
		return &InternalResolveIntentRequest{}, nil
	case InternalSnapshotCopy:
		return &InternalSnapshotCopyRequest{}, nil
	case InternalMerge:
		return &InternalMergeRequest{}, nil
	}
	return nil, util.Errorf("unhandled method %s", method)
}

// CreateReply returns an allocated response according to the specified method.
func CreateReply(method string) (Response, error) {
	switch method {
	case Contains:
		return &ContainsResponse{}, nil
	case Get:
		return &GetResponse{}, nil
	case Put:
		return &PutResponse{}, nil
	case ConditionalPut:
		return &ConditionalPutResponse{}, nil
	case Increment:
		return &IncrementResponse{}, nil
	case Delete:
		return &DeleteResponse{}, nil
	case DeleteRange:
		return &DeleteRangeResponse{}, nil
	case Scan:
		return &ScanResponse{}, nil
	case EndTransaction:
		return &EndTransactionResponse{}, nil
	case ReapQueue:
		return &ReapQueueResponse{}, nil
	case EnqueueUpdate:
		return &EnqueueUpdateResponse{}, nil
	case EnqueueMessage:
		return &EnqueueMessageResponse{}, nil
	case Batch:
		return &BatchResponse{}, nil
	case AdminSplit:
		return &AdminSplitResponse{}, nil
	case InternalHeartbeatTxn:
		return &InternalHeartbeatTxnResponse{}, nil
	case InternalPushTxn:
		return &InternalPushTxnResponse{}, nil
	case InternalResolveIntent:
		return &InternalResolveIntentResponse{}, nil
	case InternalSnapshotCopy:
		return &InternalSnapshotCopyResponse{}, nil
	case InternalMerge:
		return &InternalMergeResponse{}, nil
	}
	return nil, util.Errorf("unhandled method %s", method)
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
	switch {
	case rh.Error.Generic != nil:
		return rh.Error.Generic
	case rh.Error.NotLeader != nil:
		return rh.Error.NotLeader
	case rh.Error.RangeNotFound != nil:
		return rh.Error.RangeNotFound
	case rh.Error.RangeKeyMismatch != nil:
		return rh.Error.RangeKeyMismatch
	case rh.Error.ReadWithinUncertaintyInterval != nil:
		return rh.Error.ReadWithinUncertaintyInterval
	case rh.Error.TransactionAborted != nil:
		return rh.Error.TransactionAborted
	case rh.Error.TransactionPush != nil:
		return rh.Error.TransactionPush
	case rh.Error.TransactionRetry != nil:
		return rh.Error.TransactionRetry
	case rh.Error.TransactionStatus != nil:
		return rh.Error.TransactionStatus
	case rh.Error.WriteIntent != nil:
		return rh.Error.WriteIntent
	case rh.Error.WriteTooOld != nil:
		return rh.Error.WriteTooOld
	case rh.Error.ReadWithinUncertaintyInterval != nil:
		return rh.Error.ReadWithinUncertaintyInterval
	case rh.Error.OpRequiresTxn != nil:
		return rh.Error.OpRequiresTxn
	default:
		return nil
	}
}

// SetGoError converts the specified type into either one of the proto-
// defined error types or into a GenericError for all other Go errors.
func (rh *ResponseHeader) SetGoError(err error) {
	if err == nil {
		rh.Error = nil
		return
	}
	switch t := err.(type) {
	case *NotLeaderError:
		rh.Error = &Error{NotLeader: t}
	case *RangeNotFoundError:
		rh.Error = &Error{RangeNotFound: t}
	case *RangeKeyMismatchError:
		rh.Error = &Error{RangeKeyMismatch: t}
	case *ReadWithinUncertaintyIntervalError:
		rh.Error = &Error{ReadWithinUncertaintyInterval: t}
	case *TransactionAbortedError:
		rh.Error = &Error{TransactionAborted: t}
	case *TransactionPushError:
		rh.Error = &Error{TransactionPush: t}
	case *TransactionRetryError:
		rh.Error = &Error{TransactionRetry: t}
	case *TransactionStatusError:
		rh.Error = &Error{TransactionStatus: t}
	case *WriteIntentError:
		rh.Error = &Error{WriteIntent: t}
	case *WriteTooOldError:
		rh.Error = &Error{WriteTooOld: t}
	case *OpRequiresTxnError:
		rh.Error = &Error{OpRequiresTxn: t}
	default:
		var canRetry bool
		if r, ok := err.(util.Retryable); ok {
			canRetry = r.CanRetry()
		}
		rh.Error = &Error{
			Generic: &GenericError{
				Message:   err.Error(),
				Retryable: canRetry,
			},
		}
	}
}

// Verify verifies the integrity of the get response value.
func (gr *GetResponse) Verify(req Request) error {
	if gr.Value != nil {
		return gr.Value.Verify(req.Header().Key)
	}
	return nil
}

// Verify verifies the integrity of the conditional put response's
// actual value, if not nil.
func (cpr *ConditionalPutResponse) Verify(req Request) error {
	if cpr.ActualValue != nil {
		return cpr.ActualValue.Verify(req.Header().Key)
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
