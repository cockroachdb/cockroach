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
	"reflect"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/util"
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
	// args.RequestHeader.Key and args.RequestHeader.EndKey.
	DeleteRange = "DeleteRange"
	// Scan fetches the values for all keys which fall between
	// args.RequestHeader.Key and args.RequestHeader.EndKey.
	Scan = "Scan"
	// BeginTransaction starts a transaction by initializing a new
	// Transaction proto using the contents of the request. Note that this
	// method does not call through to the key value interface but instead
	// services it directly, as creating a new transaction requires only
	// access to the node's clock. Nothing must be read or written.
	BeginTransaction = "BeginTransaction"
	// EndTransaction either commits or aborts an ongoing transaction.
	EndTransaction = "EndTransaction"
	// AccumulateTS is used to efficiently accumulate a time series of
	// int64 quantities representing discrete subtimes. For example, a
	// key/value might represent a minute of data. Each would contain 60
	// int64 counts, each representing a second.
	AccumulateTS = "AccumulateTS"
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
	BeginTransaction:      struct{}{},
	EndTransaction:        struct{}{},
	AccumulateTS:          struct{}{},
	ReapQueue:             struct{}{},
	EnqueueUpdate:         struct{}{},
	EnqueueMessage:        struct{}{},
	AdminSplit:            struct{}{},
	InternalEndTxn:        struct{}{},
	InternalHeartbeatTxn:  struct{}{},
	InternalPushTxn:       struct{}{},
	InternalResolveIntent: struct{}{},
	InternalSnapshotCopy:  struct{}{},
}

// PublicMethods specifies the set of methods accessible via the
// public key-value API.
var PublicMethods = stringSet{
	Contains:         struct{}{},
	Get:              struct{}{},
	Put:              struct{}{},
	ConditionalPut:   struct{}{},
	Increment:        struct{}{},
	Delete:           struct{}{},
	DeleteRange:      struct{}{},
	Scan:             struct{}{},
	BeginTransaction: struct{}{},
	EndTransaction:   struct{}{},
	AccumulateTS:     struct{}{},
	ReapQueue:        struct{}{},
	EnqueueUpdate:    struct{}{},
	EnqueueMessage:   struct{}{},
	AdminSplit:       struct{}{},
}

// InternalMethods specifies the set of methods accessible only
// via the internal node RPC API.
var InternalMethods = stringSet{
	InternalEndTxn:        struct{}{},
	InternalHeartbeatTxn:  struct{}{},
	InternalPushTxn:       struct{}{},
	InternalResolveIntent: struct{}{},
	InternalSnapshotCopy:  struct{}{},
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
	AccumulateTS:          struct{}{},
	ReapQueue:             struct{}{},
	EnqueueUpdate:         struct{}{},
	EnqueueMessage:        struct{}{},
	InternalEndTxn:        struct{}{},
	InternalHeartbeatTxn:  struct{}{},
	InternalPushTxn:       struct{}{},
	InternalResolveIntent: struct{}{},
}

// TxnMethods specifies the set of methods which may be part of a
// transaction.
var TxnMethods = stringSet{
	Contains:       struct{}{},
	Get:            struct{}{},
	Put:            struct{}{},
	ConditionalPut: struct{}{},
	Increment:      struct{}{},
	Delete:         struct{}{},
	DeleteRange:    struct{}{},
	Scan:           struct{}{},
	AccumulateTS:   struct{}{},
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

// CreateArgsAndReply returns allocated request and response pairs
// according to the specified method.
func CreateArgsAndReply(method string) (Request, Response, error) {
	switch method {
	case Contains:
		return &ContainsRequest{}, &ContainsResponse{}, nil
	case Get:
		return &GetRequest{}, &GetResponse{}, nil
	case Put:
		return &PutRequest{}, &PutResponse{}, nil
	case ConditionalPut:
		return &ConditionalPutRequest{}, &ConditionalPutResponse{}, nil
	case Increment:
		return &IncrementRequest{}, &IncrementResponse{}, nil
	case Delete:
		return &DeleteRequest{}, &DeleteResponse{}, nil
	case DeleteRange:
		return &DeleteRangeRequest{}, &DeleteRangeResponse{}, nil
	case Scan:
		return &ScanRequest{}, &ScanResponse{}, nil
	case BeginTransaction:
		return &BeginTransactionRequest{}, &BeginTransactionResponse{}, nil
	case EndTransaction:
		return &EndTransactionRequest{}, &EndTransactionResponse{}, nil
	case AccumulateTS:
		return &AccumulateTSRequest{}, &AccumulateTSResponse{}, nil
	case ReapQueue:
		return &ReapQueueRequest{}, &ReapQueueResponse{}, nil
	case EnqueueUpdate:
		return &EnqueueUpdateRequest{}, &EnqueueUpdateResponse{}, nil
	case EnqueueMessage:
		return &EnqueueMessageRequest{}, &EnqueueMessageResponse{}, nil
	case AdminSplit:
		return &AdminSplitRequest{}, &AdminSplitResponse{}, nil
	case InternalEndTxn:
		return &InternalEndTxnRequest{}, &InternalEndTxnResponse{}, nil
	case InternalHeartbeatTxn:
		return &InternalHeartbeatTxnRequest{}, &InternalHeartbeatTxnResponse{}, nil
	case InternalPushTxn:
		return &InternalPushTxnRequest{}, &InternalPushTxnResponse{}, nil
	case InternalResolveIntent:
		return &InternalResolveIntentRequest{}, &InternalResolveIntentResponse{}, nil
	case InternalSnapshotCopy:
		return &InternalSnapshotCopyRequest{}, &InternalSnapshotCopyResponse{}, nil
	}
	return nil, nil, util.Errorf("unhandled method %s", method)
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

// NewReply constructs a new reply element that is compatible with the
// supplied channel.
func NewReply(replyChanI interface{}) Response {
	// TODO(pmattis): The reflection sort of sucks, but at least it is
	// localized to this one file.
	return reflect.New(reflect.TypeOf(replyChanI).Elem().Elem()).Interface().(Response)
}

// SendReply sends the supplied reply to the reply channel.
func SendReply(replyChanI interface{}, reply Response) {
	reflect.ValueOf(replyChanI).Send(reflect.ValueOf(reply))
}
