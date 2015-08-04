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
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/util/retry"
	gogoproto "github.com/gogo/protobuf/proto"
)

// A RangeID is a unique ID associated to a Raft consensus group.
type RangeID int64

// IsEmpty returns true if the client command ID has zero values.
func (ccid ClientCmdID) IsEmpty() bool {
	return ccid.WallTime == 0 && ccid.Random == 0
}

// TraceID implements tracer.Traceable and returns the ClientCmdID in the
// format "c<WallTime>.<Random>".
func (ccid ClientCmdID) TraceID() string {
	return fmt.Sprint("c", ccid.WallTime, ".", ccid.Random)
}

// TraceName implements tracer.Traceable.
func (ccid ClientCmdID) TraceName() string {
	// 1 day = 8.64*10E13ns
	return fmt.Sprint("c", ccid.WallTime%10E14, ".", ccid.Random%100)
}

const (
	isAdmin    = 1 << iota // admin cmds don't go through raft, but run on leader
	isRead                 // read-only cmds don't go through raft, but may run on leader
	isWrite                // write cmds go through raft and must be proposed on leader
	isTxnWrite             // txn write cmds start heartbeat and are marked for intent resolution
	isRange                // range commands may span multiple keys
)

// IsAdmin returns true if the request requires admin permissions.
func IsAdmin(args Request) bool {
	return (args.flags() & isAdmin) != 0
}

// IsRead returns true if the request requires read permissions.
func IsRead(args Request) bool {
	return (args.flags() & isRead) != 0
}

// IsWrite returns true if the request requires write permissions.
func IsWrite(args Request) bool {
	return (args.flags() & isWrite) != 0
}

// IsReadOnly returns true if the request only requires read
// permissions.
func IsReadOnly(args Request) bool {
	return IsRead(args) && !IsWrite(args)
}

// IsTransactionWrite returns true if the request produces write
// intents when used within a transaction.
func IsTransactionWrite(args Request) bool {
	return (args.flags() & isTxnWrite) != 0
}

// IsRange returns true if the operation is range-based and must include
// a start and an end key.
func IsRange(args Request) bool {
	return (args.flags() & isRange) != 0
}

// Request is an interface for RPC requests.
type Request interface {
	gogoproto.Message
	// Header returns the request header.
	Header() *RequestHeader
	// Method returns the request method.
	Method() Method
	// CreateReply creates a new response object.
	CreateReply() Response
	flags() int
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

// GetOrCreateCmdID returns the request header's command ID if available.
// Otherwise, creates a new ClientCmdID, initialized with current time
// and random salt.
func (rh *RequestHeader) GetOrCreateCmdID(walltime int64) (cmdID ClientCmdID) {
	if !rh.CmdID.IsEmpty() {
		cmdID = rh.CmdID
	} else {
		cmdID = ClientCmdID{
			WallTime: walltime,
			Random:   rand.Int63(),
		}
	}
	return
}

// TraceID implements tracer.Traceable by returning the first nontrivial
// TraceID of the Transaction and CmdID.
func (rh *RequestHeader) TraceID() string {
	if r := rh.Txn.TraceID(); r != "" {
		return r
	}
	return rh.CmdID.TraceID()
}

// TraceName implements tracer.Traceable and behaves like TraceID, but using
// the TraceName of the object delegated to.
func (rh *RequestHeader) TraceName() string {
	if r := rh.Txn.TraceID(); r != "" {
		return rh.Txn.TraceName()
	}
	return rh.CmdID.TraceName()
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
// InternalResolveIntentRangeResponse.
func (rr *InternalResolveIntentRangeResponse) Combine(c Response) {
	otherRR := c.(*InternalResolveIntentRangeResponse)
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
		if r, ok := err.(retry.Retryable); !ok || !r.CanRetry() {
			panic(fmt.Sprintf("inconsistent error proto; expected %T to be retryable", err))
		}
	}
	if r, ok := err.(TransactionRestartError); ok {
		if r.CanRestartTransaction() != rh.Error.TransactionRestart {
			panic(fmt.Sprintf("inconsistent error proto; expected %T to have restart mode %v",
				err, rh.Error.TransactionRestart))
		}
	} else {
		// Error type doesn't implement TransactionRestartError, so expect it to have the default.
		if rh.Error.TransactionRestart != TransactionRestart_ABORT {
			panic(fmt.Sprintf("inconsistent error proto; expected %T to have restart mode ABORT", err))
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
	rh.Error.SetResponseGoError(err)
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
	if !union.SetValue(args) {
		// TODO(tschottdorf) evaluate whether this should return an error.
		panic(fmt.Sprintf("unable to add %T to batch request", args))
	}
	if br.Key == nil {
		br.Key = args.Header().Key
		br.EndKey = args.Header().EndKey
	}
	br.Requests = append(br.Requests, union)
}

// Add adds a response to the batch response.
func (br *BatchResponse) Add(reply Response) {
	union := ResponseUnion{}
	if !union.SetValue(reply) {
		// TODO(tschottdorf) evaluate whether this should return an error.
		panic(fmt.Sprintf("unable to add %T to batch response", reply))
	}
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
func (*GetRequest) Method() Method { return Get }

// Method implements the Request interface.
func (*PutRequest) Method() Method { return Put }

// Method implements the Request interface.
func (*ConditionalPutRequest) Method() Method { return ConditionalPut }

// Method implements the Request interface.
func (*IncrementRequest) Method() Method { return Increment }

// Method implements the Request interface.
func (*DeleteRequest) Method() Method { return Delete }

// Method implements the Request interface.
func (*DeleteRangeRequest) Method() Method { return DeleteRange }

// Method implements the Request interface.
func (*ScanRequest) Method() Method { return Scan }

// Method implements the Request interface.
func (*EndTransactionRequest) Method() Method { return EndTransaction }

// Method implements the Request interface.
func (*BatchRequest) Method() Method { return Batch }

// Method implements the Request interface.
func (*AdminSplitRequest) Method() Method { return AdminSplit }

// Method implements the Request interface.
func (*AdminMergeRequest) Method() Method { return AdminMerge }

// Method implements the Request interface.
func (*InternalHeartbeatTxnRequest) Method() Method { return InternalHeartbeatTxn }

// Method implements the Request interface.
func (*InternalGCRequest) Method() Method { return InternalGC }

// Method implements the Request interface.
func (*InternalPushTxnRequest) Method() Method { return InternalPushTxn }

// Method implements the Request interface.
func (*InternalRangeLookupRequest) Method() Method { return InternalRangeLookup }

// Method implements the Request interface.
func (*InternalResolveIntentRequest) Method() Method { return InternalResolveIntent }

// Method implements the Request interface.
func (*InternalResolveIntentRangeRequest) Method() Method { return InternalResolveIntentRange }

// Method implements the Request interface.
func (*InternalMergeRequest) Method() Method { return InternalMerge }

// Method implements the Request interface.
func (*InternalLeaderLeaseRequest) Method() Method { return InternalLeaderLease }

// Method implements the Request interface.
func (*InternalTruncateLogRequest) Method() Method { return InternalTruncateLog }

// Method implements the Request interface.
func (*InternalBatchRequest) Method() Method { return InternalBatch }

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
func (*InternalResolveIntentRangeRequest) CreateReply() Response {
	return &InternalResolveIntentRangeResponse{}
}

// CreateReply implements the Request interface.
func (*InternalMergeRequest) CreateReply() Response { return &InternalMergeResponse{} }

// CreateReply implements the Request interface.
func (*InternalTruncateLogRequest) CreateReply() Response { return &InternalTruncateLogResponse{} }

// CreateReply implements the Request interface.
func (*InternalLeaderLeaseRequest) CreateReply() Response { return &InternalLeaderLeaseResponse{} }

// CreateReply implements the Request interface.
func (*InternalBatchRequest) CreateReply() Response { return &InternalBatchResponse{} }

func (*GetRequest) flags() int                        { return isRead }
func (*PutRequest) flags() int                        { return isWrite | isTxnWrite }
func (*ConditionalPutRequest) flags() int             { return isRead | isWrite | isTxnWrite }
func (*IncrementRequest) flags() int                  { return isRead | isWrite | isTxnWrite }
func (*DeleteRequest) flags() int                     { return isWrite | isTxnWrite }
func (*DeleteRangeRequest) flags() int                { return isWrite | isTxnWrite | isRange }
func (*ScanRequest) flags() int                       { return isRead | isRange }
func (*EndTransactionRequest) flags() int             { return isWrite }
func (*BatchRequest) flags() int                      { return isWrite }
func (*AdminSplitRequest) flags() int                 { return isAdmin }
func (*AdminMergeRequest) flags() int                 { return isAdmin }
func (*InternalHeartbeatTxnRequest) flags() int       { return isWrite }
func (*InternalGCRequest) flags() int                 { return isWrite | isRange }
func (*InternalPushTxnRequest) flags() int            { return isWrite }
func (*InternalRangeLookupRequest) flags() int        { return isRead }
func (*InternalResolveIntentRequest) flags() int      { return isWrite }
func (*InternalResolveIntentRangeRequest) flags() int { return isWrite | isRange }
func (*InternalMergeRequest) flags() int              { return isWrite }
func (*InternalTruncateLogRequest) flags() int        { return isWrite }
func (*InternalLeaderLeaseRequest) flags() int        { return isWrite }
func (*InternalBatchRequest) flags() int              { return isWrite }
