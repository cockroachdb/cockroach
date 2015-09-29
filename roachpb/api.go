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

package roachpb

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/gogo/protobuf/proto"
)

// A RangeID is a unique ID associated to a Raft consensus group.
type RangeID int64

// String implements the fmt.Stringer interface.
func (r RangeID) String() string {
	return strconv.FormatInt(int64(r), 10)
}

// RangeIDSlice implements sort.Interface.
type RangeIDSlice []RangeID

func (r RangeIDSlice) Len() int           { return len(r) }
func (r RangeIDSlice) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r RangeIDSlice) Less(i, j int) bool { return r[i] < r[j] }

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
	isReverse              // reverse commands traverse ranges in descending direction
	isAlone                // requests which must be alone in a batch
)

// IsReadOnly returns true iff the request is read-only.
func IsReadOnly(args Request) bool {
	flags := args.flags()
	return (flags&isRead) != 0 && (flags&isWrite) == 0
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
	proto.Message
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
	proto.Message
	// Header returns the response header.
	Header() *ResponseHeader
	// Verify verifies response integrity, as applicable.
	Verify(req Request) error
}

// Combinable is implemented by response types whose corresponding
// requests may cross range boundaries, such as Scan or DeleteRange.
// Combine() allows responses from individual ranges to be aggregated
// into a single one.
type Combinable interface {
	Combine(Response) error
}

// TODO(marc): we should assert
// var _ security.RequestWithUser = &RequestHeader{}
// here, but we need to break cycles first.

// GetUser implements security.RequestWithUser.
// KV messages are always sent by the node user.
func (*RequestHeader) GetUser() string {
	// TODO(marc): we should use security.NodeUser here, but we need to break cycles first.
	return "node"
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

func combineError(a, b interface{}) error {
	return fmt.Errorf("illegal combination: (%T).Combine(%T)", a, b)
}

// Combine is used by range-spanning Response types (e.g. Scan or DeleteRange)
// to merge their headers.
func (rh *ResponseHeader) Combine(otherRH *ResponseHeader) error {
	if rh != nil {
		if ts := otherRH.GetTimestamp(); rh.Timestamp.Less(ts) {
			rh.Timestamp = ts
		}
		if rh.Txn != nil && otherRH.GetTxn() == nil {
			rh.Txn = nil
		}
	}
	return nil
}

// Combine implements the Combinable interface.
func (sr *ScanResponse) Combine(c Response) error {
	otherSR := c.(*ScanResponse)
	if sr != nil {
		sr.Rows = append(sr.Rows, otherSR.GetRows()...)
		if err := sr.Header().Combine(otherSR.Header()); err != nil {
			return err
		}
	}
	return nil
}

// Combine implements the Combinable interface.
func (sr *ReverseScanResponse) Combine(c Response) error {
	otherSR := c.(*ReverseScanResponse)
	if sr != nil {
		sr.Rows = append(sr.Rows, otherSR.GetRows()...)
		if err := sr.Header().Combine(otherSR.Header()); err != nil {
			return err
		}
	}
	return nil
}

// Combine implements the Combinable interface.
func (dr *DeleteRangeResponse) Combine(c Response) error {
	otherDR := c.(*DeleteRangeResponse)
	if dr != nil {
		dr.NumDeleted += otherDR.GetNumDeleted()
		if err := dr.Header().Combine(otherDR.Header()); err != nil {
			return err
		}
	}
	return nil
}

// Combine implements the Combinable interface.
func (rr *ResolveIntentRangeResponse) Combine(c Response) error {
	otherRR := c.(*ResolveIntentRangeResponse)
	if rr != nil {
		if err := rr.Header().Combine(otherRR.Header()); err != nil {
			return err
		}
	}
	return nil
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
	return rh.Error.GoError()
}

// SetGoError converts the specified type into either one of the proto-
// defined error types or into an Error for all other Go errors.
func (rh *ResponseHeader) SetGoError(err error) {
	if err == nil {
		rh.Error = nil
		return
	}
	rh.Error = &Error{}
	rh.Error.SetGoError(err)
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

// Verify verifies the integrity of every value returned in the reverse scan.
func (sr *ReverseScanResponse) Verify(req Request) error {
	for _, kv := range sr.Rows {
		if err := kv.Value.Verify(kv.Key); err != nil {
			return err
		}
	}
	return nil
}

// GetInner returns the Request contained in the union.
func (ru RequestUnion) GetInner() Request {
	return ru.GetValue().(Request)
}

// GetInner returns the Response contained in the union.
func (ru ResponseUnion) GetInner() Response {
	return ru.GetValue().(Response)
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

// GetBound returns the MaxResults field in ReverseScanRequest.
func (sr *ReverseScanRequest) GetBound() int64 {
	return sr.GetMaxResults()
}

// SetBound sets the MaxResults field in ReverseScanRequest.
func (sr *ReverseScanRequest) SetBound(bound int64) {
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

// Count returns the number of rows in ReverseScanResponse.
func (sr *ReverseScanResponse) Count() int64 {
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
func (*ReverseScanRequest) Method() Method { return ReverseScan }

// Method implements the Request interface.
func (*EndTransactionRequest) Method() Method { return EndTransaction }

// Method implements the Request interface.
func (*AdminSplitRequest) Method() Method { return AdminSplit }

// Method implements the Request interface.
func (*AdminMergeRequest) Method() Method { return AdminMerge }

// Method implements the Request interface.
func (*HeartbeatTxnRequest) Method() Method { return HeartbeatTxn }

// Method implements the Request interface.
func (*GCRequest) Method() Method { return GC }

// Method implements the Request interface.
func (*PushTxnRequest) Method() Method { return PushTxn }

// Method implements the Request interface.
func (*RangeLookupRequest) Method() Method { return RangeLookup }

// Method implements the Request interface.
func (*ResolveIntentRequest) Method() Method { return ResolveIntent }

// Method implements the Request interface.
func (*ResolveIntentRangeRequest) Method() Method { return ResolveIntentRange }

// Method implements the Request interface.
func (*NoopRequest) Method() Method { return Noop }

// Method implements the Request interface.
func (*MergeRequest) Method() Method { return Merge }

// Method implements the Request interface.
func (*TruncateLogRequest) Method() Method { return TruncateLog }

// Method implements the Request interface.
func (*LeaderLeaseRequest) Method() Method { return LeaderLease }

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
func (*ReverseScanRequest) CreateReply() Response { return &ReverseScanResponse{} }

// CreateReply implements the Request interface.
func (*EndTransactionRequest) CreateReply() Response { return &EndTransactionResponse{} }

// CreateReply implements the Request interface.
func (*AdminSplitRequest) CreateReply() Response { return &AdminSplitResponse{} }

// CreateReply implements the Request interface.
func (*AdminMergeRequest) CreateReply() Response { return &AdminMergeResponse{} }

// CreateReply implements the Request interface.
func (*HeartbeatTxnRequest) CreateReply() Response { return &HeartbeatTxnResponse{} }

// CreateReply implements the Request interface.
func (*GCRequest) CreateReply() Response { return &GCResponse{} }

// CreateReply implements the Request interface.
func (*PushTxnRequest) CreateReply() Response { return &PushTxnResponse{} }

// CreateReply implements the Request interface.
func (*RangeLookupRequest) CreateReply() Response { return &RangeLookupResponse{} }

// CreateReply implements the Request interface.
func (*ResolveIntentRequest) CreateReply() Response { return &ResolveIntentResponse{} }

// CreateReply implements the Request interface.
func (*ResolveIntentRangeRequest) CreateReply() Response {
	return &ResolveIntentRangeResponse{}
}

// CreateReply implements the Request interface.
func (*NoopRequest) CreateReply() Response { return &NoopResponse{} }

// CreateReply implements the Request interface.
func (*MergeRequest) CreateReply() Response { return &MergeResponse{} }

// CreateReply implements the Request interface.
func (*TruncateLogRequest) CreateReply() Response { return &TruncateLogResponse{} }

// CreateReply implements the Request interface.
func (*LeaderLeaseRequest) CreateReply() Response { return &LeaderLeaseResponse{} }

// NewGet returns a Request initialized to get the value at key.
func NewGet(key Key) Request {
	return &GetRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
	}
}

// NewIncrement returns a Request initialized to increment the value at
// key by increment.
func NewIncrement(key Key, increment int64) Request {
	return &IncrementRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Increment: increment,
	}
}

// NewPut returns a Request initialized to put the value at key.
func NewPut(key Key, value Value) Request {
	value.InitChecksum(key)
	return &PutRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Value: value,
	}
}

// NewConditionalPut returns a Request initialized to put value as a byte
// slice at key if the existing value at key equals expValueBytes.
func NewConditionalPut(key Key, value, expValue Value) Request {
	value.InitChecksum(key)
	var expValuePtr *Value
	if expValue.Bytes != nil {
		expValuePtr = &expValue
		expValue.InitChecksum(key)
	}
	return &ConditionalPutRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Value:    value,
		ExpValue: expValuePtr,
	}
}

// NewDelete returns a Request initialized to delete the value at key.
func NewDelete(key Key) Request {
	return &DeleteRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
	}
}

// NewDeleteRange returns a Request initialized to delete the values in
// the given key range (excluding the endpoint).
func NewDeleteRange(startKey, endKey Key) Request {
	return &DeleteRangeRequest{
		RequestHeader: RequestHeader{
			Key:    startKey,
			EndKey: endKey,
		},
	}
}

// NewScan returns a Request initialized to scan from start to end keys
// with max results.
func NewScan(key, endKey Key, maxResults int64) Request {
	return &ScanRequest{
		RequestHeader: RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
		MaxResults: maxResults,
	}
}

// NewReverseScan returns a Request initialized to reverse scan from end to
// start keys with max results.
func NewReverseScan(key, endKey Key, maxResults int64) Request {
	return &ReverseScanRequest{
		RequestHeader: RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
		MaxResults: maxResults,
	}
}

func (*GetRequest) flags() int                { return isRead }
func (*PutRequest) flags() int                { return isWrite | isTxnWrite }
func (*ConditionalPutRequest) flags() int     { return isRead | isWrite | isTxnWrite }
func (*IncrementRequest) flags() int          { return isRead | isWrite | isTxnWrite }
func (*DeleteRequest) flags() int             { return isWrite | isTxnWrite }
func (*DeleteRangeRequest) flags() int        { return isWrite | isTxnWrite | isRange }
func (*ScanRequest) flags() int               { return isRead | isRange }
func (*ReverseScanRequest) flags() int        { return isRead | isRange | isReverse }
func (*EndTransactionRequest) flags() int     { return isWrite | isAlone }
func (*AdminSplitRequest) flags() int         { return isAdmin | isAlone }
func (*AdminMergeRequest) flags() int         { return isAdmin | isAlone }
func (*HeartbeatTxnRequest) flags() int       { return isWrite }
func (*GCRequest) flags() int                 { return isWrite | isRange }
func (*PushTxnRequest) flags() int            { return isWrite }
func (*RangeLookupRequest) flags() int        { return isRead }
func (*ResolveIntentRequest) flags() int      { return isWrite }
func (*ResolveIntentRangeRequest) flags() int { return isWrite | isRange }
func (*NoopRequest) flags() int               { return isRead } // slightly special
func (*MergeRequest) flags() int              { return isWrite }
func (*TruncateLogRequest) flags() int        { return isWrite }
func (*LeaderLeaseRequest) flags() int        { return isWrite }
