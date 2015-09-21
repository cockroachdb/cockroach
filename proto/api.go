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
	"errors"
	"fmt"
	"math/rand"
	"strings"

	gogoproto "github.com/gogo/protobuf/proto"
)

// A RangeID is a unique ID associated to a Raft consensus group.
type RangeID int64

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

// IsAdmin returns true if the request is an admin request.
func IsAdmin(args Request) bool {
	return (args.flags() & isAdmin) != 0
}

// IsRead returns true if the request is a read request.
func IsRead(args Request) bool {
	return (args.flags() & isRead) != 0
}

// IsWrite returns true if the request is a write request.
func IsWrite(args Request) bool {
	return (args.flags() & isWrite) != 0
}

// IsReadOnly returns true iff the request is read-only.
func IsReadOnly(args Request) bool {
	return IsRead(args) && !IsWrite(args)
}

// IsWriteOnly returns true if the request only requires write permissions.
func IsWriteOnly(args Request) bool {
	return !IsRead(args) && IsWrite(args)
}

// IsReverse returns true if the request is reverse.
func IsReverse(args Request) bool {
	return (args.flags() & isReverse) != 0
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

// IsAdmin returns true iff the BatchRequest contains an admin request.
func (ba *BatchRequest) IsAdmin() bool {
	for _, arg := range ba.Requests {
		if IsAdmin(arg.GetInner()) {
			return true
		}
	}
	return false
}

// IsRead returns true if all requests within are flagged as reading data.
func (ba *BatchRequest) IsRead() bool {
	for i := range ba.Requests {
		if !IsRead(ba.Requests[i].GetInner()) {
			return false
		}
	}
	return true
}

// IsWrite returns true iff the BatchRequest contains a write.
func (ba *BatchRequest) IsWrite() bool {
	for _, arg := range ba.Requests {
		if IsWrite(arg.GetInner()) {
			return true
		}
	}
	return false
}

// IsReadOnly returns true if all requests within are read-only.
// TODO(tschottdorf): unify with proto.IsReadOnly
func (ba *BatchRequest) IsReadOnly() bool {
	for i := range ba.Requests {
		if !IsReadOnly(ba.Requests[i].GetInner()) {
			return false
		}
	}
	return true
}

// IsReverse returns true iff the BatchRequest contains a reverse request.
func (ba *BatchRequest) IsReverse() bool {
	if len(ba.Requests) == 0 {
		panic("empty batch")
	}
	reverse := IsReverse(ba.Requests[0].GetInner())
	for _, arg := range ba.Requests[1:] {
		if req := arg.GetInner(); IsReverse(req) != reverse {
			panic(fmt.Sprintf("argument mixes reverse and non-reverse: %T", req))
		}
	}
	return reverse
}

// IsTransactionWrite returns true iff the BatchRequest contains a txn write.
func (ba *BatchRequest) IsTransactionWrite() bool {
	for _, arg := range ba.Requests {
		if IsTransactionWrite(arg.GetInner()) {
			return true
		}
	}
	return false
}

// IsRange returns true iff the BatchRequest contains a range request.
func (ba *BatchRequest) IsRange() bool {
	return (ba.flags() & isRange) != 0
}

// GetArg returns the first request of the given type, if possible.
func (ba *BatchRequest) GetArg(method Method) (Request, bool) {
	for _, arg := range ba.Requests {
		if req := arg.GetInner(); req.Method() == method {
			return req, true
		}
	}
	return nil, false
}

// First returns the first response of the given type, if possible.
func (ba *BatchResponse) First() Response {
	if len(ba.Responses) > 0 {
		return ba.Responses[0].GetInner()
	}
	return nil
}

// GetIntents returns a slice of key pairs corresponding to transactional writes
// contained in the batch.
// TODO(tschottdorf): use keys.Span here instead of []Intent. Actually
// Intent should be Intents = {Txn, []Span} so that a []Span can
// be turned into Intents easily by just adding a Txn.
func (ba *BatchRequest) GetIntents() []Intent {
	var intents []Intent
	for _, arg := range ba.Requests {
		req := arg.GetInner()
		if !IsTransactionWrite(req) {
			continue
		}
		h := req.Header()
		intents = append(intents, Intent{Key: h.Key, EndKey: h.EndKey})
	}
	return intents
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
type Combinable interface {
	Combine(Response) error
}

// GetUser implements userRequest.
// KV messages are always sent by the node user.
func (rh *RequestHeader) GetUser() string {
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

// ResetAll resets all the contained requests to their original state.
func (ba *BatchResponse) ResetAll() {
	if ba == nil {
		return
	}
	for _, rsp := range ba.Responses {
		// TODO(tschottdorf) `rsp.Reset()` isn't enough because rsp
		// isn't a pointer.
		rsp.GetInner().Reset()
	}
}

// Combine implements the Combinable interface. It combines each slot of the
// given request into the corresponding slot of the base response. The number
// of slots must be equal and the respective slots must be combinable.
// TODO(tschottdorf): write tests.
func (ba *BatchResponse) Combine(c Response) error {
	otherBatch, ok := c.(*BatchResponse)
	if !ok {
		return combineError(ba, c)
	}
	if len(otherBatch.Responses) != len(ba.Responses) {
		return errors.New("unable to combine batch responses of different length")
	}
	for i, l := 0, len(ba.Responses); i < l; i++ {
		valLeft := ba.Responses[i].GetInner()
		valRight := otherBatch.Responses[i].GetInner()
		args, lOK := valLeft.(Combinable)
		reply, rOK := valRight.(Combinable)
		if lOK && rOK {
			if err := args.Combine(reply.(Response)); err != nil {
				return err
			}
			continue
		}
		// If our slot is a NoopResponse, then whatever the other batch has is
		// the result. Note that the result can still be a NoopResponse, to be
		// filled in by a future Combine().
		if _, ok := valLeft.(*NoopResponse); ok {
			ba.Responses[i] = otherBatch.Responses[i]
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

// Add adds a request to the batch request. The batch key range is
// expanded to include the key ranges of all requests which it comprises.
func (ba *BatchRequest) Add(args Request) {
	union := RequestUnion{}
	if !union.SetValue(args) {
		panic(fmt.Sprintf("unable to add %T to batch request", args))
	}

	h := args.Header()
	if ba.Key == nil || !ba.Key.Less(h.Key) {
		ba.Key = h.Key
	} else if ba.EndKey.Less(h.Key) && !ba.Key.Equal(h.Key) {
		ba.EndKey = h.Key
	}
	if ba.EndKey == nil || (h.EndKey != nil && ba.EndKey.Less(h.EndKey)) {
		ba.EndKey = h.EndKey
	}
	ba.Requests = append(ba.Requests, union)
}

// Add adds a response to the batch response.
func (ba *BatchResponse) Add(reply Response) {
	union := ResponseUnion{}
	if !union.SetValue(reply) {
		// TODO(tschottdorf) evaluate whether this should return an error.
		panic(fmt.Sprintf("unable to add %T to batch response", reply))
	}
	ba.Responses = append(ba.Responses, union)
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

// Method implements the Request interface.
func (*BatchRequest) Method() Method { return Batch }

// Methods returns a slice of the contained methods.
func (ba *BatchRequest) Methods() []Method {
	var res []Method
	for _, arg := range ba.Requests {
		res = append(res, arg.GetInner().Method())
	}
	return res
}

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

// CreateReply implements the Request interface.
func (*BatchRequest) CreateReply() Response { return &BatchResponse{} }

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

func (ba *BatchRequest) flags() int {
	var flags int
	for _, union := range ba.Requests {
		flags |= union.GetInner().flags()
	}
	return flags
}

// Split separate the requests contained in a batch so that each subset of
// requests can be executed by a Store (without changing order). In particular,
// Admin and EndTransaction requests are always singled out and mutating
// requests separated from reads.
func (ba BatchRequest) Split() [][]RequestUnion {
	compatible := func(exFlags, newFlags int) bool {
		// If no flags are set so far, everything goes.
		if exFlags == 0 {
			return true
		}
		if (newFlags & isAlone) != 0 {
			return false
		}
		// Otherwise, the flags below must remain the same
		// with the new request added.
		// Note that we're not checking isRead: The invariants we're
		// enforcing are that a batch can't mix non-writes with writes.
		// Checking isRead would ConditionalPut and Put to conflict,
		// which is not what we want.
		const mask = isWrite | isAdmin | isReverse
		return (mask & exFlags) == (mask & newFlags)
	}
	var parts [][]RequestUnion
	for len(ba.Requests) > 0 {
		part := ba.Requests
		var gFlags int
		for i, union := range ba.Requests {
			flags := union.GetInner().flags()
			if !compatible(gFlags, flags) {
				part = ba.Requests[:i]
				break
			}
			gFlags |= flags
		}
		parts = append(parts, part)
		ba.Requests = ba.Requests[len(part):]
	}
	return parts
}

// String gives a brief summary of the contained requests and keys in the batch.
// TODO(tschottdorf): the key range is useful information, but requires `keys`.
// See #2198.
func (ba BatchRequest) String() string {
	var str []string
	for _, arg := range ba.Requests {
		req := arg.GetInner()
		h := req.Header()
		str = append(str, fmt.Sprintf("%T [%s,%s)", req, h.Key, h.EndKey))
	}
	return strings.Join(str, ", ")
}
