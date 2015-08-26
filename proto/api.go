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
	isReverse              // reverse commands traverse ranges in descending direction
)

// IsAdmin returns true if the request is an admin request.
func IsAdmin(args Request) bool {
	if br, ok := args.(*BatchRequest); ok {
		return br.IsAdmin()
	}
	return (args.flags() & isAdmin) != 0
}

// IsRead returns true if the request is a read request.
func IsRead(args Request) bool {
	if br, ok := args.(*BatchRequest); ok {
		return br.IsRead()
	}
	return (args.flags() & isRead) != 0
}

// IsWrite returns true if the request is a write request.
func IsWrite(args Request) bool {
	if br, ok := args.(*BatchRequest); ok {
		return br.IsWrite()
	}
	return (args.flags() & isWrite) != 0
}

// IsReadOnly returns true if the request is read-only.
func IsReadOnly(args Request) bool {
	if br, ok := args.(*BatchRequest); ok {
		return br.IsReadOnly()
	}
	return IsRead(args) && !IsWrite(args)
}

// IsReverse returns true if the request is reverse.
func IsReverse(args Request) bool {
	if br, ok := args.(*BatchRequest); ok {
		return br.IsReverse()
	}
	return (args.flags() & isReverse) != 0
}

// IsTransactionWrite returns true if the request produces write
// intents when used within a transaction.
func IsTransactionWrite(args Request) bool {
	if br, ok := args.(*BatchRequest); ok {
		return br.IsTransactionWrite()
	}
	return (args.flags() & isTxnWrite) != 0
}

// IsRange returns true if the operation is range-based and must include
// a start and an end key.
func IsRange(args Request) bool {
	if br, ok := args.(*BatchRequest); ok {
		return br.IsRange()
	}
	return (args.flags() & isRange) != 0
}

// IsAdmin returns true iff the BatchRequest contains an admin request.
func (br *BatchRequest) IsAdmin() bool {
	for _, arg := range br.Requests {
		if IsAdmin(arg.GetValue().(Request)) {
			return true
		}
	}
	return false
}

// IsRead returns true if all requests within are flagged as reading data.
func (br *BatchRequest) IsRead() bool {
	for i := range br.Requests {
		if !IsRead(br.Requests[i].GetValue().(Request)) {
			return false
		}
	}
	return true
}

// IsWrite returns true iff the BatchRequest contains a write.
func (br *BatchRequest) IsWrite() bool {
	for _, arg := range br.Requests {
		if IsWrite(arg.GetValue().(Request)) {
			return true
		}
	}
	return false
}

// IsReadOnly returns true if all requests within are read-only.
func (br *BatchRequest) IsReadOnly() bool {
	for i := range br.Requests {
		if !IsReadOnly(br.Requests[i].GetValue().(Request)) {
			return false
		}
	}
	return true
}

// IsReverse returns true iff the BatchRequest contains a reverse request.
func (br *BatchRequest) IsReverse() bool {
	if len(br.Requests) == 0 {
		panic("empty batch")
	}
	reverse := IsReverse(br.Requests[0].GetValue().(Request))
	for _, arg := range br.Requests[1:] {
		if req := arg.GetValue().(Request); IsReverse(req) != reverse {
			panic(fmt.Sprintf("argument mixes reverse and non-reverse: %T", req))
		}
	}
	return reverse
}

// IsTransactionWrite returns true iff the BatchRequest contains a txn write.
func (br *BatchRequest) IsTransactionWrite() bool {
	for _, arg := range br.Requests {
		if IsTransactionWrite(arg.GetValue().(Request)) {
			return true
		}
	}
	return false
}

// IsRange returns true iff the BatchRequest contains a range request.
func (br *BatchRequest) IsRange() bool {
	return (br.flags() & isRange) != 0
}

// GetArg returns the first request of the given type, if possible.
func (br *BatchRequest) GetArg(method Method) (Request, bool) {
	for _, arg := range br.Requests {
		if r, ok := GetArg(arg.GetValue().(Request), method); ok {
			return r, ok
		}
	}
	return nil, false
}

// GetIntents returns a slice of key pairs corresponding to transactional writes
// contained in the batch.
// TODO(tschottdorf) return Intent, not [2]Key.
func (br *BatchRequest) GetIntents() []Intent {
	var intents []Intent
	for _, arg := range br.Requests {
		intents = append(intents, GetIntents(arg.GetValue().(Request))...)
	}
	return intents
}

// GetArg returns true iff the request is or contains a request of the given
// type, along with the first match.
func GetArg(args Request, method Method) (Request, bool) {
	if br, ok := args.(*BatchRequest); ok {
		return br.GetArg(method)
	}
	if args.Method() == method {
		return args, true
	}
	return nil, false
}

// GetIntents returns a slice of key pairs corresponding to transactional writes
// contained in the request.
func GetIntents(args Request) []Intent {
	if br, ok := args.(*BatchRequest); ok {
		return br.GetIntents()
	}
	if !IsTransactionWrite(args) {
		return nil
	}
	return []Intent{{Key: args.Header().Key, EndKey: args.Header().EndKey}}
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
func (br *BatchResponse) ResetAll() {
	if br == nil {
		return
	}
	for _, rsp := range br.Responses {
		// TODO(tschottdorf) `rsp.Reset()` doesn't actually clear it out, I'm
		// not sure why though.
		rsp.GetValue().(Response).Reset()
	}
}

// Combine implements the Combinable interface. It combines each slot of the
// given request into the corresponding slot of the base response. The number
// of slots must be equal and the respective slots must be combinable.
func (br *BatchResponse) Combine(c Response) error {
	otherBatch, ok := c.(*BatchResponse)
	if !ok {
		return combineError(br, c)
	}
	if len(otherBatch.Responses) != len(br.Responses) {
		return errors.New("unable to combine batch responses of different length")
	}
	for i, l := 0, len(br.Responses); i < l; i++ {
		valLeft := br.Responses[i].GetValue()
		valRight := otherBatch.Responses[i].GetValue()
		args, lOK := valLeft.(Combinable)
		reply, rOK := valRight.(Combinable)
		if lOK && rOK {
			if err := args.Combine(reply.(Response)); err != nil {
				return err
			}
			continue
		}
		// If our local slot is a NoopResponse, then whatever the other batch
		// has is the result.
		if _, ok := valLeft.(*NoopResponse); ok {
			br.Responses[i] = otherBatch.Responses[i]
		} else if _, ok := valRight.(*NoopResponse); !ok {
			return fmt.Errorf("unable to combine batches with incompatible entries %T and %T", valLeft, valRight)
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

// Verify verifies the integrity of every value returned in the reverse scan.
func (sr *ReverseScanResponse) Verify(req Request) error {
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
		panic(fmt.Sprintf("unable to add %T to batch request", args))
	}
	// TODO(tschottdorf): make this obsolete.
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
func (*BatchRequest) CreateReply() Response {
	bReply := &BatchResponse{}
	return bReply
	// TODO(tschottdorf): remove. It doesn't seem to make sense to pre-allocate
	// responses because that behaves in a weird way with Merge() and, after all,
	// we want to move away from the Call pattern.
	// l := len(bArgs.Requests)
	// bReply.Responses = make([]ResponseUnion, l, l)
	// for i, arg := range bArgs.Requests {
	// 	if !bReply.Responses[i].SetValue(arg.GetValue().(Request).CreateReply()) {
	// 		panic("illegal value for batch reply")
	// 	}
	// }
	// return bReply
}

func (*GetRequest) flags() int                { return isRead }
func (*PutRequest) flags() int                { return isWrite | isTxnWrite }
func (*ConditionalPutRequest) flags() int     { return isRead | isWrite | isTxnWrite }
func (*IncrementRequest) flags() int          { return isRead | isWrite | isTxnWrite }
func (*DeleteRequest) flags() int             { return isWrite | isTxnWrite }
func (*DeleteRangeRequest) flags() int        { return isWrite | isTxnWrite | isRange }
func (*ScanRequest) flags() int               { return isRead | isRange }
func (*ReverseScanRequest) flags() int        { return isRead | isRange | isReverse }
func (*EndTransactionRequest) flags() int     { return isWrite }
func (*AdminSplitRequest) flags() int         { return isAdmin }
func (*AdminMergeRequest) flags() int         { return isAdmin }
func (*HeartbeatTxnRequest) flags() int       { return isWrite }
func (*GCRequest) flags() int                 { return isWrite | isRange }
func (*PushTxnRequest) flags() int            { return isWrite }
func (*RangeLookupRequest) flags() int        { return isRead }
func (*ResolveIntentRequest) flags() int      { return isWrite }
func (*ResolveIntentRangeRequest) flags() int { return isWrite | isRange }
func (*NoopRequest) flags() int               { return 0 } // slightly special
func (*MergeRequest) flags() int              { return isWrite }
func (*TruncateLogRequest) flags() int        { return isWrite }
func (*LeaderLeaseRequest) flags() int        { return isWrite }

// TODO(tschottdorf): BatchRequest shouldn't be a "Request" any more soon.
func (br *BatchRequest) flags() int {
	var flags int
	for _, arg := range br.Requests {
		flags |= arg.GetValue().(Request).flags()
	}
	return flags
}
