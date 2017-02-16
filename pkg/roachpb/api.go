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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package roachpb

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/gogo/protobuf/proto"
	"github.com/rlmcpherson/s3gof3r"
)

// UserPriority is a custom type for transaction's user priority.
type UserPriority float64

func (up UserPriority) String() string {
	switch up {
	case MinUserPriority:
		return "LOW"
	case UnspecifiedUserPriority, NormalUserPriority:
		return "NORMAL"
	case MaxUserPriority:
		return "HIGH"
	default:
		return fmt.Sprintf("%g", float64(up))
	}
}

const (
	// MinUserPriority is the minimum allowed user priority.
	MinUserPriority = 0.001
	// UnspecifiedUserPriority means NormalUserPriority.
	UnspecifiedUserPriority = 0
	// NormalUserPriority is set to 1, meaning ops run through the database
	// are all given equal weight when a random priority is chosen. This can
	// be set specifically via client.NewDBWithPriority().
	NormalUserPriority = 1
	// MaxUserPriority is the maximum allowed user priority.
	MaxUserPriority = 1000
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

const (
	isAdmin    = 1 << iota // admin cmds don't go through raft, but run on lease holder
	isRead                 // read-only cmds don't go through raft, but may run on lease holder
	isWrite                // write cmds go through raft and must be proposed on lease holder
	isTxn                  // txn commands may be part of a transaction
	isTxnWrite             // txn write cmds start heartbeat and are marked for intent resolution
	isRange                // range commands may span multiple keys
	isReverse              // reverse commands traverse ranges in descending direction
	isAlone                // requests which must be alone in a batch
	// Some commands can skip interacting with the command queue and the timestamp
	// cache. For example, RequestLeaseRequest is sequenced exclusively by Raft.
	// These requests still have keys in their header, but those keys are used
	// exclusively for routing the request to the right range.
	isNonKV
	// Requests for acquiring a lease skip the (proposal-time) check that the
	// proposing replica has a valid lease.
	skipLeaseCheck
)

// GetTxnID returns the transaction ID if the header has a transaction
// or else nil.
func (h Header) GetTxnID() *uuid.UUID {
	if h.Txn == nil {
		return nil
	}
	return h.Txn.ID
}

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
	Header() Span
	// SetHeader sets the request header.
	SetHeader(Span)
	// Method returns the request method.
	Method() Method
	// ShallowCopy returns a shallow copy of the receiver.
	ShallowCopy() Request
	flags() int
}

// leaseRequestor is implemented by requests dealing with leases.
// Implementors return the previous lease at the time the request
// was proposed.
type leaseRequestor interface {
	prevLease() *Lease
}

var _ leaseRequestor = &RequestLeaseRequest{}

func (rlr *RequestLeaseRequest) prevLease() *Lease {
	return rlr.PrevLease
}

var _ leaseRequestor = &TransferLeaseRequest{}

func (tlr *TransferLeaseRequest) prevLease() *Lease {
	return tlr.PrevLease
}

// Response is an interface for RPC responses.
type Response interface {
	proto.Message
	// Header returns the response header.
	Header() ResponseHeader
	// SetHeader sets the response header.
	SetHeader(ResponseHeader)
	// Verify verifies response integrity, as applicable.
	Verify(req Request) error
}

// combinable is implemented by response types whose corresponding
// requests may cross range boundaries, such as Scan or DeleteRange.
// combine() allows responses from individual ranges to be aggregated
// into a single one.
type combinable interface {
	combine(combinable) error
}

// combine is used by range-spanning Response types (e.g. Scan or DeleteRange)
// to merge their headers.
func (rh *ResponseHeader) combine(otherRH ResponseHeader) error {
	if rh.Txn != nil && otherRH.Txn == nil {
		rh.Txn = nil
	}
	if rh.ResumeSpan != nil {
		panic(fmt.Sprintf("combining %+v with %+v", rh.ResumeSpan, otherRH.ResumeSpan))
	}
	rh.ResumeSpan = otherRH.ResumeSpan
	rh.NumKeys += otherRH.NumKeys
	rh.RangeInfos = append(rh.RangeInfos, otherRH.RangeInfos...)
	return nil
}

// combine implements the combinable interface.
func (sr *ScanResponse) combine(c combinable) error {
	otherSR := c.(*ScanResponse)
	if sr != nil {
		sr.Rows = append(sr.Rows, otherSR.Rows...)
		if err := sr.ResponseHeader.combine(otherSR.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &ScanResponse{}

// combine implements the combinable interface.
func (sr *ReverseScanResponse) combine(c combinable) error {
	otherSR := c.(*ReverseScanResponse)
	if sr != nil {
		sr.Rows = append(sr.Rows, otherSR.Rows...)
		if err := sr.ResponseHeader.combine(otherSR.Header()); err != nil {
			return err
		}

	}
	return nil
}

var _ combinable = &ReverseScanResponse{}

// combine implements the combinable interface.
func (dr *DeleteRangeResponse) combine(c combinable) error {
	otherDR := c.(*DeleteRangeResponse)
	if dr != nil {
		dr.Keys = append(dr.Keys, otherDR.Keys...)
		if err := dr.ResponseHeader.combine(otherDR.Header()); err != nil {
			return err
		}
	}
	return nil
}

// combine implements the combinable interface.
func (rr *ResolveIntentRangeResponse) combine(c combinable) error {
	otherRR := c.(*ResolveIntentRangeResponse)
	if rr != nil {
		if err := rr.ResponseHeader.combine(otherRR.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &ResolveIntentRangeResponse{}

// Combine implements the combinable interface.
func (cc *CheckConsistencyResponse) combine(c combinable) error {
	if cc != nil {
		otherCC := c.(*CheckConsistencyResponse)
		if err := cc.ResponseHeader.combine(otherCC.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &CheckConsistencyResponse{}

// Combine implements the combinable interface.
func (af *ChangeFrozenResponse) combine(c combinable) error {
	if af != nil {
		otherAF := c.(*ChangeFrozenResponse)
		if err := af.ResponseHeader.combine(otherAF.Header()); err != nil {
			return err
		}
		af.RangesAffected += otherAF.RangesAffected
		if otherAF.MinStartKey.Less(af.MinStartKey) {
			af.MinStartKey = otherAF.MinStartKey
		}
		for storeID, nodeID := range otherAF.Stores {
			if af.Stores == nil {
				af.Stores = make(map[StoreID]NodeID, len(otherAF.Stores))
			}
			af.Stores[storeID] = nodeID
		}
	}
	return nil
}

var _ combinable = &ChangeFrozenResponse{}

// Combine implements the combinable interface.
func (er *ExportResponse) combine(c combinable) error {
	if er != nil {
		otherER := c.(*ExportResponse)
		if err := er.ResponseHeader.combine(otherER.Header()); err != nil {
			return err
		}
		er.Files = append(er.Files, otherER.Files...)
	}
	return nil
}

var _ combinable = &ExportResponse{}

// Header implements the Request interface.
func (rh Span) Header() Span {
	return rh
}

// SetHeader implements the Request interface.
func (rh *Span) SetHeader(other Span) {
	*rh = other
}

// Header implements the Request interface.
func (*NoopRequest) Header() Span { panic("NoopRequest has no span") }

// SetHeader implements the Request interface.
func (*NoopRequest) SetHeader(_ Span) { panic("NoopRequest has no span") }

// SetHeader implements the Response interface.
func (rh *ResponseHeader) SetHeader(other ResponseHeader) {
	*rh = other
}

// Header implements the Response interface for ResponseHeader.
func (rh ResponseHeader) Header() ResponseHeader {
	return rh
}

// Header implements the Response interface.
func (*NoopResponse) Header() ResponseHeader { return ResponseHeader{} }

// SetHeader implements the Response interface.
func (*NoopResponse) SetHeader(_ ResponseHeader) {}

// Verify implements the Response interface for ResopnseHeader with a
// default noop. Individual response types should override this method
// if they contain checksummed data which can be verified.
func (rh *ResponseHeader) Verify(req Request) error {
	return nil
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

// Verify implements the Response interface.
func (*NoopResponse) Verify(_ Request) error {
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

// MustSetInner sets the Request contained in the union. It panics if the
// request is not recognized by the union type. The RequestUnion is reset
// before being repopulated.
func (ru *RequestUnion) MustSetInner(args Request) {
	ru.Reset()
	if !ru.SetValue(args) {
		panic(fmt.Sprintf("%T excludes %T", ru, args))
	}
}

// MustSetInner sets the Response contained in the union. It panics if the
// response is not recognized by the union type. The ResponseUnion is reset
// before being repopulated.
func (ru *ResponseUnion) MustSetInner(reply Response) {
	ru.Reset()
	if !ru.SetValue(reply) {
		panic(fmt.Sprintf("%T excludes %T", ru, reply))
	}
}

// Method implements the Request interface.
func (*GetRequest) Method() Method { return Get }

// Method implements the Request interface.
func (*PutRequest) Method() Method { return Put }

// Method implements the Request interface.
func (*ConditionalPutRequest) Method() Method { return ConditionalPut }

// Method implements the Request interface.
func (*InitPutRequest) Method() Method { return InitPut }

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
func (*CheckConsistencyRequest) Method() Method { return CheckConsistency }

// Method implements the Request interface.
func (*ChangeFrozenRequest) Method() Method { return ChangeFrozen }

// Method implements the Request interface.
func (*BeginTransactionRequest) Method() Method { return BeginTransaction }

// Method implements the Request interface.
func (*EndTransactionRequest) Method() Method { return EndTransaction }

// Method implements the Request interface.
func (*AdminSplitRequest) Method() Method { return AdminSplit }

// Method implements the Request interface.
func (*AdminMergeRequest) Method() Method { return AdminMerge }

// Method implements the Request interface.
func (*AdminTransferLeaseRequest) Method() Method { return AdminTransferLease }

// Method implements the Request interface.
func (*HeartbeatTxnRequest) Method() Method { return HeartbeatTxn }

// Method implements the Request interface.
func (*GCRequest) Method() Method { return GC }

// Method implements the Request interface.
func (*PushTxnRequest) Method() Method { return PushTxn }

// Method implements the Request interface.
func (*QueryTxnRequest) Method() Method { return QueryTxn }

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
func (*RequestLeaseRequest) Method() Method { return RequestLease }

// Method implements the Request interface.
func (*TransferLeaseRequest) Method() Method { return TransferLease }

// Method implements the Request interface.
func (*LeaseInfoRequest) Method() Method { return LeaseInfo }

// Method implements the Request interface.
func (*ComputeChecksumRequest) Method() Method { return ComputeChecksum }

// Method implements the Request interface.
func (*DeprecatedVerifyChecksumRequest) Method() Method { return DeprecatedVerifyChecksum }

// Method implements the Request interface.
func (*WriteBatchRequest) Method() Method { return WriteBatch }

// Method implements the Request interface.
func (*ExportRequest) Method() Method { return Export }

// ShallowCopy implements the Request interface.
func (gr *GetRequest) ShallowCopy() Request {
	shallowCopy := *gr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (pr *PutRequest) ShallowCopy() Request {
	shallowCopy := *pr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (cpr *ConditionalPutRequest) ShallowCopy() Request {
	shallowCopy := *cpr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (pr *InitPutRequest) ShallowCopy() Request {
	shallowCopy := *pr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (ir *IncrementRequest) ShallowCopy() Request {
	shallowCopy := *ir
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (dr *DeleteRequest) ShallowCopy() Request {
	shallowCopy := *dr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (drr *DeleteRangeRequest) ShallowCopy() Request {
	shallowCopy := *drr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (sr *ScanRequest) ShallowCopy() Request {
	shallowCopy := *sr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (rsr *ReverseScanRequest) ShallowCopy() Request {
	shallowCopy := *rsr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (ccr *CheckConsistencyRequest) ShallowCopy() Request {
	shallowCopy := *ccr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (afr *ChangeFrozenRequest) ShallowCopy() Request {
	shallowCopy := *afr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (btr *BeginTransactionRequest) ShallowCopy() Request {
	shallowCopy := *btr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (etr *EndTransactionRequest) ShallowCopy() Request {
	shallowCopy := *etr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (asr *AdminSplitRequest) ShallowCopy() Request {
	shallowCopy := *asr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (amr *AdminMergeRequest) ShallowCopy() Request {
	shallowCopy := *amr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (atlr *AdminTransferLeaseRequest) ShallowCopy() Request {
	shallowCopy := *atlr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (htr *HeartbeatTxnRequest) ShallowCopy() Request {
	shallowCopy := *htr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (gcr *GCRequest) ShallowCopy() Request {
	shallowCopy := *gcr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (ptr *PushTxnRequest) ShallowCopy() Request {
	shallowCopy := *ptr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (qtr *QueryTxnRequest) ShallowCopy() Request {
	shallowCopy := *qtr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (rlr *RangeLookupRequest) ShallowCopy() Request {
	shallowCopy := *rlr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (rir *ResolveIntentRequest) ShallowCopy() Request {
	shallowCopy := *rir
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (rirr *ResolveIntentRangeRequest) ShallowCopy() Request {
	shallowCopy := *rirr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (nr *NoopRequest) ShallowCopy() Request {
	shallowCopy := *nr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (mr *MergeRequest) ShallowCopy() Request {
	shallowCopy := *mr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (tlr *TruncateLogRequest) ShallowCopy() Request {
	shallowCopy := *tlr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (rlr *RequestLeaseRequest) ShallowCopy() Request {
	shallowCopy := *rlr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (tlr *TransferLeaseRequest) ShallowCopy() Request {
	shallowCopy := *tlr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (lt *LeaseInfoRequest) ShallowCopy() Request {
	shallowCopy := *lt
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (ccr *ComputeChecksumRequest) ShallowCopy() Request {
	shallowCopy := *ccr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (dvcr *DeprecatedVerifyChecksumRequest) ShallowCopy() Request {
	shallowCopy := *dvcr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *WriteBatchRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (ekr *ExportRequest) ShallowCopy() Request {
	shallowCopy := *ekr
	return &shallowCopy
}

// NewGet returns a Request initialized to get the value at key.
func NewGet(key Key) Request {
	return &GetRequest{
		Span: Span{
			Key: key,
		},
	}
}

// NewIncrement returns a Request initialized to increment the value at
// key by increment.
func NewIncrement(key Key, increment int64) Request {
	return &IncrementRequest{
		Span: Span{
			Key: key,
		},
		Increment: increment,
	}
}

// NewPut returns a Request initialized to put the value at key.
func NewPut(key Key, value Value) Request {
	value.InitChecksum(key)
	return &PutRequest{
		Span: Span{
			Key: key,
		},
		Value: value,
	}
}

// NewPutInline returns a Request initialized to put the value at key
// using an inline value.
func NewPutInline(key Key, value Value) Request {
	value.InitChecksum(key)
	return &PutRequest{
		Span: Span{
			Key: key,
		},
		Value:  value,
		Inline: true,
	}
}

// NewConditionalPut returns a Request initialized to put value as a byte
// slice at key if the existing value at key equals expValueBytes.
func NewConditionalPut(key Key, value, expValue Value) Request {
	value.InitChecksum(key)
	var expValuePtr *Value
	if expValue.RawBytes != nil {
		// Make a copy to avoid forcing expValue itself on to the heap.
		expValueTmp := expValue
		expValuePtr = &expValueTmp
		expValue.InitChecksum(key)
	}
	return &ConditionalPutRequest{
		Span: Span{
			Key: key,
		},
		Value:    value,
		ExpValue: expValuePtr,
	}
}

// NewInitPut returns a Request initialized to put the value at key,
// as long as the key doesn't exist, returning an error if the key
// exists and the existing value is different from value.
func NewInitPut(key Key, value Value) Request {
	value.InitChecksum(key)
	return &InitPutRequest{
		Span: Span{
			Key: key,
		},
		Value: value,
	}
}

// NewDelete returns a Request initialized to delete the value at key.
func NewDelete(key Key) Request {
	return &DeleteRequest{
		Span: Span{
			Key: key,
		},
	}
}

// NewDeleteRange returns a Request initialized to delete the values in
// the given key range (excluding the endpoint).
func NewDeleteRange(startKey, endKey Key, returnKeys bool) Request {
	return &DeleteRangeRequest{
		Span: Span{
			Key:    startKey,
			EndKey: endKey,
		},
		ReturnKeys: returnKeys,
	}
}

// NewScan returns a Request initialized to scan from start to end keys
// with max results.
func NewScan(key, endKey Key) Request {
	return &ScanRequest{
		Span: Span{
			Key:    key,
			EndKey: endKey,
		},
	}
}

// NewChangeFrozen returns a Request initialized to scan from start to end keys.
func NewChangeFrozen(key, endKey Key, frozen bool, mustVersion string) Request {
	return &ChangeFrozenRequest{
		Span: Span{
			Key:    key,
			EndKey: endKey,
		},
		Frozen:      frozen,
		MustVersion: mustVersion,
	}
}

// NewCheckConsistency returns a Request initialized to scan from start to end keys.
func NewCheckConsistency(key, endKey Key, withDiff bool) Request {
	return &CheckConsistencyRequest{
		Span: Span{
			Key:    key,
			EndKey: endKey,
		},
		WithDiff: withDiff,
	}
}

// NewReverseScan returns a Request initialized to reverse scan from end to
// start keys with max results.
func NewReverseScan(key, endKey Key) Request {
	return &ReverseScanRequest{
		Span: Span{
			Key:    key,
			EndKey: endKey,
		},
	}
}

func (*GetRequest) flags() int            { return isRead | isTxn }
func (*PutRequest) flags() int            { return isWrite | isTxn | isTxnWrite }
func (*ConditionalPutRequest) flags() int { return isRead | isWrite | isTxn | isTxnWrite }
func (*InitPutRequest) flags() int        { return isRead | isWrite | isTxn | isTxnWrite }
func (*IncrementRequest) flags() int      { return isRead | isWrite | isTxn | isTxnWrite }
func (*DeleteRequest) flags() int         { return isWrite | isTxn | isTxnWrite }
func (drr *DeleteRangeRequest) flags() int {
	// DeleteRangeRequest has different properties if the "inline" flag is set.
	// This flag indicates that the request is deleting inline MVCC values,
	// which cannot be deleted transactionally - inline DeleteRange will thus
	// fail if executed as part of a transaction. This alternate flag set
	// is needed to prevent the command from being automatically wrapped into a
	// transaction by TxnCoordSender, which can occur if the command spans
	// multiple ranges.
	//
	// TODO(mrtracy): The behavior of DeleteRangeRequest with "inline" set has
	// likely diverged enough that it should be promoted into its own command.
	// However, it is complicated to plumb a new command through the system,
	// while this special case in flags() fixes all current issues succinctly.
	// This workaround does not preclude us from creating a separate
	// "DeleteInlineRange" command at a later date.
	if drr.Inline {
		return isWrite | isRange | isAlone
	}
	return isWrite | isTxn | isTxnWrite | isRange
}
func (*ScanRequest) flags() int               { return isRead | isRange | isTxn }
func (*ReverseScanRequest) flags() int        { return isRead | isRange | isReverse | isTxn }
func (*BeginTransactionRequest) flags() int   { return isWrite | isTxn }
func (*EndTransactionRequest) flags() int     { return isWrite | isTxn | isAlone }
func (*AdminSplitRequest) flags() int         { return isAdmin | isAlone }
func (*AdminMergeRequest) flags() int         { return isAdmin | isAlone }
func (*AdminTransferLeaseRequest) flags() int { return isAdmin | isAlone }
func (*HeartbeatTxnRequest) flags() int       { return isWrite | isTxn }
func (*GCRequest) flags() int                 { return isWrite | isRange }
func (*PushTxnRequest) flags() int            { return isWrite | isAlone }
func (*QueryTxnRequest) flags() int           { return isRead | isAlone }
func (*RangeLookupRequest) flags() int        { return isRead }
func (*ResolveIntentRequest) flags() int      { return isWrite }
func (*ResolveIntentRangeRequest) flags() int { return isWrite | isRange }
func (*NoopRequest) flags() int               { return isRead } // slightly special
func (*TruncateLogRequest) flags() int        { return isWrite | isNonKV }

// MergeRequests are considered "non KV" because they do not need to be gated
// by the command queue (reordering is ok) and they operate on non-MVCC data so
// the timestamp cache is also unnecessary.
func (*MergeRequest) flags() int { return isWrite | isNonKV }

func (*RequestLeaseRequest) flags() int {
	return isWrite | isAlone | isNonKV | skipLeaseCheck
}

// LeaseInfoRequest is usually executed in an INCONSISTENT batch, which has the
// effect of the `skipLeaseCheck` flag that lease write operations have.
func (*LeaseInfoRequest) flags() int { return isRead | isNonKV | isAlone }
func (*TransferLeaseRequest) flags() int {
	// TODO(andrei): update this comment.
	// TransferLeaseRequest requires the lease, which is checked in
	// `AdminTransferLease()` at proposal time and in the usual way for write
	// commands at apply time.
	// But it can't be checked at propose time through the
	// `redirectOnOrAcquireLease` call because, by the time that call is made, the
	// replica has registered that a transfer is in progress and
	// `redirectOnOrAcquireLease` already tentatively redirects to the
	// future lease holder.
	return isWrite | isAlone | isNonKV | skipLeaseCheck
}
func (*ComputeChecksumRequest) flags() int          { return isWrite | isNonKV | isRange }
func (*DeprecatedVerifyChecksumRequest) flags() int { return isWrite }
func (*CheckConsistencyRequest) flags() int         { return isAdmin | isRange }
func (*ChangeFrozenRequest) flags() int             { return isWrite | isRange | isNonKV }
func (*WriteBatchRequest) flags() int               { return isWrite | isRange }
func (*ExportRequest) flags() int                   { return isRead | isRange }

// Keys returns credentials in an s3gof3r.Keys
func (b *ExportStorage_S3) Keys() s3gof3r.Keys {
	return s3gof3r.Keys{
		AccessKey: b.AccessKey,
		SecretKey: b.Secret,
	}
}

// InsertRangeInfo inserts ri into a slice of RangeInfo's if a descriptor for
// the same range is not already present. If it is present, it's overwritten;
// the rationale being that ri is newer information than what we had before.
func InsertRangeInfo(ris []RangeInfo, ri RangeInfo) []RangeInfo {
	for i := range ris {
		if ris[i].Desc.RangeID == ri.Desc.RangeID {
			ris[i] = ri
			return ris
		}
	}
	return append(ris, ri)
}
