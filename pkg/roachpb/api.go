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

package roachpb

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
)

// UserPriority is a custom type for transaction's user priority.
type UserPriority float64

func (up UserPriority) String() string {
	switch up {
	case MinUserPriority:
		return "low"
	case UnspecifiedUserPriority, NormalUserPriority:
		return "normal"
	case MaxUserPriority:
		return "high"
	default:
		return fmt.Sprintf("%g", float64(up))
	}
}

const (
	// MinUserPriority is the minimum allowed user priority.
	MinUserPriority UserPriority = 0.001
	// UnspecifiedUserPriority means NormalUserPriority.
	UnspecifiedUserPriority UserPriority = 0
	// NormalUserPriority is set to 1, meaning ops run through the database
	// are all given equal weight when a random priority is chosen. This can
	// be set specifically via client.NewDBWithPriority().
	NormalUserPriority UserPriority = 1
	// MaxUserPriority is the maximum allowed user priority.
	MaxUserPriority UserPriority = 1000
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

// RequiresReadLease returns whether the ReadConsistencyType requires
// that a read-only request be performed on an active valid leaseholder.
func (rc ReadConsistencyType) RequiresReadLease() bool {
	switch rc {
	case CONSISTENT:
		return true
	case READ_UNCOMMITTED:
		return true
	case INCONSISTENT:
		return false
	}
	panic("unreachable")
}

// SupportsBatch determines whether the methods in the provided batch
// are supported by the ReadConsistencyType, returning an error if not.
func (rc ReadConsistencyType) SupportsBatch(ba BatchRequest) error {
	switch rc {
	case CONSISTENT:
		return nil
	case READ_UNCOMMITTED, INCONSISTENT:
		for _, ru := range ba.Requests {
			m := ru.GetInner().Method()
			switch m {
			case Get, Scan, ReverseScan:
			default:
				return errors.Errorf("method %s not allowed with %s batch", m, rc)
			}
		}
		return nil
	}
	panic("unreachable")
}

const (
	isAdmin        = 1 << iota // admin cmds don't go through raft, but run on lease holder
	isRead                     // read-only cmds don't go through raft, but may run on lease holder
	isWrite                    // write cmds go through raft and must be proposed on lease holder
	isTxn                      // txn commands may be part of a transaction
	isTxnWrite                 // txn write cmds start heartbeat and are marked for intent resolution
	isRange                    // range commands may span multiple keys
	isReverse                  // reverse commands traverse ranges in descending direction
	isAlone                    // requests which must be alone in a batch
	isPrefix                   // requests which should be grouped with the next request in a batch
	isUnsplittable             // range command that must not be split during sending
	// Requests for acquiring a lease skip the (proposal-time) check that the
	// proposing replica has a valid lease.
	skipLeaseCheck
	consultsTSCache       // mutating commands which write data at a timestamp
	updatesReadTSCache    // commands which update the read timestamp cache
	updatesWriteTSCache   // commands which update the write timestamp cache
	updatesTSCacheOnError // commands which make read data available on errors
	needsRefresh          // commands which require refreshes to avoid serializable retries
)

// IsReadOnly returns true iff the request is read-only.
func IsReadOnly(args Request) bool {
	flags := args.flags()
	return (flags&isRead) != 0 && (flags&isWrite) == 0
}

// IsTransactional returns true if the request may be part of a
// transaction.
func IsTransactional(args Request) bool {
	return (args.flags() & isTxn) != 0
}

// IsTransactionWrite returns true if the request produces write
// intents when used within a transaction.
func IsTransactionWrite(args Request) bool {
	return (args.flags() & isTxnWrite) != 0
}

// IsRange returns true if the command is range-based and must include
// a start and an end key.
func IsRange(args Request) bool {
	return (args.flags() & isRange) != 0
}

// ConsultsTimestampCache returns whether the command must consult
// the timestamp cache to determine whether a mutation is safe at
// a proposed timestamp or needs to move to a higher timestamp to
// avoid re-writing history.
func ConsultsTimestampCache(args Request) bool {
	return (args.flags() & consultsTSCache) != 0
}

// UpdatesTimestampCache returns whether the command must update
// the read timestamp cache in order to set a low water mark for the
// timestamp at which mutations to overlapping key(s) can write
// such that they don't re-write history.
func UpdatesTimestampCache(args Request) bool {
	return (args.flags() & (updatesReadTSCache | updatesWriteTSCache)) != 0
}

// UpdatesWriteTimestampCache returns whether the command must update
// the write timestamp cache.
func UpdatesWriteTimestampCache(args Request) bool {
	return (args.flags() & updatesWriteTSCache) != 0
}

// UpdatesTimestampCacheOnError returns whether the command must
// update the timestamp cache even on error, as in some cases the data
// which was read is returned (e.g. ConditionalPut ConditionFailedError).
func UpdatesTimestampCacheOnError(args Request) bool {
	return (args.flags() & updatesTSCacheOnError) != 0
}

// NeedsRefresh returns whether the command must be refreshed in
// order to avoid client-side retries on serializable transactions.
func NeedsRefresh(args Request) bool {
	return (args.flags() & needsRefresh) != 0
}

// Request is an interface for RPC requests.
type Request interface {
	protoutil.Message
	// Header returns the request header.
	Header() RequestHeader
	// SetHeader sets the request header.
	SetHeader(RequestHeader)
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
	prevLease() Lease
}

var _ leaseRequestor = &RequestLeaseRequest{}

func (rlr *RequestLeaseRequest) prevLease() Lease {
	return rlr.PrevLease
}

var _ leaseRequestor = &TransferLeaseRequest{}

func (tlr *TransferLeaseRequest) prevLease() Lease {
	return tlr.PrevLease
}

// Response is an interface for RPC responses.
type Response interface {
	protoutil.Message
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
		return errors.Errorf("combining %+v with %+v", rh.ResumeSpan, otherRH.ResumeSpan)
	}
	rh.ResumeSpan = otherRH.ResumeSpan
	rh.ResumeReason = otherRH.ResumeReason
	rh.NumKeys += otherRH.NumKeys
	rh.RangeInfos = append(rh.RangeInfos, otherRH.RangeInfos...)
	return nil
}

// combine implements the combinable interface.
func (sr *ScanResponse) combine(c combinable) error {
	otherSR := c.(*ScanResponse)
	if sr != nil {
		sr.Rows = append(sr.Rows, otherSR.Rows...)
		sr.IntentRows = append(sr.IntentRows, otherSR.IntentRows...)
		sr.BatchResponses = append(sr.BatchResponses, otherSR.BatchResponses...)
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
		sr.IntentRows = append(sr.IntentRows, otherSR.IntentRows...)
		sr.BatchResponses = append(sr.BatchResponses, otherSR.BatchResponses...)
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

// Combine implements the combinable interface.
func (r *AdminScatterResponse) combine(c combinable) error {
	if r != nil {
		otherR := c.(*AdminScatterResponse)
		if err := r.ResponseHeader.combine(otherR.Header()); err != nil {
			return err
		}
		r.Ranges = append(r.Ranges, otherR.Ranges...)
	}
	return nil
}

var _ combinable = &AdminScatterResponse{}

// Header implements the Request interface.
func (rh RequestHeader) Header() RequestHeader {
	return rh
}

// SetHeader implements the Request interface.
func (rh *RequestHeader) SetHeader(other RequestHeader) {
	*rh = other
}

// Span returns the key range that the Request operates over.
func (rh RequestHeader) Span() Span {
	return Span{Key: rh.Key, EndKey: rh.EndKey}
}

// SetSpan addresses the RequestHeader to the specified key span.
func (rh *RequestHeader) SetSpan(s Span) {
	rh.Key = s.Key
	rh.EndKey = s.EndKey
}

// RequestHeaderFromSpan creates a RequestHeader addressed at the specified key
// span.
func RequestHeaderFromSpan(s Span) RequestHeader {
	return RequestHeader{Key: s.Key, EndKey: s.EndKey}
}

func (h *BatchResponse_Header) combine(o BatchResponse_Header) error {
	if h.Error != nil || o.Error != nil {
		return errors.Errorf(
			"can't combine batch responses with errors, have errors %q and %q",
			h.Error, o.Error,
		)
	}
	h.Timestamp.Forward(o.Timestamp)
	if h.Txn == nil {
		h.Txn = o.Txn
	} else {
		h.Txn.Update(o.Txn)
	}
	h.Now.Forward(o.Now)
	h.CollectedSpans = append(h.CollectedSpans, o.CollectedSpans...)
	return nil
}

// SetHeader implements the Response interface.
func (rh *ResponseHeader) SetHeader(other ResponseHeader) {
	*rh = other
}

// Header implements the Response interface for ResponseHeader.
func (rh ResponseHeader) Header() ResponseHeader {
	return rh
}

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

// MustSetInner sets the Request contained in the union. It panics if the
// request is not recognized by the union type. The RequestUnion is reset
// before being repopulated.
func (ru *RequestUnion) MustSetInner(args Request) {
	ru.Reset()
	if !ru.SetInner(args) {
		panic(fmt.Sprintf("%T excludes %T", ru, args))
	}
}

// MustSetInner sets the Response contained in the union. It panics if the
// response is not recognized by the union type. The ResponseUnion is reset
// before being repopulated.
func (ru *ResponseUnion) MustSetInner(reply Response) {
	ru.Reset()
	if !ru.SetInner(reply) {
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
func (*ClearRangeRequest) Method() Method { return ClearRange }

// Method implements the Request interface.
func (*ScanRequest) Method() Method { return Scan }

// Method implements the Request interface.
func (*ReverseScanRequest) Method() Method { return ReverseScan }

// Method implements the Request interface.
func (*CheckConsistencyRequest) Method() Method { return CheckConsistency }

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
func (*AdminChangeReplicasRequest) Method() Method { return AdminChangeReplicas }

// Method implements the Request interface.
func (*AdminRelocateRangeRequest) Method() Method { return AdminRelocateRange }

// Method implements the Request interface.
func (*HeartbeatTxnRequest) Method() Method { return HeartbeatTxn }

// Method implements the Request interface.
func (*GCRequest) Method() Method { return GC }

// Method implements the Request interface.
func (*PushTxnRequest) Method() Method { return PushTxn }

// Method implements the Request interface.
func (*QueryTxnRequest) Method() Method { return QueryTxn }

// Method implements the Request interface.
func (*QueryIntentRequest) Method() Method { return QueryIntent }

// Method implements the Request interface.
func (*ResolveIntentRequest) Method() Method { return ResolveIntent }

// Method implements the Request interface.
func (*ResolveIntentRangeRequest) Method() Method { return ResolveIntentRange }

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
func (*WriteBatchRequest) Method() Method { return WriteBatch }

// Method implements the Request interface.
func (*ExportRequest) Method() Method { return Export }

// Method implements the Request interface.
func (*ImportRequest) Method() Method { return Import }

// Method implements the Request interface.
func (*AdminScatterRequest) Method() Method { return AdminScatter }

// Method implements the Request interface.
func (*AddSSTableRequest) Method() Method { return AddSSTable }

// Method implements the Request interface.
func (*RecomputeStatsRequest) Method() Method { return RecomputeStats }

// Method implements the Request interface.
func (*RefreshRequest) Method() Method { return Refresh }

// Method implements the Request interface.
func (*RefreshRangeRequest) Method() Method { return RefreshRange }

// Method implements the Request interface.
func (*SubsumeRequest) Method() Method { return Subsume }

// Method implements the Request interface.
func (*RangeStatsRequest) Method() Method { return RangeStats }

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
func (crr *ClearRangeRequest) ShallowCopy() Request {
	shallowCopy := *crr
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
func (acrr *AdminChangeReplicasRequest) ShallowCopy() Request {
	shallowCopy := *acrr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (acrr *AdminRelocateRangeRequest) ShallowCopy() Request {
	shallowCopy := *acrr
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
func (pir *QueryIntentRequest) ShallowCopy() Request {
	shallowCopy := *pir
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
func (r *WriteBatchRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (ekr *ExportRequest) ShallowCopy() Request {
	shallowCopy := *ekr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *ImportRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *AdminScatterRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *AddSSTableRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *RecomputeStatsRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *RefreshRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *RefreshRangeRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *SubsumeRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *RangeStatsRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

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

// NewPutInline returns a Request initialized to put the value at key
// using an inline value.
func NewPutInline(key Key, value Value) Request {
	value.InitChecksum(key)
	return &PutRequest{
		RequestHeader: RequestHeader{
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
		RequestHeader: RequestHeader{
			Key: key,
		},
		Value:    value,
		ExpValue: expValuePtr,
	}
}

// NewInitPut returns a Request initialized to put the value at key, as long as
// the key doesn't exist, returning a ConditionFailedError if the key exists and
// the existing value is different from value. If failOnTombstones is set to
// true, tombstones count as mismatched values and will cause a
// ConditionFailedError.
func NewInitPut(key Key, value Value, failOnTombstones bool) Request {
	value.InitChecksum(key)
	return &InitPutRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Value:            value,
		FailOnTombstones: failOnTombstones,
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
func NewDeleteRange(startKey, endKey Key, returnKeys bool) Request {
	return &DeleteRangeRequest{
		RequestHeader: RequestHeader{
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
		RequestHeader: RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
	}
}

// NewReverseScan returns a Request initialized to reverse scan from end to
// start keys with max results.
func NewReverseScan(key, endKey Key) Request {
	return &ReverseScanRequest{
		RequestHeader: RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
	}
}

func (*GetRequest) flags() int { return isRead | isTxn | updatesReadTSCache | needsRefresh }
func (*PutRequest) flags() int { return isWrite | isTxn | isTxnWrite | consultsTSCache }

// ConditionalPut effectively reads and may not write, so must update
// the timestamp cache. Note that on ConditionFailedErrors
// ConditionalPut returns the read data and must update the timestamp
// cache. ConditionalPuts do not require a refresh because on write-too-old
// errors, they return an error immediately instead of continuing a
// serializable transaction to be retried at end transaction.
func (*ConditionalPutRequest) flags() int {
	return isRead | isWrite | isTxn | isTxnWrite | updatesReadTSCache | updatesTSCacheOnError | consultsTSCache
}

// InitPut, like ConditionalPut, effectively reads and may not write.
// It also may return the actual data read on ConditionFailedErrors,
// so must update the timestamp cache on errors. InitPuts do not require
// a refresh because on write-too-old errors, they return an error
// immediately instead of continuing a serializable transaction to be
// retried at end transaction.
func (*InitPutRequest) flags() int {
	return isRead | isWrite | isTxn | isTxnWrite | updatesReadTSCache | updatesTSCacheOnError | consultsTSCache
}

// Increment reads the existing value, but always leaves an intent so
// it does not need to update the timestamp cache. Increments do not
// require a refresh because on write-too-old errors, they return an
// error immediately instead of continuing a serializable transaction
// to be retried at end transaction.
func (*IncrementRequest) flags() int {
	return isRead | isWrite | isTxn | isTxnWrite | consultsTSCache
}

func (*DeleteRequest) flags() int { return isWrite | isTxn | isTxnWrite | consultsTSCache }
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
	// DeleteRange updates the timestamp cache as it doesn't leave
	// intents or tombstones for keys which don't yet exist. By updating
	// the write timestamp cache, it forces subsequent writes to get a
	// write-too-old error and avoids the phantom delete anomaly.
	return isWrite | isTxn | isTxnWrite | isRange | updatesWriteTSCache | needsRefresh | consultsTSCache
}

// Note that ClearRange commands cannot be part of a transaction as
// they clear all MVCC versions.
func (*ClearRangeRequest) flags() int { return isWrite | isRange | isAlone }
func (*ScanRequest) flags() int       { return isRead | isRange | isTxn | updatesReadTSCache | needsRefresh }
func (*ReverseScanRequest) flags() int {
	return isRead | isRange | isReverse | isTxn | updatesReadTSCache | needsRefresh
}
func (*BeginTransactionRequest) flags() int { return isWrite | isTxn }

// EndTransaction updates the write timestamp cache to prevent
// replays. Replays for the same transaction key and timestamp will
// have Txn.WriteTooOld=true and must retry on EndTransaction.
func (*EndTransactionRequest) flags() int      { return isWrite | isTxn | isAlone | updatesWriteTSCache }
func (*AdminSplitRequest) flags() int          { return isAdmin | isAlone }
func (*AdminMergeRequest) flags() int          { return isAdmin | isAlone }
func (*AdminTransferLeaseRequest) flags() int  { return isAdmin | isAlone }
func (*AdminChangeReplicasRequest) flags() int { return isAdmin | isAlone }
func (*AdminRelocateRangeRequest) flags() int  { return isAdmin | isAlone }
func (*HeartbeatTxnRequest) flags() int        { return isWrite | isTxn }
func (*GCRequest) flags() int                  { return isWrite | isRange }

// PushTxnRequest updates the read timestamp cache when pushing a transaction's
// timestamp and updates the write timestamp cache when aborting a transaction.
func (*PushTxnRequest) flags() int {
	return isWrite | isAlone | updatesReadTSCache | updatesWriteTSCache
}
func (*QueryTxnRequest) flags() int { return isRead | isAlone }

// QueryIntent only updates the read timestamp cache when attempting
// to prevent an intent that is found missing from ever being written
// in the future. See QueryIntentRequest_PREVENT.
func (*QueryIntentRequest) flags() int        { return isRead | isPrefix | updatesReadTSCache }
func (*ResolveIntentRequest) flags() int      { return isWrite }
func (*ResolveIntentRangeRequest) flags() int { return isWrite | isRange }
func (*TruncateLogRequest) flags() int        { return isWrite }
func (*MergeRequest) flags() int              { return isWrite }
func (*RequestLeaseRequest) flags() int       { return isWrite | isAlone | skipLeaseCheck }

// LeaseInfoRequest is usually executed in an INCONSISTENT batch, which has the
// effect of the `skipLeaseCheck` flag that lease write operations have.
func (*LeaseInfoRequest) flags() int { return isRead | isAlone }
func (*TransferLeaseRequest) flags() int {
	// TransferLeaseRequest requires the lease, which is checked in
	// `AdminTransferLease()` before the TransferLeaseRequest is created and sent
	// for evaluation and in the usual way at application time (i.e.
	// replica.processRaftCommand() checks that the lease hasn't changed since the
	// command resulting from the evaluation of TransferLeaseRequest was
	// proposed).
	//
	// But we're marking it with skipLeaseCheck because `redirectOnOrAcquireLease`
	// can't be used before evaluation as, by the time that call would be made,
	// the store has registered that a transfer is in progress and
	// `redirectOnOrAcquireLease` would already tentatively redirect to the future
	// lease holder.
	return isWrite | isAlone | skipLeaseCheck
}
func (*RecomputeStatsRequest) flags() int   { return isWrite | isAlone }
func (*ComputeChecksumRequest) flags() int  { return isWrite }
func (*CheckConsistencyRequest) flags() int { return isAdmin | isRange }
func (*WriteBatchRequest) flags() int       { return isWrite | isRange }
func (*ExportRequest) flags() int           { return isRead | isRange | updatesReadTSCache }
func (*ImportRequest) flags() int           { return isAdmin | isAlone }
func (*AdminScatterRequest) flags() int     { return isAdmin | isAlone | isRange }
func (*AddSSTableRequest) flags() int       { return isWrite | isAlone | isRange | isUnsplittable }

// RefreshRequest and RefreshRangeRequest both determine which timestamp cache
// they update based on their Write parameter.
func (r *RefreshRequest) flags() int {
	if r.Write {
		return isRead | isTxn | updatesWriteTSCache
	}
	return isRead | isTxn | updatesReadTSCache
}
func (r *RefreshRangeRequest) flags() int {
	if r.Write {
		return isRead | isTxn | isRange | updatesWriteTSCache
	}
	return isRead | isTxn | isRange | updatesReadTSCache
}

func (*SubsumeRequest) flags() int    { return isRead | isAlone | updatesReadTSCache }
func (*RangeStatsRequest) flags() int { return isRead }

// Keys returns credentials in an aws.Config.
func (b *ExportStorage_S3) Keys() *aws.Config {
	return &aws.Config{
		Credentials: credentials.NewStaticCredentials(b.AccessKey, b.Secret, b.TempToken),
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

// Add combines the values from other, for use on an accumulator BulkOpSummary.
func (b *BulkOpSummary) Add(other BulkOpSummary) {
	b.DataSize += other.DataSize
	b.Rows += other.Rows
	b.IndexEntries += other.IndexEntries
	b.SystemRecords += other.SystemRecords
}

// MustSetValue is like SetValue, except it resets the enum and panics if the
// provided value is not a valid variant type.
func (e *RangeFeedEvent) MustSetValue(value interface{}) {
	e.Reset()
	if !e.SetValue(value) {
		panic(fmt.Sprintf("%T excludes %T", e, value))
	}
}
