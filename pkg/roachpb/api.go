// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"context"
	"fmt"

	_ "github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil" // see RequestHeader
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
)

//go:generate mockgen -package=roachpbmock -destination=roachpbmock/mocks_generated.go . InternalClient,Internal_RangeFeedClient,Internal_MuxRangeFeedClient

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

// SupportsBatch determines whether the methods in the provided batch
// are supported by the ReadConsistencyType, returning an error if not.
func (rc ReadConsistencyType) SupportsBatch(ba *BatchRequest) error {
	switch rc {
	case CONSISTENT:
		return nil
	case READ_UNCOMMITTED, INCONSISTENT:
		for _, ru := range ba.Requests {
			m := ru.GetInner().Method()
			switch m {
			case Get, Scan, ReverseScan, QueryResolvedTimestamp:
			default:
				return errors.Errorf("method %s not allowed with %s batch", m, rc)
			}
		}
		return nil
	}
	panic("unreachable")
}

type flag int

const (
	isAdmin                                  flag = 1 << iota // admin cmds don't go through raft, but run on lease holder
	isRead                                                    // read-only cmds don't go through raft, but may run on lease holder
	isWrite                                                   // write cmds go through raft and must be proposed on lease holder
	isTxn                                                     // txn commands may be part of a transaction
	isLocking                                                 // locking cmds acquire locks for their transaction
	isIntentWrite                                             // intent write cmds leave intents when they succeed
	isRange                                                   // range commands may span multiple keys
	isReverse                                                 // reverse commands traverse ranges in descending direction
	isAlone                                                   // requests which must be alone in a batch
	isPrefix                                                  // requests which, in a batch, must not be split from the following request
	isUnsplittable                                            // range command that must not be split during sending
	skipsLeaseCheck                                           // commands which skip the check that the evaluating replica has a valid lease
	appliesTSCache                                            // commands which apply the timestamp cache and closed timestamp
	updatesTSCache                                            // commands which update the timestamp cache
	updatesTSCacheOnErr                                       // commands which make read data available on errors
	needsRefresh                                              // commands which require refreshes to avoid serializable retries
	canBackpressure                                           // commands which deserve backpressure when a Range grows too large
	canSkipLocked                                             // commands which can evaluate under the SkipLocked wait policy
	bypassesReplicaCircuitBreaker                             // commands which bypass the replica circuit breaker, i.e. opt out of fail-fast
	requiresClosedTSOlderThanStorageSnapshot                  // commands which read a replica's closed timestamp that is older than the state of the storage engine
)

// flagDependencies specifies flag dependencies, asserted by TestFlagCombinations.
var flagDependencies = map[flag][]flag{
	isAdmin:         {isAlone},
	isLocking:       {isTxn},
	isIntentWrite:   {isWrite, isLocking},
	appliesTSCache:  {isWrite},
	skipsLeaseCheck: {isAlone},
}

// flagExclusions specifies flag incompatibilities, asserted by TestFlagCombinations.
var flagExclusions = map[flag][]flag{
	skipsLeaseCheck: {isIntentWrite},
}

// IsReadOnly returns true iff the request is read-only. A request is
// read-only if it does not go through raft, meaning that it cannot
// change any replicated state. However, read-only requests may still
// acquire locks with an unreplicated durability level; see IsLocking.
func IsReadOnly(args Request) bool {
	flags := args.flags()
	return (flags&isRead) != 0 && (flags&isWrite) == 0
}

// IsBlindWrite returns true iff the request is a blind-write. A request is a
// blind-write if it is a write that does not observe any key-value state when
// modifying that state (such as puts and deletes). This is in contrast with
// read-write requests, which do observe key-value state when modifying the
// state and may base their modifications off of this existing state (such as
// conditional puts and increments).
//
// As a result of being "blind", blind-writes are allowed to be more freely
// re-ordered with other writes. In practice, this means that they can be
// evaluated with a read timestamp below another write and a write timestamp
// above that write without needing to re-evaluate. This allows the WriteTooOld
// error that is generated during such an occurrence to be deferred.
func IsBlindWrite(args Request) bool {
	flags := args.flags()
	return (flags&isRead) == 0 && (flags&isWrite) != 0
}

// IsTransactional returns true if the request may be part of a
// transaction.
func IsTransactional(args Request) bool {
	return (args.flags() & isTxn) != 0
}

// IsLocking returns true if the request acquires locks when used within
// a transaction.
func IsLocking(args Request) bool {
	return (args.flags() & isLocking) != 0
}

// LockingDurability returns the durability of the locks acquired by the
// request. The function assumes that IsLocking(args).
func LockingDurability(args Request) lock.Durability {
	if IsReadOnly(args) {
		return lock.Unreplicated
	}
	return lock.Replicated
}

// IsIntentWrite returns true if the request produces write intents at
// the request's sequence number when used within a transaction.
func IsIntentWrite(args Request) bool {
	return (args.flags() & isIntentWrite) != 0
}

// IsRange returns true if the command is range-based and must include
// a start and an end key.
func IsRange(args Request) bool {
	return (args.flags() & isRange) != 0
}

// AppliesTimestampCache returns whether the command is a write that applies the
// timestamp cache (and closed timestamp), possibly pushing its write timestamp
// into the future to avoid re-writing history.
func AppliesTimestampCache(args Request) bool {
	return (args.flags() & appliesTSCache) != 0
}

// UpdatesTimestampCache returns whether the command must update
// the timestamp cache in order to set a low water mark for the
// timestamp at which mutations to overlapping key(s) can write
// such that they don't re-write history.
func UpdatesTimestampCache(args Request) bool {
	return (args.flags() & updatesTSCache) != 0
}

// UpdatesTimestampCacheOnError returns whether the command must
// update the timestamp cache even on error, as in some cases the data
// which was read is returned (e.g. ConditionalPut ConditionFailedError).
func UpdatesTimestampCacheOnError(args Request) bool {
	return (args.flags() & updatesTSCacheOnErr) != 0
}

// NeedsRefresh returns whether the command must be refreshed in
// order to avoid client-side retries on serializable transactions.
func NeedsRefresh(args Request) bool {
	return (args.flags() & needsRefresh) != 0
}

// CanBackpressure returns whether the command can be backpressured
// when waiting for a Range to split after it has grown too large.
func CanBackpressure(args Request) bool {
	return (args.flags() & canBackpressure) != 0
}

// CanSkipLocked returns whether the command can evaluate under the
// SkipLocked wait policy.
func CanSkipLocked(args Request) bool {
	return (args.flags() & canSkipLocked) != 0
}

// BypassesReplicaCircuitBreaker returns whether the command bypasses
// the per-Replica circuit breakers. These requests will thus hang when
// addressed to an unavailable range (instead of failing fast).
func BypassesReplicaCircuitBreaker(args Request) bool {
	return (args.flags() & bypassesReplicaCircuitBreaker) != 0
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
	flags() flag
}

// LockingReadRequest is an interface used to expose the key-level locking
// strength of a read-only request.
type LockingReadRequest interface {
	Request
	KeyLockingStrength() lock.Strength
}

var _ LockingReadRequest = (*GetRequest)(nil)

// KeyLockingStrength implements the LockingReadRequest interface.
func (gr *GetRequest) KeyLockingStrength() lock.Strength {
	return gr.KeyLocking
}

var _ LockingReadRequest = (*ScanRequest)(nil)

// KeyLockingStrength implements the LockingReadRequest interface.
func (sr *ScanRequest) KeyLockingStrength() lock.Strength {
	return sr.KeyLocking
}

var _ LockingReadRequest = (*ReverseScanRequest)(nil)

// KeyLockingStrength implements the LockingReadRequest interface.
func (rsr *ReverseScanRequest) KeyLockingStrength() lock.Strength {
	return rsr.KeyLocking
}

// SizedWriteRequest is an interface used to expose the number of bytes a
// request might write.
type SizedWriteRequest interface {
	Request
	WriteBytes() int64
}

var _ SizedWriteRequest = (*PutRequest)(nil)

// WriteBytes makes PutRequest implement SizedWriteRequest.
func (pr *PutRequest) WriteBytes() int64 {
	return int64(len(pr.Key)) + int64(pr.Value.Size())
}

var _ SizedWriteRequest = (*ConditionalPutRequest)(nil)

// WriteBytes makes ConditionalPutRequest implement SizedWriteRequest.
func (cpr *ConditionalPutRequest) WriteBytes() int64 {
	return int64(len(cpr.Key)) + int64(cpr.Value.Size())
}

var _ SizedWriteRequest = (*InitPutRequest)(nil)

// WriteBytes makes InitPutRequest implement SizedWriteRequest.
func (pr *InitPutRequest) WriteBytes() int64 {
	return int64(len(pr.Key)) + int64(pr.Value.Size())
}

var _ SizedWriteRequest = (*IncrementRequest)(nil)

// WriteBytes makes IncrementRequest implement SizedWriteRequest.
func (ir *IncrementRequest) WriteBytes() int64 {
	return int64(len(ir.Key)) + 8 // assume 8 bytes for the int64
}

var _ SizedWriteRequest = (*DeleteRequest)(nil)

// WriteBytes makes DeleteRequest implement SizedWriteRequest.
func (dr *DeleteRequest) WriteBytes() int64 {
	return int64(len(dr.Key))
}

var _ SizedWriteRequest = (*AddSSTableRequest)(nil)

// WriteBytes makes AddSSTableRequest implement SizedWriteRequest.
func (r *AddSSTableRequest) WriteBytes() int64 {
	return int64(len(r.Data))
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

// CombineResponses attempts to combine the two provided responses. If both of
// the responses are combinable, they will be combined. If neither are
// combinable, the function is a no-op and returns a nil error. If one of the
// responses is combinable and the other isn't, the function returns an error.
func CombineResponses(left, right Response) error {
	cLeft, lOK := left.(combinable)
	cRight, rOK := right.(combinable)
	if lOK && rOK {
		return cLeft.combine(cRight)
	} else if lOK != rOK {
		return errors.Errorf("can not combine %T and %T", left, right)
	}
	return nil
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
	rh.NumBytes += otherRH.NumBytes
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

var _ combinable = &DeleteRangeResponse{}

// combine implements the combinable interface.
func (dr *RevertRangeResponse) combine(c combinable) error {
	otherDR := c.(*RevertRangeResponse)
	if dr != nil {
		if err := dr.ResponseHeader.combine(otherDR.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &RevertRangeResponse{}

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

// combine implements the combinable interface.
func (cc *CheckConsistencyResponse) combine(c combinable) error {
	if cc != nil {
		otherCC := c.(*CheckConsistencyResponse)
		cc.Result = append(cc.Result, otherCC.Result...)
		if err := cc.ResponseHeader.combine(otherCC.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &CheckConsistencyResponse{}

// combine implements the combinable interface.
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

// combine implements the combinable interface.
func (r *AdminScatterResponse) combine(c combinable) error {
	if r != nil {
		otherR := c.(*AdminScatterResponse)
		if err := r.ResponseHeader.combine(otherR.Header()); err != nil {
			return err
		}

		r.RangeInfos = append(r.RangeInfos, otherR.RangeInfos...)
		r.MVCCStats.Add(otherR.MVCCStats)
		r.ReplicasScatteredBytes += otherR.ReplicasScatteredBytes
	}
	return nil
}

var _ combinable = &AdminScatterResponse{}

func (avptr *AdminVerifyProtectedTimestampResponse) combine(c combinable) error {
	other := c.(*AdminVerifyProtectedTimestampResponse)
	if avptr != nil {
		avptr.DeprecatedFailedRanges = append(avptr.DeprecatedFailedRanges,
			other.DeprecatedFailedRanges...)
		avptr.VerificationFailedRanges = append(avptr.VerificationFailedRanges,
			other.VerificationFailedRanges...)
		if err := avptr.ResponseHeader.combine(other.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &AdminVerifyProtectedTimestampResponse{}

// combine implements the combinable interface.
func (r *QueryResolvedTimestampResponse) combine(c combinable) error {
	if r != nil {
		otherR := c.(*QueryResolvedTimestampResponse)
		if err := r.ResponseHeader.combine(otherR.Header()); err != nil {
			return err
		}

		r.ResolvedTS.Backward(otherR.ResolvedTS)
	}
	return nil
}

var _ combinable = &QueryResolvedTimestampResponse{}

// combine implements the combinable interface.
func (r *BarrierResponse) combine(c combinable) error {
	otherR := c.(*BarrierResponse)
	if r != nil {
		if err := r.ResponseHeader.combine(otherR.Header()); err != nil {
			return err
		}
		r.Timestamp.Forward(otherR.Timestamp)
	}
	return nil
}

var _ combinable = &BarrierResponse{}

// combine implements the combinable interface.
func (r *QueryLocksResponse) combine(c combinable) error {
	otherR := c.(*QueryLocksResponse)
	if r != nil {
		if err := r.ResponseHeader.combine(otherR.Header()); err != nil {
			return err
		}
		r.Locks = append(r.Locks, otherR.Locks...)
	}
	return nil
}

var _ combinable = &QueryLocksResponse{}

// combine implements the combinable interface.
func (r *IsSpanEmptyResponse) combine(c combinable) error {
	otherR := c.(*IsSpanEmptyResponse)
	if r != nil {
		if err := r.ResponseHeader.combine(otherR.Header()); err != nil {
			return err
		}
		// Given the request doesn't actually count anything, and instead
		// hijacks NumKeys to indicate whether there is any data, there's
		// no good reason to have it take on a value greater than 1.
		if r.ResponseHeader.NumKeys > 1 {
			r.ResponseHeader.NumKeys = 1
		}
	}
	return nil
}

var _ combinable = &IsSpanEmptyResponse{}

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
	if txn := o.Txn; txn != nil {
		if h.Txn == nil {
			h.Txn = txn.Clone()
		} else {
			h.Txn.Update(txn)
		}
	}
	h.Now.Forward(o.Now)
	h.RangeInfos = append(h.RangeInfos, o.RangeInfos...)
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

// Verify implements the Response interface for ResponseHeader with a
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
func (*RevertRangeRequest) Method() Method { return RevertRange }

// Method implements the Request interface.
func (*ScanRequest) Method() Method { return Scan }

// Method implements the Request interface.
func (*ReverseScanRequest) Method() Method { return ReverseScan }

// Method implements the Request interface.
func (*CheckConsistencyRequest) Method() Method { return CheckConsistency }

// Method implements the Request interface.
func (*EndTxnRequest) Method() Method { return EndTxn }

// Method implements the Request interface.
func (*AdminSplitRequest) Method() Method { return AdminSplit }

// Method implements the Request interface.
func (*AdminUnsplitRequest) Method() Method { return AdminUnsplit }

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
func (*RecoverTxnRequest) Method() Method { return RecoverTxn }

// Method implements the Request interface.
func (*QueryTxnRequest) Method() Method { return QueryTxn }

// Method implements the Request interface.
func (*QueryIntentRequest) Method() Method { return QueryIntent }

// Method implements the Request interface.
func (*QueryLocksRequest) Method() Method { return QueryLocks }

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
func (*ProbeRequest) Method() Method { return Probe }

// Method implements the Request interface.
func (*LeaseInfoRequest) Method() Method { return LeaseInfo }

// Method implements the Request interface.
func (*ComputeChecksumRequest) Method() Method { return ComputeChecksum }

// Method implements the Request interface.
func (*ExportRequest) Method() Method { return Export }

// Method implements the Request interface.
func (*AdminScatterRequest) Method() Method { return AdminScatter }

// Method implements the Request interface.
func (*AddSSTableRequest) Method() Method { return AddSSTable }

// Method implements the Request interface.
func (*MigrateRequest) Method() Method { return Migrate }

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

// Method implements the Request interface.
func (*AdminVerifyProtectedTimestampRequest) Method() Method { return AdminVerifyProtectedTimestamp }

// Method implements the Request interface.
func (*QueryResolvedTimestampRequest) Method() Method { return QueryResolvedTimestamp }

// Method implements the Request interface.
func (*BarrierRequest) Method() Method { return Barrier }

// Method implements the Request interface.
func (*IsSpanEmptyRequest) Method() Method { return IsSpanEmpty }

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
func (crr *RevertRangeRequest) ShallowCopy() Request {
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
func (etr *EndTxnRequest) ShallowCopy() Request {
	shallowCopy := *etr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (asr *AdminSplitRequest) ShallowCopy() Request {
	shallowCopy := *asr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (aur *AdminUnsplitRequest) ShallowCopy() Request {
	shallowCopy := *aur
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
func (rtr *RecoverTxnRequest) ShallowCopy() Request {
	shallowCopy := *rtr
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
func (pir *QueryLocksRequest) ShallowCopy() Request {
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
func (r *ProbeRequest) ShallowCopy() Request {
	shallowCopy := *r
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
func (ekr *ExportRequest) ShallowCopy() Request {
	shallowCopy := *ekr
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
func (r *MigrateRequest) ShallowCopy() Request {
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

// ShallowCopy implements the Request interface.
func (r *AdminVerifyProtectedTimestampRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *QueryResolvedTimestampRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *BarrierRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *IsSpanEmptyRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// NewGet returns a Request initialized to get the value at key. If
// forUpdate is true, an unreplicated, exclusive lock is acquired on on
// the key, if it exists.
func NewGet(key Key, forUpdate bool) Request {
	return &GetRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		KeyLocking: scanLockStrength(forUpdate),
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

// NewConditionalPut returns a Request initialized to put value at key if the
// existing value at key equals expValue.
//
// The callee takes ownership of value's underlying bytes and it will mutate
// them. The caller retains ownership of expVal; NewConditionalPut will copy it
// into the request.
func NewConditionalPut(key Key, value Value, expValue []byte, allowNotExist bool) Request {
	value.InitChecksum(key)
	return &ConditionalPutRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Value:               value,
		ExpBytes:            expValue,
		AllowIfDoesNotExist: allowNotExist,
	}
}

// NewConditionalPutInline returns a Request initialized to put an inline value
// at key if the existing value at key equals expValue.
//
// The callee takes ownership of value's underlying bytes and it will mutate
// them. The caller retains ownership of expVal; NewConditionalPut will copy it
// into the request.
func NewConditionalPutInline(key Key, value Value, expValue []byte, allowNotExist bool) Request {
	value.InitChecksum(key)
	return &ConditionalPutRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Value:               value,
		ExpBytes:            expValue,
		AllowIfDoesNotExist: allowNotExist,
		Inline:              true,
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

// NewScan returns a Request initialized to scan from start to end keys.
// If forUpdate is true, unreplicated, exclusive locks are acquired on
// each of the resulting keys.
func NewScan(key, endKey Key, forUpdate bool) Request {
	return &ScanRequest{
		RequestHeader: RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
		KeyLocking: scanLockStrength(forUpdate),
	}
}

// NewReverseScan returns a Request initialized to reverse scan from end.
// If forUpdate is true, unreplicated, exclusive locks are acquired on
// each of the resulting keys.
func NewReverseScan(key, endKey Key, forUpdate bool) Request {
	return &ReverseScanRequest{
		RequestHeader: RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
		KeyLocking: scanLockStrength(forUpdate),
	}
}

func scanLockStrength(forUpdate bool) lock.Strength {
	if forUpdate {
		return lock.Exclusive
	}
	return lock.None
}

func flagForLockStrength(l lock.Strength) flag {
	if l != lock.None {
		return isLocking
	}
	return 0
}

func (gr *GetRequest) flags() flag {
	maybeLocking := flagForLockStrength(gr.KeyLocking)
	return isRead | isTxn | maybeLocking | updatesTSCache | needsRefresh | canSkipLocked
}

func (*PutRequest) flags() flag {
	return isWrite | isTxn | isLocking | isIntentWrite | appliesTSCache | canBackpressure
}

// ConditionalPut effectively reads without writing if it hits a
// ConditionFailedError, so it must update the timestamp cache in this case.
// ConditionalPuts do not require a refresh because on write-too-old errors,
// they return an error immediately instead of continuing a serializable
// transaction to be retried at end transaction.
func (*ConditionalPutRequest) flags() flag {
	return isRead | isWrite | isTxn | isLocking | isIntentWrite |
		appliesTSCache | updatesTSCache | updatesTSCacheOnErr | canBackpressure
}

// InitPut, like ConditionalPut, effectively reads without writing if it hits a
// ConditionFailedError, so it must update the timestamp cache in this case.
// InitPuts do not require a refresh because on write-too-old errors, they
// return an error immediately instead of continuing a serializable transaction
// to be retried at end transaction.
func (*InitPutRequest) flags() flag {
	return isRead | isWrite | isTxn | isLocking | isIntentWrite |
		appliesTSCache | updatesTSCache | updatesTSCacheOnErr | canBackpressure
}

// Increment reads the existing value, but always leaves an intent so
// it does not need to update the timestamp cache. Increments do not
// require a refresh because on write-too-old errors, they return an
// error immediately instead of continuing a serializable transaction
// to be retried at end transaction.
func (*IncrementRequest) flags() flag {
	return isRead | isWrite | isTxn | isLocking | isIntentWrite | appliesTSCache | canBackpressure
}

func (*DeleteRequest) flags() flag {
	// isRead because of the FoundKey boolean in the response, indicating whether
	// an existing key was deleted at the read timestamp. isIntentWrite allows
	// omitting needsRefresh. For background, see:
	// https://github.com/cockroachdb/cockroach/pull/89375
	return isRead | isWrite | isTxn | isLocking | isIntentWrite | appliesTSCache | canBackpressure
}

func (drr *DeleteRangeRequest) flags() flag {
	// DeleteRangeRequest using MVCC range tombstones cannot be transactional.
	if drr.UseRangeTombstone {
		return isWrite | isRange | isAlone | appliesTSCache
	}
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
		return isRead | isWrite | isRange | isAlone
	}
	// DeleteRange updates the timestamp cache as it doesn't leave intents or
	// tombstones for keys which don't yet exist or keys that already have
	// tombstones on them, but still wants to prevent anybody from writing under
	// it. Note that, even if we didn't update the ts cache, deletes of keys
	// that exist would not be lost (since the DeleteRange leaves intents on
	// those keys), but deletes of "empty space" would.
	return isRead | isWrite | isTxn | isLocking | isIntentWrite | isRange |
		appliesTSCache | updatesTSCache | needsRefresh | canBackpressure
}

// Note that ClearRange commands cannot be part of a transaction as
// they clear all MVCC versions.
func (*ClearRangeRequest) flags() flag {
	return isWrite | isRange | isAlone | bypassesReplicaCircuitBreaker
}

// Note that RevertRange commands cannot be part of a transaction as
// they clear all MVCC versions above their target time.
func (*RevertRangeRequest) flags() flag {
	return isWrite | isRange | isAlone | bypassesReplicaCircuitBreaker
}

func (sr *ScanRequest) flags() flag {
	maybeLocking := flagForLockStrength(sr.KeyLocking)
	return isRead | isRange | isTxn | maybeLocking | updatesTSCache | needsRefresh | canSkipLocked
}

func (rsr *ReverseScanRequest) flags() flag {
	maybeLocking := flagForLockStrength(rsr.KeyLocking)
	return isRead | isRange | isReverse | isTxn | maybeLocking | updatesTSCache | needsRefresh | canSkipLocked
}

// EndTxn updates the timestamp cache to prevent replays.
// Replays for the same transaction key and timestamp will have
// Txn.WriteTooOld=true and must retry on EndTxn.
func (*EndTxnRequest) flags() flag              { return isWrite | isTxn | isAlone | updatesTSCache }
func (*AdminSplitRequest) flags() flag          { return isAdmin | isAlone }
func (*AdminUnsplitRequest) flags() flag        { return isAdmin | isAlone }
func (*AdminMergeRequest) flags() flag          { return isAdmin | isAlone }
func (*AdminTransferLeaseRequest) flags() flag  { return isAdmin | isAlone }
func (*AdminChangeReplicasRequest) flags() flag { return isAdmin | isAlone }
func (*AdminRelocateRangeRequest) flags() flag  { return isAdmin | isAlone }

func (gcr *GCRequest) flags() flag {
	// We defensively let GCRequest bypass the circuit breaker because otherwise,
	// the GC queue might busy loop on an unavailable range, doing lots of work
	// but never making progress.
	flags := isWrite | isRange | bypassesReplicaCircuitBreaker
	// For clear range requests that GC entire range we don't want to batch with
	// anything else.
	if gcr.ClearRange != nil {
		flags |= isAlone
	}
	return flags
}

// HeartbeatTxn updates the timestamp cache with transaction records,
// to avoid checking for them on disk when considering 1PC evaluation.
func (*HeartbeatTxnRequest) flags() flag { return isWrite | isTxn | updatesTSCache }

// PushTxnRequest updates different marker keys in the timestamp cache when
// pushing a transaction's timestamp and when aborting a transaction.
func (*PushTxnRequest) flags() flag {
	return isWrite | isAlone | updatesTSCache | updatesTSCache
}
func (*RecoverTxnRequest) flags() flag { return isWrite | isAlone | updatesTSCache }
func (*QueryTxnRequest) flags() flag   { return isRead | isAlone }

// QueryIntent only updates the timestamp cache when attempting to prevent an
// intent that is found missing from ever being written in the future. See
// QueryIntentRequest_PREVENT.
func (*QueryIntentRequest) flags() flag {
	return isRead | isPrefix | updatesTSCache | updatesTSCacheOnErr
}
func (*QueryLocksRequest) flags() flag         { return isRead | isRange }
func (*ResolveIntentRequest) flags() flag      { return isWrite }
func (*ResolveIntentRangeRequest) flags() flag { return isWrite | isRange }
func (*TruncateLogRequest) flags() flag        { return isWrite }
func (*MergeRequest) flags() flag              { return isWrite | canBackpressure }
func (*RequestLeaseRequest) flags() flag {
	return isWrite | isAlone | skipsLeaseCheck | bypassesReplicaCircuitBreaker
}

// LeaseInfoRequest is usually executed in an INCONSISTENT batch, which has the
// effect of the `skipsLeaseCheck` flag that lease write operations have.
func (*LeaseInfoRequest) flags() flag { return isRead | isAlone }
func (*TransferLeaseRequest) flags() flag {
	// TransferLeaseRequest requires the lease, which is checked in
	// `AdminTransferLease()` before the TransferLeaseRequest is created and sent
	// for evaluation and in the usual way at application time (i.e.
	// replica.processRaftCommand() checks that the lease hasn't changed since the
	// command resulting from the evaluation of TransferLeaseRequest was
	// proposed).
	//
	// But we're marking it with skipsLeaseCheck because `redirectOnOrAcquireLease`
	// can't be used before evaluation as, by the time that call would be made,
	// the store has registered that a transfer is in progress and
	// `redirectOnOrAcquireLease` would already tentatively redirect to the future
	// lease holder.
	//
	// Note that we intentionally don't let TransferLease bypass the Replica
	// circuit breaker. Transferring a lease while the replication layer is
	// unavailable results in the "old" leaseholder relinquishing the ability
	// to serve (strong) reads, without being able to hand over the lease.
	return isWrite | isAlone | skipsLeaseCheck
}
func (*ProbeRequest) flags() flag {
	return isWrite | isAlone | skipsLeaseCheck | bypassesReplicaCircuitBreaker
}
func (*RecomputeStatsRequest) flags() flag   { return isWrite | isAlone }
func (*ComputeChecksumRequest) flags() flag  { return isWrite }
func (*CheckConsistencyRequest) flags() flag { return isAdmin | isRange | isAlone }
func (*ExportRequest) flags() flag {
	return isRead | isRange | updatesTSCache | bypassesReplicaCircuitBreaker
}
func (*AdminScatterRequest) flags() flag                  { return isAdmin | isRange | isAlone }
func (*AdminVerifyProtectedTimestampRequest) flags() flag { return isAdmin | isRange | isAlone }
func (r *AddSSTableRequest) flags() flag {
	flags := isWrite | isRange | isAlone | isUnsplittable | canBackpressure | bypassesReplicaCircuitBreaker
	if r.SSTTimestampToRequestTimestamp.IsSet() {
		flags |= appliesTSCache
	}
	return flags
}
func (*MigrateRequest) flags() flag { return isWrite | isRange | isAlone }

// RefreshRequest and RefreshRangeRequest both determine which timestamp cache
// they update based on their Write parameter.
func (r *RefreshRequest) flags() flag {
	return isRead | isTxn | updatesTSCache
}
func (r *RefreshRangeRequest) flags() flag {
	return isRead | isTxn | isRange | updatesTSCache
}

func (*SubsumeRequest) flags() flag    { return isRead | isAlone | updatesTSCache }
func (*RangeStatsRequest) flags() flag { return isRead }
func (*QueryResolvedTimestampRequest) flags() flag {
	return isRead | isRange | requiresClosedTSOlderThanStorageSnapshot
}
func (*BarrierRequest) flags() flag     { return isWrite | isRange }
func (*IsSpanEmptyRequest) flags() flag { return isRead | isRange }

// IsParallelCommit returns whether the EndTxn request is attempting to perform
// a parallel commit. See txn_interceptor_committer.go for a discussion about
// parallel commits.
func (etr *EndTxnRequest) IsParallelCommit() bool {
	return etr.Commit && len(etr.InFlightWrites) > 0
}

// BulkOpSummaryID returns the key within a BulkOpSummary's EntryCounts map for
// the given table and index ID. This logic is mirrored in c++ in rowcounter.cc.
func BulkOpSummaryID(tableID, indexID uint64) uint64 {
	return (tableID << 32) | indexID
}

// Add combines the values from other, for use on an accumulator BulkOpSummary.
func (b *BulkOpSummary) Add(other BulkOpSummary) {
	b.DataSize += other.DataSize
	b.SSTDataSize += other.SSTDataSize
	b.DeprecatedRows += other.DeprecatedRows
	b.DeprecatedIndexEntries += other.DeprecatedIndexEntries

	if other.EntryCounts != nil && b.EntryCounts == nil {
		b.EntryCounts = make(map[uint64]int64, len(other.EntryCounts))
	}
	for i := range other.EntryCounts {
		b.EntryCounts[i] += other.EntryCounts[i]
	}
}

// MustSetValue is like SetValue, except it resets the enum and panics if the
// provided value is not a valid variant type.
func (e *RangeFeedEvent) MustSetValue(value interface{}) {
	e.Reset()
	if !e.SetValue(value) {
		panic(errors.AssertionFailedf("%T excludes %T", e, value))
	}
}

// ShallowCopy returns a shallow copy of the receiver and its variant type.
func (e *RangeFeedEvent) ShallowCopy() *RangeFeedEvent {
	cpy := *e
	switch t := cpy.GetValue().(type) {
	case *RangeFeedValue:
		cpyVal := *t
		cpy.MustSetValue(&cpyVal)
	case *RangeFeedCheckpoint:
		cpyChk := *t
		cpy.MustSetValue(&cpyChk)
	case *RangeFeedSSTable:
		cpySST := *t
		cpy.MustSetValue(&cpySST)
	case *RangeFeedDeleteRange:
		cpyDelRange := *t
		cpy.MustSetValue(&cpyDelRange)
	case *RangeFeedError:
		cpyErr := *t
		cpy.MustSetValue(&cpyErr)
	default:
		panic(fmt.Sprintf("unexpected RangeFeedEvent variant: %v", t))
	}
	return &cpy
}

// Timestamp is part of rangefeedbuffer.Event.
func (e *RangeFeedValue) Timestamp() hlc.Timestamp {
	return e.Value.Timestamp
}

// MakeReplicationChanges returns a slice of changes of the given type with an
// item for each target.
func MakeReplicationChanges(
	changeType ReplicaChangeType, targets ...ReplicationTarget,
) []ReplicationChange {
	chgs := make([]ReplicationChange, 0, len(targets))
	for _, target := range targets {
		chgs = append(chgs, ReplicationChange{
			ChangeType: changeType,
			Target:     target,
		})
	}
	return chgs
}

// ReplicationChangesForPromotion returns the replication changes that
// correspond to the promotion of a non-voter to a voter.
func ReplicationChangesForPromotion(target ReplicationTarget) []ReplicationChange {
	return []ReplicationChange{
		{ChangeType: ADD_VOTER, Target: target}, {ChangeType: REMOVE_NON_VOTER, Target: target},
	}
}

// ReplicationChangesForDemotion returns the replication changes that correspond
// to the demotion of a voter to a non-voter.
func ReplicationChangesForDemotion(target ReplicationTarget) []ReplicationChange {
	return []ReplicationChange{
		{ChangeType: ADD_NON_VOTER, Target: target}, {ChangeType: REMOVE_VOTER, Target: target},
	}
}

// AddChanges adds a batch of changes to the request in a backwards-compatible
// way.
func (acrr *AdminChangeReplicasRequest) AddChanges(chgs ...ReplicationChange) {
	acrr.InternalChanges = append(acrr.InternalChanges, chgs...)

	acrr.DeprecatedChangeType = chgs[0].ChangeType
	for _, chg := range chgs {
		acrr.DeprecatedTargets = append(acrr.DeprecatedTargets, chg.Target)
	}
}

// ReplicationChanges is a slice of ReplicationChange.
type ReplicationChanges []ReplicationChange

func (rc ReplicationChanges) byType(typ ReplicaChangeType) []ReplicationTarget {
	var sl []ReplicationTarget
	for _, chg := range rc {
		if chg.ChangeType == typ {
			sl = append(sl, chg.Target)
		}
	}
	return sl
}

// VoterAdditions returns a slice of all contained replication changes that add replicas.
func (rc ReplicationChanges) VoterAdditions() []ReplicationTarget {
	return rc.byType(ADD_VOTER)
}

// VoterRemovals returns a slice of all contained replication changes that remove replicas.
func (rc ReplicationChanges) VoterRemovals() []ReplicationTarget {
	return rc.byType(REMOVE_VOTER)
}

// NonVoterAdditions returns a slice of all contained replication
// changes that add non-voters.
func (rc ReplicationChanges) NonVoterAdditions() []ReplicationTarget {
	return rc.byType(ADD_NON_VOTER)
}

// NonVoterRemovals returns a slice of all contained replication changes
// that remove non-voters.
func (rc ReplicationChanges) NonVoterRemovals() []ReplicationTarget {
	return rc.byType(REMOVE_NON_VOTER)
}

// Changes returns the changes requested by this AdminChangeReplicasRequest, taking
// the deprecated method of doing so into account.
func (acrr *AdminChangeReplicasRequest) Changes() []ReplicationChange {
	if len(acrr.InternalChanges) > 0 {
		return acrr.InternalChanges
	}

	sl := make([]ReplicationChange, len(acrr.DeprecatedTargets))
	for _, target := range acrr.DeprecatedTargets {
		sl = append(sl, ReplicationChange{
			ChangeType: acrr.DeprecatedChangeType,
			Target:     target,
		})
	}
	return sl
}

// AsLockUpdate creates a lock update message corresponding to the given resolve
// intent request.
func (rir *ResolveIntentRequest) AsLockUpdate() LockUpdate {
	return LockUpdate{
		Span:              rir.Span(),
		Txn:               rir.IntentTxn,
		Status:            rir.Status,
		IgnoredSeqNums:    rir.IgnoredSeqNums,
		ClockWhilePending: rir.ClockWhilePending,
	}
}

// AsLockUpdate creates a lock update message corresponding to the given resolve
// intent range request.
func (rirr *ResolveIntentRangeRequest) AsLockUpdate() LockUpdate {
	return LockUpdate{
		Span:              rirr.Span(),
		Txn:               rirr.IntentTxn,
		Status:            rirr.Status,
		IgnoredSeqNums:    rirr.IgnoredSeqNums,
		ClockWhilePending: rirr.ClockWhilePending,
	}
}

// CreateStoreIdent creates a store identifier out of the details captured
// within the join node response (the join node RPC is used to allocate a store
// ID for the client's first store).
func (r *JoinNodeResponse) CreateStoreIdent() (StoreIdent, error) {
	nodeID, storeID := NodeID(r.NodeID), StoreID(r.StoreID)
	clusterID, err := uuid.FromBytes(r.ClusterID)
	if err != nil {
		return StoreIdent{}, err
	}

	sIdent := StoreIdent{
		ClusterID: clusterID,
		NodeID:    nodeID,
		StoreID:   storeID,
	}
	return sIdent, nil
}

// IsEmpty returns true if the NumKeys field of the ResponseHeader is 0,
// indicating that the span is empty.
func (r *IsSpanEmptyResponse) IsEmpty() bool {
	return r.NumKeys == 0
}

// SafeFormat implements redact.SafeFormatter.
func (c *ContentionEvent) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("conflicted with %s on %s for %.3fs", c.TxnMeta.ID, c.Key, c.Duration.Seconds())
}

// String implements fmt.Stringer.
func (c *ContentionEvent) String() string {
	return redact.StringWithoutMarkers(c)
}

// Equal returns whether the two structs are identical. Needed for compatibility
// with proto2.
func (c *TenantConsumption) Equal(other *TenantConsumption) bool {
	return *c == *other
}

// Equal is used by generated code when TenantConsumption is embedded in a
// proto2.
var _ = (*TenantConsumption).Equal

// Add consumption from the given structure.
func (c *TenantConsumption) Add(other *TenantConsumption) {
	c.RU += other.RU
	c.KVRU += other.KVRU
	c.ReadBatches += other.ReadBatches
	c.ReadRequests += other.ReadRequests
	c.ReadBytes += other.ReadBytes
	c.WriteBatches += other.WriteBatches
	c.WriteRequests += other.WriteRequests
	c.WriteBytes += other.WriteBytes
	c.SQLPodsCPUSeconds += other.SQLPodsCPUSeconds
	c.PGWireEgressBytes += other.PGWireEgressBytes
	c.ExternalIOIngressBytes += other.ExternalIOIngressBytes
	c.ExternalIOEgressBytes += other.ExternalIOEgressBytes
}

// Sub subtracts consumption, making sure no fields become negative.
func (c *TenantConsumption) Sub(other *TenantConsumption) {
	if c.RU < other.RU {
		c.RU = 0
	} else {
		c.RU -= other.RU
	}

	if c.KVRU < other.KVRU {
		c.KVRU = 0
	} else {
		c.KVRU -= other.KVRU
	}

	if c.ReadBatches < other.ReadBatches {
		c.ReadBatches = 0
	} else {
		c.ReadBatches -= other.ReadBatches
	}

	if c.ReadRequests < other.ReadRequests {
		c.ReadRequests = 0
	} else {
		c.ReadRequests -= other.ReadRequests
	}

	if c.ReadBytes < other.ReadBytes {
		c.ReadBytes = 0
	} else {
		c.ReadBytes -= other.ReadBytes
	}

	if c.WriteBatches < other.WriteBatches {
		c.WriteBatches = 0
	} else {
		c.WriteBatches -= other.WriteBatches
	}

	if c.WriteRequests < other.WriteRequests {
		c.WriteRequests = 0
	} else {
		c.WriteRequests -= other.WriteRequests
	}

	if c.WriteBytes < other.WriteBytes {
		c.WriteBytes = 0
	} else {
		c.WriteBytes -= other.WriteBytes
	}

	if c.SQLPodsCPUSeconds < other.SQLPodsCPUSeconds {
		c.SQLPodsCPUSeconds = 0
	} else {
		c.SQLPodsCPUSeconds -= other.SQLPodsCPUSeconds
	}

	if c.PGWireEgressBytes < other.PGWireEgressBytes {
		c.PGWireEgressBytes = 0
	} else {
		c.PGWireEgressBytes -= other.PGWireEgressBytes
	}

	if c.ExternalIOEgressBytes < other.ExternalIOEgressBytes {
		c.ExternalIOEgressBytes = 0
	} else {
		c.ExternalIOEgressBytes -= other.ExternalIOEgressBytes
	}

	if c.ExternalIOIngressBytes < other.ExternalIOIngressBytes {
		c.ExternalIOIngressBytes = 0
	} else {
		c.ExternalIOIngressBytes -= other.ExternalIOIngressBytes
	}
}

func humanizePointCount(n uint64) redact.SafeString {
	return redact.SafeString(humanize.SI(float64(n), ""))
}

// SafeFormat implements redact.SafeFormatter.
func (s *ScanStats) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("scan stats: stepped %d times (%d internal); seeked %d times (%d internal); "+
		"block-bytes: (total %s, cached %s); "+
		"points: (count %s, key-bytes %s, value-bytes %s, tombstoned: %s) "+
		"ranges: (count %s), (contained-points %s, skipped-points %s)",
		s.NumInterfaceSteps, s.NumInternalSteps, s.NumInterfaceSeeks, s.NumInternalSeeks,
		humanizeutil.IBytes(int64(s.BlockBytes)),
		humanizeutil.IBytes(int64(s.BlockBytesInCache)),
		humanizePointCount(s.PointCount),
		humanizeutil.IBytes(int64(s.KeyBytes)),
		humanizeutil.IBytes(int64(s.ValueBytes)),
		humanizePointCount(s.PointsCoveredByRangeTombstones),
		humanizePointCount(s.RangeKeyCount),
		humanizePointCount(s.RangeKeyContainedPoints),
		humanizePointCount(s.RangeKeySkippedPoints))
}

// String implements fmt.Stringer.
func (s *ScanStats) String() string {
	return redact.StringWithoutMarkers(s)
}

// TenantSettingsPrecedence identifies the precedence of a set of setting
// overrides. It is used by the TenantSettings API which supports passing
// multiple overrides for the same setting.
type TenantSettingsPrecedence uint32

const (
	// SpecificTenantOverrides is the high precedence for tenant setting overrides.
	// These overrides take precedence over AllTenantsOverrides.
	SpecificTenantOverrides TenantSettingsPrecedence = 1 + iota

	// AllTenantsOverrides is the low precedence for tenant setting overrides.
	// These overrides are only effectual for a tenant if there is no override
	// with the SpecificTenantOverrides precedence..
	AllTenantsOverrides
)

// RangeFeedEventSink is an interface for sending a single rangefeed event.
type RangeFeedEventSink interface {
	Context() context.Context
	Send(*RangeFeedEvent) error
}

// RangeFeedEventProducer is an adapter for receiving rangefeed events with either
// the legacy RangeFeed RPC, or the MuxRangeFeed RPC.
type RangeFeedEventProducer interface {
	// Recv receives the next rangefeed event. an io.EOF error indicates that the
	// range needs to be restarted.
	Recv() (*RangeFeedEvent, error)
}
