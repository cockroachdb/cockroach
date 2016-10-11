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

package kv

import (
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/cockroachdb/cockroach/util/tracing"
)

// Default constants for timeouts.
const (
	// TODO(bdarnell): make SendNextTimeout configurable.
	// https://github.com/cockroachdb/cockroach/issues/6719
	defaultSendNextTimeout = 500 * time.Millisecond
	defaultClientTimeout   = 10 * time.Second

	// The default maximum number of ranges to return from a range
	// lookup.
	defaultRangeLookupMaxRanges = 8
	// The default size of the range lease holder cache.
	defaultLeaseHolderCacheSize = 1 << 16
	// The default size of the range descriptor cache.
	defaultRangeDescriptorCacheSize = 1 << 20
)

// A firstRangeMissingError indicates that the first range has not yet
// been gossiped. This will be the case for a node which hasn't yet
// joined the gossip network.
type firstRangeMissingError struct{}

// Error implements the error interface.
func (f firstRangeMissingError) Error() string {
	return "the descriptor for the first range is not available via gossip"
}

// A DistSender provides methods to access Cockroach's monolithic,
// distributed key value store. Each method invocation triggers a
// lookup or lookups to find replica metadata for implicated key
// ranges. RPCs are sent to one or more of the replicas to satisfy
// the method invocation.
type DistSender struct {
	// Ctx is the "base context" for DistSender, used for log messages that are
	// not associated with a more specific request context. It also holds the
	// Tracer that should be used.
	Ctx context.Context

	// nodeDescriptor, if set, holds the descriptor of the node the
	// DistSender lives on. It should be accessed via getNodeDescriptor(),
	// which tries to obtain the value from the Gossip network if the
	// descriptor is unknown.
	nodeDescriptor unsafe.Pointer
	// clock is used to set time for some calls. E.g. read-only ops
	// which span ranges and don't require read consistency.
	clock *hlc.Clock
	// gossip provides up-to-date information about the start of the
	// key range, used to find the replica metadata for arbitrary key
	// ranges.
	gossip *gossip.Gossip
	// rangeCache caches replica metadata for key ranges.
	rangeCache           *rangeDescriptorCache
	rangeLookupMaxRanges int32
	// leaseHolderCache caches range lease holders by range ID.
	leaseHolderCache *leaseHolderCache
	transportFactory TransportFactory
	rpcContext       *rpc.Context
	rpcRetryOptions  retry.Options
	sendNextTimeout  time.Duration
}

var _ client.Sender = &DistSender{}

// rpcSendFn is the function type used to dispatch RPC calls.
type rpcSendFn func(SendOptions, ReplicaSlice,
	roachpb.BatchRequest, *rpc.Context) (*roachpb.BatchResponse, error)

// DistSenderConfig holds configuration and auxiliary objects that can be passed
// to NewDistSender.
type DistSenderConfig struct {
	// Base context for the DistSender. The context can have a Tracer set. If nil,
	// context.Background() will be used.
	Ctx context.Context

	Clock                    *hlc.Clock
	RangeDescriptorCacheSize int32
	// RangeLookupMaxRanges sets how many ranges will be prefetched into the
	// range descriptor cache when dispatching a range lookup request.
	RangeLookupMaxRanges int32
	LeaseHolderCacheSize int32
	RPCRetryOptions      *retry.Options
	// nodeDescriptor, if provided, is used to describe which node the DistSender
	// lives on, for instance when deciding where to send RPCs.
	// Usually it is filled in from the Gossip network on demand.
	nodeDescriptor *roachpb.NodeDescriptor
	// The RPC dispatcher. Defaults to grpc but can be changed here for testing
	// purposes.
	TransportFactory  TransportFactory
	RPCContext        *rpc.Context
	RangeDescriptorDB RangeDescriptorDB
	SendNextTimeout   time.Duration
}

// NewDistSender returns a batch.Sender instance which connects to the
// Cockroach cluster via the supplied gossip instance. Supplying a
// DistSenderContext or the fields within is optional. For omitted values, sane
// defaults will be used.
func NewDistSender(cfg *DistSenderConfig, g *gossip.Gossip) *DistSender {
	if cfg == nil {
		cfg = &DistSenderConfig{}
	}

	ds := &DistSender{gossip: g}

	ds.Ctx = cfg.Ctx
	if ds.Ctx == nil {
		ds.Ctx = context.Background()
	}

	if ds.Ctx.Done() != nil {
		panic("context with cancel or deadline")
	}

	if tracing.TracerFromCtx(ds.Ctx) == nil {
		ds.Ctx = tracing.WithTracer(ds.Ctx, tracing.NewTracer())
	}

	ds.clock = cfg.Clock
	if ds.clock == nil {
		ds.clock = hlc.NewClock(hlc.UnixNano)
	}

	if cfg.nodeDescriptor != nil {
		atomic.StorePointer(&ds.nodeDescriptor, unsafe.Pointer(cfg.nodeDescriptor))
	}
	rcSize := cfg.RangeDescriptorCacheSize
	if rcSize <= 0 {
		rcSize = defaultRangeDescriptorCacheSize
	}
	rdb := cfg.RangeDescriptorDB
	if rdb == nil {
		rdb = ds
	}
	ds.rangeCache = newRangeDescriptorCache(ds.Ctx, rdb, int(rcSize))
	lcSize := cfg.LeaseHolderCacheSize
	if lcSize <= 0 {
		lcSize = defaultLeaseHolderCacheSize
	}
	ds.leaseHolderCache = newLeaseHolderCache(int(lcSize))
	if cfg.RangeLookupMaxRanges <= 0 {
		ds.rangeLookupMaxRanges = defaultRangeLookupMaxRanges
	}
	if cfg.TransportFactory != nil {
		ds.transportFactory = cfg.TransportFactory
	}
	ds.rpcRetryOptions = base.DefaultRetryOptions()
	if cfg.RPCRetryOptions != nil {
		ds.rpcRetryOptions = *cfg.RPCRetryOptions
	}
	if cfg.RPCContext != nil {
		ds.rpcContext = cfg.RPCContext
		if ds.rpcRetryOptions.Closer == nil {
			ds.rpcRetryOptions.Closer = ds.rpcContext.Stopper.ShouldQuiesce()
		}
	}
	if cfg.SendNextTimeout != 0 {
		ds.sendNextTimeout = cfg.SendNextTimeout
	} else {
		ds.sendNextTimeout = defaultSendNextTimeout
	}

	if g != nil {
		g.RegisterCallback(gossip.KeyFirstRangeDescriptor,
			func(_ string, value roachpb.Value) {
				if log.V(1) {
					var desc roachpb.RangeDescriptor
					if err := value.GetProto(&desc); err != nil {
						log.Errorf(ds.Ctx, "unable to parse gossiped first range descriptor: %s", err)
					} else {
						log.Infof(ds.Ctx,
							"gossiped first range descriptor: %+v", desc.Replicas)
					}
				}
				err := ds.rangeCache.EvictCachedRangeDescriptor(roachpb.RKeyMin, nil, false)
				if err != nil {
					log.Warningf(ds.Ctx, "failed to evict first range descriptor: %s", err)
				}
			})
	}
	return ds
}

// RangeLookup implements the RangeDescriptorDB interface.
// RangeLookup dispatches a RangeLookup request for the given metadata
// key to the replicas of the given range. Note that we allow
// inconsistent reads when doing range lookups for efficiency. Getting
// stale data is not a correctness problem but instead may
// infrequently result in additional latency as additional range
// lookups may be required. Note also that rangeLookup bypasses the
// DistSender's Send() method, so there is no error inspection and
// retry logic here; this is not an issue since the lookup performs a
// single inconsistent read only.
func (ds *DistSender) RangeLookup(
	ctx context.Context, key roachpb.RKey, desc *roachpb.RangeDescriptor, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, *roachpb.Error) {
	ba := roachpb.BatchRequest{}
	ba.ReadConsistency = roachpb.INCONSISTENT
	ba.Add(&roachpb.RangeLookupRequest{
		Span: roachpb.Span{
			// We can interpret the RKey as a Key here since it's a metadata
			// lookup; those are never local.
			Key: key.AsRawKey(),
		},
		MaxRanges: ds.rangeLookupMaxRanges,
		Reverse:   useReverseScan,
	})
	replicas := newReplicaSlice(ds.gossip, desc)
	replicas.Shuffle()
	br, err := ds.sendRPC(ctx, desc.RangeID, replicas, ba)
	if err != nil {
		return nil, nil, roachpb.NewError(err)
	}
	if br.Error != nil {
		return nil, nil, br.Error
	}
	resp := br.Responses[0].GetInner().(*roachpb.RangeLookupResponse)
	return resp.Ranges, resp.PrefetchedRanges, nil
}

// FirstRange implements the RangeDescriptorDB interface.
// FirstRange returns the RangeDescriptor for the first range on the cluster,
// which is retrieved from the gossip protocol instead of the datastore.
func (ds *DistSender) FirstRange() (*roachpb.RangeDescriptor, error) {
	if ds.gossip == nil {
		panic("with `nil` Gossip, DistSender must not use itself as rangeDescriptorDB")
	}
	rangeDesc := &roachpb.RangeDescriptor{}
	if err := ds.gossip.GetInfoProto(gossip.KeyFirstRangeDescriptor, rangeDesc); err != nil {
		return nil, firstRangeMissingError{}
	}
	return rangeDesc, nil
}

// optimizeReplicaOrder sorts the replicas in the order in which they are to be
// used for sending RPCs (meaning in the order in which they'll be probed for
// the lease). "Closer" replicas (matching in more attributes) are ordered
// first. Replicas matching in the same number of attributes are shuffled
// randomly.
// If the current node is a replica, then it'll be the first one.
func (ds *DistSender) optimizeReplicaOrder(replicas ReplicaSlice) {
	// TODO(spencer): going to need to also sort by affinity; closest
	// ping time should win. Makes sense to have the rpc client/server
	// heartbeat measure ping times. With a bit of seasoning, each
	// node will be able to order the healthy replicas based on latency.

	// Unless we know better, send the RPCs randomly.
	nodeDesc := ds.getNodeDescriptor()
	// If we don't know which node we're on, don't optimize anything.
	if nodeDesc == nil {
		replicas.Shuffle()
		return
	}
	// Sort replicas by attribute affinity (if any), which we treat as a stand-in
	// for proximity (for now).
	replicas.SortByCommonAttributePrefix(nodeDesc.Attrs.Attrs)

	// If there is a replica in local node, move it to the front.
	if i := replicas.FindReplicaByNodeID(nodeDesc.NodeID); i > 0 {
		replicas.MoveToFront(i)
	}
}

// getNodeDescriptor returns ds.nodeDescriptor, but makes an attempt to load
// it from the Gossip network if a nil value is found.
// We must jump through hoops here to get the node descriptor because it's not available
// until after the node has joined the gossip network and been allowed to initialize
// its stores.
func (ds *DistSender) getNodeDescriptor() *roachpb.NodeDescriptor {
	if desc := atomic.LoadPointer(&ds.nodeDescriptor); desc != nil {
		return (*roachpb.NodeDescriptor)(desc)
	}
	if ds.gossip == nil {
		return nil
	}

	ownNodeID := ds.gossip.GetNodeID()
	if ownNodeID > 0 {
		// TODO(tschottdorf): Consider instead adding the NodeID of the
		// coordinator to the header, so we can get this from incoming
		// requests. Just in case we want to mostly eliminate gossip here.
		nodeDesc := &roachpb.NodeDescriptor{}
		if err := ds.gossip.GetInfoProto(gossip.MakeNodeIDKey(ownNodeID), nodeDesc); err == nil {
			atomic.StorePointer(&ds.nodeDescriptor, unsafe.Pointer(nodeDesc))
			return nodeDesc
		}
	}
	log.Infof(ds.Ctx, "unable to determine this node's attributes for replica "+
		"selection; node is most likely bootstrapping")
	return nil
}

// sendRPC sends one or more RPCs to replicas from the supplied roachpb.Replica
// slice. Returns an RPC error if the request could not be sent. Note
// that the reply may contain a higher level error and must be checked in
// addition to the RPC error.
// The replicas are assume to have been ordered by preference, closer ones (if
// any) at the front.
func (ds *DistSender) sendRPC(
	ctx context.Context, rangeID roachpb.RangeID, replicas ReplicaSlice, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	if len(replicas) == 0 {
		return nil, roachpb.NewSendError(
			fmt.Sprintf("no replica node addresses available via gossip for range %d", rangeID))
	}

	// TODO(pmattis): This needs to be tested. If it isn't set we'll
	// still route the request appropriately by key, but won't receive
	// RangeNotFoundErrors.
	ba.RangeID = rangeID

	// Set RPC opts with stipulation that one of N RPCs must succeed.
	rpcOpts := SendOptions{
		ctx:              ctx,
		SendNextTimeout:  ds.sendNextTimeout,
		transportFactory: ds.transportFactory,
	}
	tracing.AnnotateTrace()
	defer tracing.AnnotateTrace()

	reply, err := ds.sendToReplicas(rpcOpts, rangeID, replicas, ba, ds.rpcContext)
	if err != nil {
		return nil, err
	}
	return reply, nil
}

// CountRanges returns the number of ranges that encompass the given key span.
func (ds *DistSender) CountRanges(rs roachpb.RSpan) (int64, error) {
	var count int64
	for {
		desc, needAnother, _, err := ds.getDescriptors(
			context.Background(), rs, nil, false /*useReverseScan*/)
		if err != nil {
			return -1, err
		}
		count++
		if !needAnother {
			break
		}
		rs.Key = desc.EndKey
	}
	return count, nil
}

// getDescriptors looks up the range descriptor to use for a query over the
// key range span rs with the given options. The lookup takes into consideration
// the last range descriptor that the caller had used for this key range span,
// if any, and if the last range descriptor has been evicted because it was
// found to be stale, which is all managed through the evictionToken. The
// function should be provided with an evictionToken if one was acquired from
// this function on a previous call. If not, an empty evictionToken can be provided.
//
// The range descriptor which contains the range in which the request should
// start its query is returned first. Next returned is an evictionToken. In
// case the descriptor is discovered stale, the returned evictionToken's evict
// method should be called; it evicts the cache appropriately. Finally, the
// returned bool is true in case the given range reaches outside the returned
// descriptor.
func (ds *DistSender) getDescriptors(
	ctx context.Context, rs roachpb.RSpan, evictToken *evictionToken, useReverseScan bool,
) (*roachpb.RangeDescriptor, bool, *evictionToken, error) {
	var descKey roachpb.RKey
	if !useReverseScan {
		descKey = rs.Key
	} else {
		descKey = rs.EndKey
	}

	desc, returnToken, err := ds.rangeCache.LookupRangeDescriptor(
		ctx, descKey, evictToken, useReverseScan,
	)
	if err != nil {
		return nil, false, returnToken, err
	}

	// Checks whether need to get next range descriptor.
	var needAnother bool
	if useReverseScan {
		needAnother = rs.Key.Less(desc.StartKey)
	} else {
		needAnother = desc.EndKey.Less(rs.EndKey)
	}

	return desc, needAnother, returnToken, nil
}

// sendSingleRange gathers and rearranges the replicas, and makes an RPC call.
func (ds *DistSender) sendSingleRange(
	ctx context.Context, ba roachpb.BatchRequest, desc *roachpb.RangeDescriptor,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// Try to send the call.
	replicas := newReplicaSlice(ds.gossip, desc)

	// Rearrange the replicas so that those replicas with long common
	// prefix of attributes end up first. If there's no prefix, this is a
	// no-op.
	ds.optimizeReplicaOrder(replicas)

	// If this request needs to go to a lease holder and we know who that is, move
	// it to the front.
	if !(ba.IsReadOnly() && ba.ReadConsistency == roachpb.INCONSISTENT) {
		if leaseHolder, ok := ds.leaseHolderCache.Lookup(desc.RangeID); ok {
			if i := replicas.FindReplica(leaseHolder.StoreID); i >= 0 {
				replicas.MoveToFront(i)
			}
		}
	}

	// TODO(tschottdorf): should serialize the trace here, not higher up.
	br, err := ds.sendRPC(ctx, desc.RangeID, replicas, ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// If the reply contains a timestamp, update the local HLC with it.
	if br.Error != nil && br.Error.Now != hlc.ZeroTimestamp {
		ds.clock.Update(br.Error.Now)
	} else if br.Now != hlc.ZeroTimestamp {
		ds.clock.Update(br.Now)
	}

	// Untangle the error from the received response.
	pErr := br.Error
	br.Error = nil // scrub the response error
	return br, pErr
}

// Send implements the batch.Sender interface. It subdivides
// the Batch into batches admissible for sending (preventing certain
// illegal mixtures of requests), executes each individual part
// (which may span multiple ranges), and recombines the response.
// When the request spans ranges, it is split up and the corresponding
// ranges queried serially, in ascending order.
// In particular, the first write in a transaction may not be part of the first
// request sent. This is relevant since the first write is a BeginTransaction
// request, thus opening up a window of time during which there may be intents
// of a transaction, but no entry. Pushing such a transaction will succeed, and
// may lead to the transaction being aborted early.
func (ds *DistSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	tracing.AnnotateTrace()

	// In the event that timestamp isn't set and read consistency isn't
	// required, set the timestamp using the local clock.
	if ba.ReadConsistency == roachpb.INCONSISTENT && ba.Timestamp.Equal(hlc.ZeroTimestamp) {
		ba.Timestamp = ds.clock.Now()
	}

	if ba.Txn != nil {
		// Make a copy here since the code below modifies it in different places.
		// TODO(tschottdorf): be smarter about this - no need to do it for
		// requests that don't get split.
		txnClone := ba.Txn.Clone()
		ba.Txn = &txnClone

		if len(ba.Txn.ObservedTimestamps) == 0 {
			// Ensure the local NodeID is marked as free from clock offset;
			// the transaction's timestamp was taken off the local clock.
			if nDesc := ds.getNodeDescriptor(); nDesc != nil {
				// TODO(tschottdorf): future refactoring should move this to txn
				// creation in TxnCoordSender, which is currently unaware of the
				// NodeID (and wraps *DistSender through client.Sender since it
				// also needs test compatibility with *LocalSender).
				//
				// Taking care below to not modify any memory referenced from
				// our BatchRequest which may be shared with others.
				//
				// We already have a clone of our txn (see above), so we can
				// modify it freely.
				//
				// Zero the existing data. That makes sure that if we had
				// something of size zero but with capacity, we don't re-use the
				// existing space (which others may also use). This is just to
				// satisfy paranoia/OCD and not expected to matter in practice.
				ba.Txn.ResetObservedTimestamps()
				// OrigTimestamp is the HLC timestamp at which the Txn started, so
				// this effectively means no more uncertainty on this node.
				ba.Txn.UpdateObservedTimestamp(nDesc.NodeID, ba.Txn.OrigTimestamp)
			}
		}
	}

	if len(ba.Requests) < 1 {
		panic("empty batch")
	}

	if ba.MaxSpanRequestKeys != 0 {
		// Verify that the batch contains only specific range requests or the
		// Begin/EndTransactionRequest. Verify that a batch with a ReverseScan
		// only contains ReverseScan range requests.
		isReverse := ba.IsReverse()
		for _, req := range ba.Requests {
			inner := req.GetInner()
			switch inner.(type) {
			case *roachpb.ScanRequest, *roachpb.DeleteRangeRequest:
				// Accepted range requests. All other range requests are still
				// not supported.
				// TODO(vivek): don't enumerate all range requests.
				if isReverse {
					return nil, roachpb.NewErrorf("batch with limit contains both forward and reverse scans")
				}

			case *roachpb.BeginTransactionRequest, *roachpb.EndTransactionRequest, *roachpb.ReverseScanRequest:
				continue

			default:
				return nil, roachpb.NewErrorf("batch with limit contains %T request", inner)
			}
		}
	}

	var rplChunks []*roachpb.BatchResponse
	parts := ba.Split(false /* don't split ET */)
	if len(parts) > 1 && ba.MaxSpanRequestKeys != 0 {
		// We already verified above that the batch contains only scan requests of the same type.
		// Such a batch should never need splitting.
		panic("batch with MaxSpanRequestKeys needs splitting")
	}
	for len(parts) > 0 {
		part := parts[0]
		ba.Requests = part
		rpl, pErr, shouldSplitET := ds.sendChunk(ctx, ba)
		if shouldSplitET {
			// If we tried to send a single round-trip EndTransaction but
			// it looks like it's going to hit multiple ranges, split it
			// here and try again.
			if len(parts) != 1 {
				panic("EndTransaction not in last chunk of batch")
			}
			parts = ba.Split(true /* split ET */)
			if len(parts) != 2 {
				panic("split of final EndTransaction chunk resulted in != 2 parts")
			}
			continue
		}
		if pErr != nil {
			return nil, pErr
		}
		// Propagate transaction from last reply to next request. The final
		// update is taken and put into the response's main header.
		ba.UpdateTxn(rpl.Txn)
		rplChunks = append(rplChunks, rpl)
		parts = parts[1:]
	}

	reply := rplChunks[0]
	for _, rpl := range rplChunks[1:] {
		reply.Responses = append(reply.Responses, rpl.Responses...)
		reply.CollectedSpans = append(reply.CollectedSpans, rpl.CollectedSpans...)
	}
	reply.BatchResponse_Header = rplChunks[len(rplChunks)-1].BatchResponse_Header
	return reply, nil
}

// sendChunk is in charge of sending an "admissible" piece of batch, i.e. one
// which doesn't need to be subdivided further before going to a range (so no
// mixing of forward and reverse scans, etc). The parameters and return values
// correspond to client.Sender with the exception of the returned boolean,
// which is true when indicating that the caller should retry but needs to send
// EndTransaction in a separate request.
func (ds *DistSender) sendChunk(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error, bool) {
	isReverse := ba.IsReverse()

	// TODO(radu): when contexts are properly plumbed, we should be able to get
	// the tracer from ctx, not from the DistSender.
	ctx, cleanup := tracing.EnsureContext(ctx, tracing.TracerFromCtx(ds.Ctx))
	defer cleanup()

	// The minimal key range encompassing all requests contained within.
	// Local addressing has already been resolved.
	// TODO(tschottdorf): consider rudimentary validation of the batch here
	// (for example, non-range requests with EndKey, or empty key ranges).
	rs, err := keys.Range(ba)
	if err != nil {
		return nil, roachpb.NewError(err), false
	}
	var br *roachpb.BatchResponse

	// Send the request to one range per iteration.
	for {
		// Increase the sequence counter only once before sending RPCs to
		// the ranges involved in this chunk of the batch (as opposed to for
		// each RPC individually). On RPC errors, there's no guarantee that
		// the request hasn't made its way to the target regardless of the
		// error; we'd like the second execution to be caught by the sequence
		// cache if that happens. There is a small chance that that we address
		// a range twice in this chunk (stale/suboptimal descriptors due to
		// splits/merges) which leads to a transaction retry.
		// TODO(tschottdorf): it's possible that if we don't evict from the
		//   cache we could be in for a busy loop.
		ba.SetNewRequest()

		var curReply *roachpb.BatchResponse
		var desc *roachpb.RangeDescriptor
		var evictToken *evictionToken
		var needAnother bool
		var pErr *roachpb.Error
		var finished bool
		for r := retry.StartWithCtx(ctx, ds.rpcRetryOptions); r.Next(); {
			// Get range descriptor (or, when spanning range, descriptors). Our
			// error handling below may clear them on certain errors, so we
			// refresh (likely from the cache) on every retry.
			var err error
			desc, needAnother, evictToken, err = ds.getDescriptors(ctx, rs, evictToken, isReverse)

			// getDescriptors may fail retryably if, for example, the first
			// range isn't available via Gossip. Assume that all errors at
			// this level are retryable. Non-retryable errors would be for
			// things like malformed requests which we should have checked
			// for before reaching this point.
			if err != nil {
				log.Event(ctx, "range descriptor lookup failed: "+err.Error())
				if log.V(1) {
					log.Warning(ctx, err)
				}
				pErr = roachpb.NewError(err)
				continue
			}

			if needAnother && br == nil {
				// TODO(tschottdorf): we should have a mechanism for discovering
				// range merges (descriptor staleness will mostly go unnoticed),
				// or we'll be turning single-range queries into multi-range
				// queries for no good reason.

				// If there's no transaction and op spans ranges, possibly
				// re-run as part of a transaction for consistency. The
				// case where we don't need to re-run is if the read
				// consistency is not required.
				if ba.Txn == nil && ba.IsPossibleTransaction() &&
					ba.ReadConsistency != roachpb.INCONSISTENT {
					return nil, roachpb.NewError(&roachpb.OpRequiresTxnError{}), false
				}
				// If the request is more than but ends with EndTransaction, we
				// want the caller to come again with the EndTransaction in an
				// extra call.
				if l := len(ba.Requests) - 1; l > 0 && ba.Requests[l].GetInner().Method() == roachpb.EndTransaction {
					return nil, roachpb.NewError(errors.New("cannot send 1PC txn to multiple ranges")), true /* shouldSplitET */
				}
			}

			// It's possible that the returned descriptor misses parts of the
			// keys it's supposed to scan after it's truncated to match the
			// descriptor. Example revscan [a,g), first desc lookup for "g"
			// returns descriptor [c,d) -> [d,g) is never scanned.
			// We evict and retry in such a case.
			frontOfCurSpan := rs.Key
			containsFn := (*roachpb.RangeDescriptor).ContainsKey
			if isReverse {
				frontOfCurSpan = rs.EndKey
				containsFn = (*roachpb.RangeDescriptor).ContainsExclusiveEndKey
			}

			if !containsFn(desc, frontOfCurSpan) {
				if err := evictToken.Evict(ctx); err != nil {
					return nil, roachpb.NewError(err), false
				}
				// On addressing errors, don't backoff; retry immediately.
				r.Reset()
				log.VEventf(1, ctx,
					"addressing error: %s not appropriate for remaining range %s",
					desc, rs)
				continue
			}

			curReply, pErr = func() (*roachpb.BatchResponse, *roachpb.Error) {
				// Truncate the request to our current key range.
				intersected, iErr := rs.Intersect(desc)
				if iErr != nil {
					return nil, roachpb.NewError(iErr)
				}
				truncBA, numActive, trErr := truncate(ba, intersected)
				if numActive == 0 && trErr == nil {
					// This shouldn't happen in the wild, but some tests
					// exercise it.
					return nil, roachpb.NewErrorf("truncation resulted in empty batch on [%s,%s): %s",
						rs.Key, rs.EndKey, ba)
				}
				if trErr != nil {
					return nil, roachpb.NewError(trErr)
				}
				return ds.sendSingleRange(ctx, truncBA, desc)
			}()
			// If sending succeeded, break this loop.
			if pErr == nil {
				finished = true
				break
			}

			log.VEventf(1, ctx, "reply error %s: %s", ba, pErr)

			// Error handling: If the error indicates that our range
			// descriptor is out of date, evict it from the cache and try
			// again. Errors that apply only to a single replica were
			// handled in send().
			//
			// TODO(bdarnell): Don't retry endlessly. If we fail twice in a
			// row and the range descriptor hasn't changed, return the error
			// to our caller.
			switch tErr := pErr.GetDetail().(type) {
			case *roachpb.SendError:
				// We've tried all the replicas without success. Either
				// they're all down, or we're using an out-of-date range
				// descriptor. Invalidate the cache and try again with the new
				// metadata.
				if err := evictToken.Evict(ctx); err != nil {
					return nil, roachpb.NewError(err), false
				}
				continue
			case *roachpb.RangeKeyMismatchError:
				// Range descriptor might be out of date - evict it. This is
				// likely the result of a range split. If we have new range
				// descriptors, insert them instead as long as they are different
				// from the last descriptor to avoid endless loops.
				var replacements []roachpb.RangeDescriptor
				different := func(rd *roachpb.RangeDescriptor) bool {
					return !desc.RSpan().Equal(rd.RSpan())
				}
				if tErr.MismatchedRange != nil && different(tErr.MismatchedRange) {
					replacements = append(replacements, *tErr.MismatchedRange)
				}
				if tErr.SuggestedRange != nil && different(tErr.SuggestedRange) {
					if containsFn(tErr.SuggestedRange, frontOfCurSpan) {
						replacements = append(replacements, *tErr.SuggestedRange)

					}
				}
				// Same as Evict() if replacements is empty.
				if err := evictToken.EvictAndReplace(ctx, replacements...); err != nil {
					return nil, roachpb.NewError(err), false
				}
				// On addressing errors, don't backoff; retry immediately.
				r.Reset()
				if log.V(1) {
					log.Warning(ctx, tErr)
				}
				continue
			}
			break
		}

		// Immediately return if querying a range failed non-retryably.
		if pErr != nil {
			log.Eventf(ctx, "non-retryable failure: %s", pErr)
			return nil, pErr, false
		} else if !finished {
			log.Event(ctx, "DistSender gave up")
			select {
			case <-ds.rpcRetryOptions.Closer:
				return nil, roachpb.NewError(&roachpb.NodeUnavailableError{}), false
			case <-ctx.Done():
				return nil, roachpb.NewError(ctx.Err()), false
			default:
				log.Fatal(ctx, "exited retry loop with nil error but finished=false")
			}
		}

		ba.UpdateTxn(curReply.Txn)

		if br == nil {
			// First response from a Range.
			br = curReply
		} else {
			// This was the second or later call in a cross-Range request.
			// Combine the new response with the existing one.
			if err := br.Combine(curReply); err != nil {
				return nil, roachpb.NewError(err), false
			}
		}

		if isReverse {
			// In next iteration, query previous range.
			// We use the StartKey of the current descriptor as opposed to the
			// EndKey of the previous one since that doesn't have bugs when
			// stale descriptors come into play.
			rs.EndKey, err = prev(ba, desc.StartKey)
		} else {
			// In next iteration, query next range.
			// It's important that we use the EndKey of the current descriptor
			// as opposed to the StartKey of the next one: if the former is stale,
			// it's possible that the next range has since merged the subsequent
			// one, and unless both descriptors are stale, the next descriptor's
			// StartKey would move us to the beginning of the current range,
			// resulting in a duplicate scan.
			rs.Key, err = next(ba, desc.EndKey)
		}
		if err != nil {
			return nil, roachpb.NewError(err), false
		}

		if ba.MaxSpanRequestKeys > 0 {
			// Count how many results we received.
			var numResults int64
			for _, resp := range curReply.Responses {
				numResults += resp.GetInner().Header().NumKeys
			}
			if numResults > ba.MaxSpanRequestKeys {
				panic(fmt.Sprintf("received %d results, limit was %d", numResults, ba.MaxSpanRequestKeys))
			}
			ba.MaxSpanRequestKeys -= numResults
			if ba.MaxSpanRequestKeys == 0 {
				// prepare the batch response after meeting the max key limit.
				fillSkippedResponses(ba, br, rs)
				// done, exit loop.
				log.Event(ctx, "request is saturated")
				return br, nil, false
			}
		}

		// If this was the last range accessed by this call, exit loop.
		if !needAnother {
			return br, nil, false
		}

		// key cannot be less that the end key.
		if !rs.Key.Less(rs.EndKey) {
			panic(fmt.Sprintf("start key %s is less than %s", rs.Key, rs.EndKey))
		}

		log.Event(ctx, "querying next range")
	}
}

// fillSkippedResponses after meeting the batch key max limit for range
// requests.
func fillSkippedResponses(
	ba roachpb.BatchRequest, br *roachpb.BatchResponse, nextSpan roachpb.RSpan,
) {
	// Some requests might have NoopResponses; we must replace them with empty
	// responses of the proper type.
	for i, req := range ba.Requests {
		if _, ok := br.Responses[i].GetInner().(*roachpb.NoopResponse); !ok {
			continue
		}
		var reply roachpb.Response
		switch t := req.GetInner().(type) {
		case *roachpb.ScanRequest:
			reply = &roachpb.ScanResponse{}

		case *roachpb.ReverseScanRequest:
			reply = &roachpb.ReverseScanResponse{}

		case *roachpb.DeleteRangeRequest:
			reply = &roachpb.DeleteRangeResponse{}

		case *roachpb.BeginTransactionRequest, *roachpb.EndTransactionRequest:
			continue

		default:
			panic(fmt.Sprintf("bad type %T", t))
		}
		union := roachpb.ResponseUnion{}
		union.MustSetInner(reply)
		br.Responses[i] = union
	}
	// Set the ResumeSpan for future batch requests.
	isReverse := ba.IsReverse()
	for i, resp := range br.Responses {
		req := ba.Requests[i].GetInner()
		if !roachpb.IsRange(req) {
			continue
		}
		hdr := resp.GetInner().Header()
		origSpan := req.Header()
		if isReverse {
			if hdr.ResumeSpan != nil {
				// The ResumeSpan.Key might be set to the StartKey of a range;
				// correctly set it to the Key of the original request span.
				hdr.ResumeSpan.Key = origSpan.Key
			} else if roachpb.RKey(origSpan.Key).Less(nextSpan.EndKey) {
				// Some keys have yet to be processed.
				hdr.ResumeSpan = &origSpan
				if nextSpan.EndKey.Less(roachpb.RKey(origSpan.EndKey)) {
					// The original span has been partially processed.
					hdr.ResumeSpan.EndKey = nextSpan.EndKey.AsRawKey()
				}
			}
		} else {
			if hdr.ResumeSpan != nil {
				// The ResumeSpan.EndKey might be set to the EndKey of a
				// range; correctly set it to the EndKey of the original
				// request span.
				hdr.ResumeSpan.EndKey = origSpan.EndKey
			} else if nextSpan.Key.Less(roachpb.RKey(origSpan.EndKey)) {
				// Some keys have yet to be processed.
				hdr.ResumeSpan = &origSpan
				if roachpb.RKey(origSpan.Key).Less(nextSpan.Key) {
					// The original span has been partially processed.
					hdr.ResumeSpan.Key = nextSpan.Key.AsRawKey()
				}
			}
		}
		br.Responses[i].GetInner().SetHeader(hdr)
	}
}

// sendToReplicas sends one or more RPCs to clients specified by the slice of
// replicas. On success, Send returns the first successful reply. Otherwise,
// Send returns an error if and as soon as the number of failed RPCs exceeds
// the available endpoints less the number of required replies.
func (ds *DistSender) sendToReplicas(
	opts SendOptions,
	rangeID roachpb.RangeID,
	replicas ReplicaSlice,
	args roachpb.BatchRequest,
	rpcContext *rpc.Context,
) (*roachpb.BatchResponse, error) {
	if len(replicas) < 1 {
		return nil, roachpb.NewSendError(
			fmt.Sprintf("insufficient replicas (%d) to satisfy send request of %d",
				len(replicas), 1))
	}

	done := make(chan BatchCall, len(replicas))

	transportFactory := opts.transportFactory
	if transportFactory == nil {
		transportFactory = grpcTransportFactory
	}
	transport, err := transportFactory(opts, rpcContext, replicas, args)
	if err != nil {
		return nil, err
	}
	defer transport.Close()
	if transport.IsExhausted() {
		return nil, roachpb.NewSendError(
			fmt.Sprintf("sending to all %d replicas failed", len(replicas)))
	}

	// Send the first request.
	pending := 1
	log.VEventf(2, opts.ctx, "sending RPC for batch: %s", args)
	transport.SendNext(done)

	// Wait for completions. This loop will retry operations that fail
	// with errors that reflect per-replica state and may succeed on
	// other replicas.
	var sendNextTimer timeutil.Timer
	defer sendNextTimer.Stop()
	for {
		sendNextTimer.Reset(opts.SendNextTimeout)
		select {
		case <-sendNextTimer.C:
			sendNextTimer.Read = true
			// On successive RPC timeouts, send to additional replicas if available.
			if !transport.IsExhausted() {
				log.VEventf(2, opts.ctx, "timeout, trying next peer")
				pending++
				transport.SendNext(done)
			}

		case call := <-done:
			pending--
			err := call.Err
			if err == nil {
				if log.V(2) {
					log.Infof(opts.ctx, "RPC reply: %s", call.Reply)
				} else if log.V(1) && call.Reply.Error != nil {
					log.Infof(opts.ctx, "application error: %s", call.Reply.Error)
				}

				if !ds.handlePerReplicaError(rangeID, call.Reply.Error) {
					return call.Reply, nil
				}

				// Extract the detail so it can be included in the error
				// message if this is our last replica.
				//
				// TODO(bdarnell): The last error is not necessarily the best
				// one to return; we may want to remember the "best" error
				// we've seen (for example, a NotLeaseHolderError conveys more
				// information than a RangeNotFound).
				err = call.Reply.Error.GoError()
			} else if log.V(1) {
				log.Warningf(opts.ctx, "RPC error: %s", err)
			}

			// Send to additional replicas if available.
			if !transport.IsExhausted() {
				log.VEventf(2, opts.ctx, "error, trying next peer: %s", err)
				pending++
				transport.SendNext(done)
			}
			if pending == 0 {
				log.VEventf(2, opts.ctx,
					"sending to all %d replicas failed; last error: %s",
					len(replicas), err)
				return nil, roachpb.NewSendError(
					fmt.Sprintf("sending to all %d replicas failed; last error: %v",
						len(replicas), err))
			}
		}
	}
}

// handlePerReplicaError returns true if the given error is likely to
// be unique to the replica that reported it, and retrying on other
// replicas is likely to produce different results. This method should
// be called only once for each error as it may have side effects such
// as updating caches.
func (ds *DistSender) handlePerReplicaError(rangeID roachpb.RangeID, pErr *roachpb.Error) bool {
	switch tErr := pErr.GetDetail().(type) {
	case *roachpb.RangeNotFoundError:
		return true
	case *roachpb.NodeUnavailableError:
		return true
	case *roachpb.NotLeaseHolderError:
		if tErr.LeaseHolder != nil {
			// If the replica we contacted knows the new lease holder, update the cache.
			ds.updateLeaseHolderCache(rangeID, *tErr.LeaseHolder)

			// TODO(bdarnell): Move the new lease holder to the head of the queue
			// for the next retry.
		}
		return true
	}
	return false
}

// updateLeaseHolderCache updates the cached lease holder for the given range.
func (ds *DistSender) updateLeaseHolderCache(
	rangeID roachpb.RangeID, newLeaseHolder roachpb.ReplicaDescriptor,
) {
	if log.V(1) {
		if oldLeaseHolder, ok := ds.leaseHolderCache.Lookup(rangeID); ok {
			if (newLeaseHolder == roachpb.ReplicaDescriptor{}) {
				log.Infof(ds.Ctx, "range %d: evicting cached lease holder %+v", rangeID, oldLeaseHolder)
			} else if newLeaseHolder != oldLeaseHolder {
				log.Infof(ds.Ctx, "range %d: replacing cached lease holder %+v with %+v", rangeID, oldLeaseHolder, newLeaseHolder)
			}
		} else {
			log.Infof(ds.Ctx, "range %d: caching new lease holder %+v", rangeID, newLeaseHolder)
		}
	}
	ds.leaseHolderCache.Update(rangeID, newLeaseHolder)
}
