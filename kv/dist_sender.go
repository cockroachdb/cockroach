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

package kv

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/tracer"

	gogoproto "github.com/gogo/protobuf/proto"
)

// Default constants for timeouts.
const (
	defaultSendNextTimeout = 500 * time.Millisecond
	defaultRPCTimeout      = 5 * time.Second
	defaultClientTimeout   = 10 * time.Second
	retryBackoff           = 250 * time.Millisecond
	maxRetryBackoff        = 30 * time.Second

	// The default maximum number of ranges to return from a range
	// lookup.
	defaultRangeLookupMaxRanges = 8
	// The default size of the leader cache.
	defaultLeaderCacheSize = 1 << 16
	// The default size of the range descriptor cache.
	defaultRangeDescriptorCacheSize = 1 << 20
)

var defaultRPCRetryOptions = retry.Options{
	InitialBackoff: retryBackoff,
	MaxBackoff:     maxRetryBackoff,
	Multiplier:     2,
}

// A firstRangeMissingError indicates that the first range has not yet
// been gossiped. This will be the case for a node which hasn't yet
// joined the gossip network.
type firstRangeMissingError struct{}

// Error implements the error interface.
func (f firstRangeMissingError) Error() string {
	return "the descriptor for the first range is not available via gossip"
}

// CanRetry implements the retry.Retryable interface.
func (f firstRangeMissingError) CanRetry() bool { return true }

// A noNodesAvailError specifies that no node addresses in a replica set
// were available via the gossip network.
type noNodeAddrsAvailError struct{}

// Error implements the error interface.
func (n noNodeAddrsAvailError) Error() string {
	return "no replica node addresses available via gossip"
}

// CanRetry implements the retry.Retryable interface.
func (n noNodeAddrsAvailError) CanRetry() bool { return true }

// A DistSender provides methods to access Cockroach's monolithic,
// distributed key value store. Each method invocation triggers a
// lookup or lookups to find replica metadata for implicated key
// ranges. RPCs are sent to one or more of the replicas to satisfy
// the method invocation.
type DistSender struct {
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
	// leaderCache caches the last known leader replica for range
	// consensus groups.
	leaderCache *leaderCache
	// RPCSend is used to send RPC calls and defaults to rpc.Send
	// outside of tests.
	rpcSend         rpcSendFn
	rpcRetryOptions retry.Options
}

var _ client.Sender = &DistSender{}

// rpcSendFn is the function type used to dispatch RPC calls.
type rpcSendFn func(rpc.Options, string, []net.Addr,
	func(addr net.Addr) gogoproto.Message, func() gogoproto.Message,
	*rpc.Context) ([]gogoproto.Message, error)

// DistSenderContext holds auxiliary objects that can be passed to
// NewDistSender.
type DistSenderContext struct {
	Clock                    *hlc.Clock
	RangeDescriptorCacheSize int32
	// RangeLookupMaxRanges sets how many ranges will be prefetched into the
	// range descriptor cache when dispatching a range lookup request.
	RangeLookupMaxRanges int32
	LeaderCacheSize      int32
	RPCRetryOptions      *retry.Options
	// nodeDescriptor, if provided, is used to describe which node the DistSender
	// lives on, for instance when deciding where to send RPCs.
	// Usually it is filled in from the Gossip network on demand.
	nodeDescriptor *proto.NodeDescriptor
	// The RPC dispatcher. Defaults to rpc.Send but can be changed here
	// for testing purposes.
	RPCSend           rpcSendFn
	RangeDescriptorDB rangeDescriptorDB
}

// NewDistSender returns a batch.Sender instance which connects to the
// Cockroach cluster via the supplied gossip instance. Supplying a
// DistSenderContext or the fields within is optional. For omitted values, sane
// defaults will be used.
func NewDistSender(ctx *DistSenderContext, gossip *gossip.Gossip) *DistSender {
	if ctx == nil {
		ctx = &DistSenderContext{}
	}
	clock := ctx.Clock
	if clock == nil {
		clock = hlc.NewClock(hlc.UnixNano)
	}
	ds := &DistSender{
		clock:  clock,
		gossip: gossip,
	}
	if ctx.nodeDescriptor != nil {
		atomic.StorePointer(&ds.nodeDescriptor, unsafe.Pointer(ctx.nodeDescriptor))
	}
	rcSize := ctx.RangeDescriptorCacheSize
	if rcSize <= 0 {
		rcSize = defaultRangeDescriptorCacheSize
	}
	rdb := ctx.RangeDescriptorDB
	if rdb == nil {
		rdb = ds
	}
	ds.rangeCache = newRangeDescriptorCache(rdb, int(rcSize))
	lcSize := ctx.LeaderCacheSize
	if lcSize <= 0 {
		lcSize = defaultLeaderCacheSize
	}
	ds.leaderCache = newLeaderCache(int(lcSize))
	if ctx.RangeLookupMaxRanges <= 0 {
		ds.rangeLookupMaxRanges = defaultRangeLookupMaxRanges
	}
	ds.rpcSend = rpc.Send
	if ctx.RPCSend != nil {
		ds.rpcSend = ctx.RPCSend
	}
	ds.rpcRetryOptions = defaultRPCRetryOptions
	if ctx.RPCRetryOptions != nil {
		ds.rpcRetryOptions = *ctx.RPCRetryOptions
	}

	return ds
}

// lookupOptions capture additional options to pass to RangeLookup.
type lookupOptions struct {
	considerIntents bool
	useReverseScan  bool
}

// rangeLookup dispatches an RangeLookup request for the given
// metadata key to the replicas of the given range. Note that we allow
// inconsistent reads when doing range lookups for efficiency. Getting
// stale data is not a correctness problem but instead may
// infrequently result in additional latency as additional range
// lookups may be required. Note also that rangeLookup bypasses the
// DistSender's Send() method, so there is no error inspection and
// retry logic here; this is not an issue since the lookup performs a
// single inconsistent read only.
func (ds *DistSender) rangeLookup(key proto.Key, options lookupOptions,
	desc *proto.RangeDescriptor) ([]proto.RangeDescriptor, error) {
	ba := proto.BatchRequest{}
	ba.ReadConsistency = proto.INCONSISTENT
	ba.Add(&proto.RangeLookupRequest{
		RequestHeader: proto.RequestHeader{
			Key:             key,
			ReadConsistency: proto.INCONSISTENT,
		},
		MaxRanges:       ds.rangeLookupMaxRanges,
		ConsiderIntents: options.considerIntents,
		Reverse:         options.useReverseScan,
	})
	replicas := newReplicaSlice(ds.gossip, desc)
	// TODO(tschottdorf) consider a Trace here, potentially that of the request
	// that had the cache miss and waits for the result.
	reply, err := ds.sendRPC(nil /* Trace */, desc.RangeID, replicas, rpc.OrderRandom, ba)
	if err != nil {
		return nil, err
	}
	br := reply.(*proto.BatchResponse)
	if err := br.GoError(); err != nil {
		return nil, err
	}
	return br.Responses[0].GetInner().(*proto.RangeLookupResponse).Ranges, nil
}

// firstRange returns the RangeDescriptor for the first range on the cluster,
// which is retrieved from the gossip protocol instead of the datastore.
func (ds *DistSender) firstRange() (*proto.RangeDescriptor, error) {
	if ds.gossip == nil {
		panic("with `nil` Gossip, DistSender must not use itself as rangeDescriptorDB")
	}
	rangeDesc := &proto.RangeDescriptor{}
	if err := ds.gossip.GetInfoProto(gossip.KeyFirstRangeDescriptor, rangeDesc); err != nil {
		return nil, firstRangeMissingError{}
	}
	return rangeDesc, nil
}

func (ds *DistSender) optimizeReplicaOrder(replicas replicaSlice) rpc.OrderingPolicy {
	// Unless we know better, send the RPCs randomly.
	order := rpc.OrderingPolicy(rpc.OrderRandom)
	nodeDesc := ds.getNodeDescriptor()
	// If we don't know which node we're on, don't optimize anything.
	if nodeDesc == nil {
		return order
	}
	// Sort replicas by attribute affinity, which we treat as a stand-in for
	// proximity (for now).
	if replicas.SortByCommonAttributePrefix(nodeDesc.Attrs.Attrs) > 0 {
		// There's at least some attribute prefix, and we hope that the
		// replicas that come early in the slice are now located close to
		// us and hence better candidates.
		order = rpc.OrderStable
	}
	// If there is a replica in local node, move it to the front.
	if i := replicas.FindReplicaByNodeID(nodeDesc.NodeID); i > 0 {
		replicas.MoveToFront(i)
		order = rpc.OrderStable
	}
	return order
}

// getNodeDescriptor returns ds.nodeDescriptor, but makes an attempt to load
// it from the Gossip network if a nil value is found.
// We must jump through hoops here to get the node descriptor because it's not available
// until after the node has joined the gossip network and been allowed to initialize
// its stores.
func (ds *DistSender) getNodeDescriptor() *proto.NodeDescriptor {
	if desc := atomic.LoadPointer(&ds.nodeDescriptor); desc != nil {
		return (*proto.NodeDescriptor)(desc)
	}
	if ds.gossip == nil {
		return nil
	}

	ownNodeID := ds.gossip.GetNodeID()
	if ownNodeID > 0 {
		// TODO(tschottdorf): Consider instead adding the NodeID of the
		// coordinator to the header, so we can get this from incoming
		// requests. Just in case we want to mostly eliminate gossip here.
		nodeDesc := &proto.NodeDescriptor{}
		if err := ds.gossip.GetInfoProto(gossip.MakeNodeIDKey(ownNodeID), nodeDesc); err == nil {
			atomic.StorePointer(&ds.nodeDescriptor, unsafe.Pointer(nodeDesc))
			return nodeDesc
		}
	}
	log.Infof("unable to determine this node's attributes for replica " +
		"selection; node is most likely bootstrapping")
	return nil
}

// sendRPC sends one or more RPCs to replicas from the supplied proto.Replica
// slice. First, replicas which have gossiped addresses are corralled (and
// rearranged depending on proximity and whether the request needs to go to a
// leader) and then sent via rpc.Send, with requirement that one RPC to a
// server must succeed. Returns an RPC error if the request could not be sent.
// Note that the reply may contain a higher level error and must be checked in
// addition to the RPC error.
func (ds *DistSender) sendRPC(trace *tracer.Trace, rangeID proto.RangeID, replicas replicaSlice, order rpc.OrderingPolicy,
	args proto.BatchRequest) (proto.Response, error) {
	// TODO args->ba
	if len(replicas) == 0 {
		return nil, util.Errorf("replicas set is empty")
	}

	// Build a slice of replica addresses (if gossiped).
	var addrs []net.Addr
	replicaMap := map[string]*proto.ReplicaDescriptor{}
	for i := range replicas {
		addr := replicas[i].NodeDesc.Address
		addrs = append(addrs, addr)
		replicaMap[addr.String()] = &replicas[i].ReplicaDescriptor
	}
	if len(addrs) == 0 {
		return nil, noNodeAddrsAvailError{}
	}

	// TODO(pmattis): This needs to be tested. If it isn't set we'll
	// still route the request appropriately by key, but won't receive
	// RangeNotFoundErrors.
	args.RangeID = rangeID

	// Set RPC opts with stipulation that one of N RPCs must succeed.
	rpcOpts := rpc.Options{
		N:               1,
		Ordering:        order,
		SendNextTimeout: defaultSendNextTimeout,
		Timeout:         defaultRPCTimeout,
		Trace:           trace,
	}
	// getArgs clones the arguments on demand for all but the first replica.
	firstArgs := true
	getArgs := func(addr net.Addr) gogoproto.Message {
		var a *proto.BatchRequest
		// Use the supplied args proto if this is our first address.
		if firstArgs {
			firstArgs = false
			a = &args
		} else {
			// Otherwise, copy the args value and set the replica in the header.
			a = gogoproto.Clone(&args).(*proto.BatchRequest)
		}
		if addr != nil {
			// TODO(tschottdorf): see len(replicas) above.
			a.Replica = *replicaMap[addr.String()]
		}
		return a
	}
	// RPCs are sent asynchronously and there is no synchronized access to
	// the reply object, so we don't pass itself to RPCSend.
	// Otherwise there maybe a race case:
	// If the RPC call times out using our original reply object,
	// we must not use it any more; the rpc call might still return
	// and just write to it at any time.
	// args.CreateReply() should be cheaper than gogoproto.Clone which use reflect.
	getReply := func() gogoproto.Message {
		return &proto.BatchResponse{}
	}

	replies, err := ds.rpcSend(rpcOpts, "Node."+proto.Batch.String(),
		addrs, getArgs, getReply, ds.gossip.RPCContext)
	if err != nil {
		return nil, err
	}
	return replies[0].(proto.Response), nil
}

// getDescriptors looks up the range descriptor to use for a query over the
// key range [from,to), with the given lookupOptions. The range descriptor
// which contains the range in which the request should start its query is
// returned first; the returned bool is true in case the given range reaches
// outside the first descriptor.
// In case either of the descriptors is discovered stale, the returned closure
// should be called; it evicts the cache appropriately.
// Note that `from` and `to` are not necessarily Key and EndKey from a
// RequestHeader; it's assumed that they've been translated to key addresses
// already (via KeyAddress).
func (ds *DistSender) getDescriptors(from, to proto.Key, options lookupOptions) (*proto.RangeDescriptor, bool, func(), *proto.Error) {
	var desc *proto.RangeDescriptor
	var err error
	var descKey proto.Key
	if !options.useReverseScan {
		descKey = from
	} else {
		descKey = to
	}
	desc, err = ds.rangeCache.LookupRangeDescriptor(descKey, options)

	if err != nil {
		return nil, false, nil, proto.NewError(err)
	}

	// Checks whether need to get next range descriptor. If so, returns true.
	needAnother := func(desc *proto.RangeDescriptor, isReverse bool) bool {
		if isReverse {
			return from.Less(desc.StartKey)
		}
		return desc.EndKey.Less(to)
	}

	evict := func() {
		ds.rangeCache.EvictCachedRangeDescriptor(descKey, desc, options.useReverseScan)
	}

	return desc, needAnother(desc, options.useReverseScan), evict, nil
}

// sendAttempt gathers and rearranges the replicas, and makes an RPC call.
func (ds *DistSender) sendAttempt(trace *tracer.Trace, ba proto.BatchRequest, desc *proto.RangeDescriptor) (*proto.BatchResponse, *proto.Error) {
	defer trace.Epoch("sending RPC")()

	leader := ds.leaderCache.Lookup(proto.RangeID(desc.RangeID))

	// Try to send the call.
	replicas := newReplicaSlice(ds.gossip, desc)

	// Rearrange the replicas so that those replicas with long common
	// prefix of attributes end up first. If there's no prefix, this is a
	// no-op.
	order := ds.optimizeReplicaOrder(replicas)

	// If this request needs to go to a leader and we know who that is, move
	// it to the front.
	if !(ba.IsReadOnly() && ba.ReadConsistency == proto.INCONSISTENT) &&
		leader.StoreID > 0 {
		if i := replicas.FindReplica(leader.StoreID); i >= 0 {
			replicas.MoveToFront(i)
			order = rpc.OrderStable
		}
	}

	// TODO(tschottdorf) &ba -> ba
	resp, err := ds.sendRPC(trace, desc.RangeID, replicas, order, ba)
	if err != nil {
		return nil, proto.NewError(err)
	}
	// Untangle the error from the received response.
	br := resp.(*proto.BatchResponse)
	pErr := br.Error
	br.Error = nil // scrub the response error
	return br, pErr
}

// Send implements the batch.Sender interface. It subdivides
// the Batch into batches admissible for sending (preventing certain
// illegal mixtures of requests), executes each individual part
// (which may span multiple ranges), and recombines the response.
func (ds *DistSender) Send(ctx context.Context, ba proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {
	// In the event that timestamp isn't set and read consistency isn't
	// required, set the timestamp using the local clock.
	// TODO(tschottdorf): right place for this?
	if ba.ReadConsistency == proto.INCONSISTENT && ba.Timestamp.Equal(proto.ZeroTimestamp) {
		// Make sure that after the call, args hasn't changed.
		defer func(timestamp proto.Timestamp) {
			ba.Timestamp = timestamp
		}(ba.Timestamp)
		ba.Timestamp = ds.clock.Now()
	}

	// TODO(tschottdorf): provisional instantiation.
	return newChunkingSender(ds.sendChunk).Send(ctx, ba)
}

// sendChunk is in charge of sending an "admissible" piece of batch, i.e. one
// which doesn't need to be subdivided further before going to a range (so no
// mixing of forward and reverse scans, etc).
func (ds *DistSender) sendChunk(ctx context.Context, ba proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {
	// TODO(tschottdorf): prepare for removing Key and EndKey from BatchRequest,
	// making sure that anything that relies on them goes bust.
	ba.Key, ba.EndKey = nil, nil

	isReverse := ba.IsReverse()

	trace := tracer.FromCtx(ctx)

	// The minimal key range encompassing all requests contained within.
	// Local addressing has already been resolved.
	// TODO(tschottdorf): consider rudimentary validation of the batch here
	// (for example, non-range requests with EndKey, or empty key ranges).
	from, to := keys.Range(ba)
	var br *proto.BatchResponse
	// Send the request to one range per iteration.
	for {
		options := lookupOptions{
			useReverseScan: isReverse,
		}

		var curReply *proto.BatchResponse
		var desc *proto.RangeDescriptor
		var needAnother bool
		var pErr *proto.Error
		for r := retry.Start(ds.rpcRetryOptions); r.Next(); {
			// Get range descriptor (or, when spanning range, descriptors). Our
			// error handling below may clear them on certain errors, so we
			// refresh (likely from the cache) on every retry.
			descDone := trace.Epoch("meta descriptor lookup")
			var evictDesc func()
			desc, needAnother, evictDesc, pErr = ds.getDescriptors(from, to, options)
			descDone()

			// getDescriptors may fail retryably if the first range isn't
			// available via Gossip.
			if pErr != nil {
				if pErr.Retryable {
					if log.V(1) {
						log.Warning(pErr)
					}
					continue
				}
				break
			}

			// If there's no transaction and op spans ranges, possibly
			// re-run as part of a transaction for consistency. The
			// case where we don't need to re-run is if the read
			// consistency is not required.
			if needAnother && ba.Txn == nil && ba.IsRange() &&
				ba.ReadConsistency != proto.INCONSISTENT {
				return nil, proto.NewError(&proto.OpRequiresTxnError{})
			}

			// It's possible that the returned descriptor misses parts of the
			// keys it's supposed to scan after it's truncated to match the
			// descriptor. Example revscan [a,g), first desc lookup for "g"
			// returns descriptor [c,d) -> [d,g) is never scanned.
			// We evict and retry in such a case.
			if (isReverse && !desc.ContainsKeyRange(desc.StartKey, to)) || (!isReverse && !desc.ContainsKeyRange(from, desc.EndKey)) {
				evictDesc()
				continue
			}

			curReply, pErr = func() (*proto.BatchResponse, *proto.Error) {
				// Truncate the request to our current key range.
				untruncate, numActive, trErr := truncate(&ba, desc, from, to)
				if numActive == 0 && trErr == nil {
					untruncate()
					// This shouldn't happen in the wild, but some tests
					// exercise it.
					return nil, proto.NewError(util.Errorf("truncation resulted in empty batch on [%s,%s): %s",
						from, to, ba))
				}
				defer untruncate()
				if trErr != nil {
					return nil, proto.NewError(trErr)
				}
				// TODO(tschottdorf): make key range on batch redundant. The
				// requests within dictate it anyways.
				ba.Key, ba.EndKey = keys.Range(ba)
				reply, err := ds.sendAttempt(trace, ba, desc)
				ba.Key, ba.EndKey = nil, nil

				if err != nil {
					if log.V(1) {
						log.Warningf("failed to invoke %s: %s", ba, pErr)
					}
				}
				return reply, err
			}()
			// If sending succeeded, break this loop.
			if pErr == nil {
				break
			}

			// Error handling below.
			// If retryable, allow retry. For range not found or range
			// key mismatch errors, we don't backoff on the retry,
			// but reset the backoff loop so we can retry immediately.
			switch tErr := pErr.GoError().(type) {
			case *proto.SendError:
				// For an RPC error to occur, we must've been unable to contact
				// any replicas. In this case, likely all nodes are down (or
				// not getting back to us within a reasonable amount of time).
				// We may simply not be trying to talk to the up-to-date
				// replicas, so clearing the descriptor here should be a good
				// idea.
				// TODO(tschottdorf): If a replica group goes dead, this
				// will cause clients to put high read pressure on the first
				// range, so there should be some rate limiting here.
				evictDesc()
				if tErr.CanRetry() {
					continue
				}
			case *proto.RangeNotFoundError, *proto.RangeKeyMismatchError:
				trace.Event(fmt.Sprintf("reply error: %T", tErr))
				// Range descriptor might be out of date - evict it.
				evictDesc()
				// On addressing errors, don't backoff; retry immediately.
				r.Reset()
				if log.V(1) {
					log.Warning(tErr)
				}
				// On retries, allow [uncommitted] intents on range descriptor
				// lookups to be returned 50% of the time in order to succeed
				// at finding the transaction record pointed to by the intent
				// itself. The 50% probability of returning either the current
				// intent or the previously committed value balances between
				// the two cases where the intent's txn hasn't yet been
				// committed (the previous value is correct), or the intent's
				// txn has been committed (the intent value is correct).
				options.considerIntents = true
				continue
			case *proto.NotLeaderError:
				trace.Event(fmt.Sprintf("reply error: %T", tErr))
				newLeader := tErr.GetLeader()
				// Verify that leader is a known replica according to the
				// descriptor. If not, we've got a stale replica; evict cache.
				// Next, cache the new leader.
				if newLeader != nil {
					if i, _ := desc.FindReplica(newLeader.StoreID); i == -1 {
						if log.V(1) {
							log.Infof("error indicates unknown leader %s, expunging descriptor %s", newLeader, desc)
						}
						evictDesc()
					}
				} else {
					newLeader = &proto.ReplicaDescriptor{}
				}
				ds.updateLeaderCache(proto.RangeID(desc.RangeID), *newLeader)
				if log.V(1) {
					log.Warning(tErr)
				}
				r.Reset()
				continue
			case retry.Retryable:
				if tErr.CanRetry() {
					if log.V(1) {
						log.Warning(tErr)
					}
					trace.Event(fmt.Sprintf("reply error: %T", tErr))
					continue
				}
			}
			break
		}

		// Immediately return if querying a range failed non-retryably.
		if pErr != nil {
			return nil, pErr
		}

		first := br == nil
		if first {
			// First response from a Range.
			br = curReply
		} else {
			// This was the second or later call in a cross-Range request.
			// Combine the new response with the existing one.
			if err := br.Combine(curReply); err != nil {
				// TODO(tschottdorf): return nil, proto.NewError(err)
				panic(err)
			}
		}

		// If this request has a bound (such as MaxResults in
		// ScanRequest) and we are going to query at least one more range,
		// check whether enough rows have been retrieved.
		// TODO(tschottdorf): need tests for executing a multi-range batch
		// with various bounded requests which saturate at different times.
		if needAnother {
			// Start with the assumption that all requests are saturated.
			// Below, we look at each and decide whether that's true.
			// Everything that is indeed saturated is "masked out" from the
			// batch request; only if that's all requests does needAnother
			// remain false.
			needAnother = false
			if first {
				// Clone ba.Requests. This is because we're multi-range, and
				// some requests may be bounded, which could lead to them being
				// masked out once they're saturated. We don't want to risk
				// removing requests that way in the "master copy" since that
				// could lead to omitting requests in certain retry scenarios.
				ba.Requests = append([]proto.RequestUnion(nil), ba.Requests...)
			}
			for i, union := range ba.Requests {
				args := union.GetInner()
				if _, ok := args.(*proto.NoopRequest); ok {
					// NoopRequests are skipped.
					continue
				}
				boundedArg, ok := args.(proto.Bounded)
				if !ok {
					// Non-bounded request. We will have to query all ranges.
					needAnother = true
					continue
				}
				prevBound := boundedArg.GetBound()
				cReply, ok := curReply.Responses[i].GetInner().(proto.Countable)
				if !ok || prevBound <= 0 {
					// Request bounded, but without max results. Again, will
					// need to query everything we can. The case in which the reply
					// isn't countable occurs when the request wasn't active for
					// that range (since it didn't apply to it), so the response
					// is a NoopResponse.
					needAnother = true
					continue
				}
				nextBound := prevBound - cReply.Count()
				if nextBound <= 0 {
					// We've hit max results for this piece of the batch. Mask
					// it out (we've copied the requests slice above, so this
					// is kosher).
					ba.Requests[i].Reset() // necessary (no one-of?)
					if !ba.Requests[i].SetValue(&proto.NoopRequest{}) {
						panic("RequestUnion excludes NoopRequest")
					}
					continue
				}
				// The request isn't saturated yet.
				needAnother = true
				boundedArg.SetBound(nextBound)
			}
		}

		// If this was the last range accessed by this call, exit loop.
		if !needAnother {
			return br, nil
		}

		if isReverse {
			// In next iteration, query previous range.
			// We use the StartKey of the current descriptor as opposed to the
			// EndKey of the previous one since that doesn't have bugs when
			// stale descriptors come into play.
			to = prev(ba, desc.StartKey)
		} else {
			// In next iteration, query next range.
			// It's important that we use the EndKey of the current descriptor
			// as opposed to the StartKey of the next one: if the former is stale,
			// it's possible that the next range has since merged the subsequent
			// one, and unless both descriptors are stale, the next descriptor's
			// StartKey would move us to the beginning of the current range,
			// resulting in a duplicate scan.
			from = next(ba, desc.EndKey)
		}
		trace.Event("querying next range")
	}
}

// updateLeaderCache updates the cached leader for the given range,
// evicting any previous value in the process.
func (ds *DistSender) updateLeaderCache(rid proto.RangeID, leader proto.ReplicaDescriptor) {
	oldLeader := ds.leaderCache.Lookup(rid)
	if leader.StoreID != oldLeader.StoreID {
		if log.V(1) {
			log.Infof("range %d: new cached leader store %d (old: %d)", rid, leader.StoreID, oldLeader.StoreID)
		}
		ds.leaderCache.Update(rid, leader)
	}
}
