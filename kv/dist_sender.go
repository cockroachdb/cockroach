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

	"github.com/cockroachdb/cockroach/batch"
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

var _ batch.Sender = &DistSender{}

// RPCSendFn is the function type used to dispatch RPC calls.
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
	ignoreIntents  bool
	useReverseScan bool
}

// rangeLookup dispatches an RangeLookup request for the given
// metadata key to the replicas of the given range. Note that we allow
// inconsistent reads when doing range lookups for efficiency. Getting
// stale data is not a correctness problem but instead may
// infrequently result in additional latency as additional range
// lookups may be required. Note also that rangeLookup bypasses the
// DistSender's SendBatch() method, so there is no error inspection and
// retry logic here; this is not an issue since the lookup performs a
// single inconsistent read only.
func (ds *DistSender) rangeLookup(key proto.Key, options lookupOptions,
	desc *proto.RangeDescriptor) ([]proto.RangeDescriptor, error) {
	args, unwrap := batch.MaybeWrap(&proto.RangeLookupRequest{
		RequestHeader: proto.RequestHeader{
			Key:             key,
			ReadConsistency: proto.INCONSISTENT,
		},
		MaxRanges:     ds.rangeLookupMaxRanges,
		IgnoreIntents: options.ignoreIntents,
		Reverse:       options.useReverseScan,
	})
	replicas := newReplicaSlice(ds.gossip, desc)
	// TODO(tschottdorf) consider a Trace here, potentially that of the request
	// that had the cache miss and waits for the result.
	reply, err := ds.sendRPC(nil /* Trace */, desc.RangeID, replicas, rpc.OrderRandom, args)
	reply = unwrap(reply)
	if err != nil {
		return nil, err
	}
	rlReply := reply.(*proto.RangeLookupResponse)
	if rlReply.Error != nil {
		return nil, rlReply.GoError()
	}
	return rlReply.Ranges, nil
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
	args proto.Request) (proto.Response, error) {
	if len(replicas) == 0 {
		// TODO(tschottdorf): this gets in the way of some tests. Consider
		// refactoring so that gossip is mocked out more easily. Provisional
		// code. return nil, util.Errorf("%s: replicas set is empty",
		// args.Method())
	}

	// Build a slice of replica addresses (if gossiped).
	var addrs []net.Addr
	replicaMap := map[string]*proto.Replica{}
	for i := range replicas {
		addr := replicas[i].NodeDesc.Address
		addrs = append(addrs, addr)
		replicaMap[addr.String()] = &replicas[i].Replica
	}
	if len(addrs) == 0 {
		// TODO(tschottdorf): see len(replicas) above.
		// return nil, noNodeAddrsAvailError{}
	}

	// TODO(pmattis): This needs to be tested. If it isn't set we'll
	// still route the request appropriately by key, but won't receive
	// RangeNotFoundErrors.
	args.Header().RangeID = rangeID

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
		var a proto.Request
		// Use the supplied args proto if this is our first address.
		if firstArgs {
			firstArgs = false
			a = args
		} else {
			// Otherwise, copy the args value and set the replica in the header.
			a = gogoproto.Clone(args).(proto.Request)
		}
		if addr != nil {
			// TODO(tschottdorf): see len(replicas) above.
			a.Header().Replica = *replicaMap[addr.String()]
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
		return args.CreateReply()
	}

	replies, err := ds.rpcSend(rpcOpts, "Node."+args.Method().String(),
		addrs, getArgs, getReply, ds.gossip.RPCContext)
	if err != nil {
		return nil, err
	}
	return replies[0].(proto.Response), nil
}

// getDescriptors looks up the range descriptor to use for a query over the
// key range [from,to), with the given lookupOptions. The range descriptor
// which contains the range in which the request should start its query is
// returned first; a second one is optionally returned in case the given
// range reaches outside the first descriptor.
// In case either of the descriptors is discovered stale, the returned closure
// should be called; it evicts the cache appropriately.
// Note that `from` and `to` are not necessarily Key and EndKey from a
// RequestHeader; it's assumed that they've been translated to key addresses
// already (via KeyAddress).
func (ds *DistSender) getDescriptors(from, to proto.Key, options lookupOptions) (*proto.RangeDescriptor, *proto.RangeDescriptor, func(), error) {
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
		return nil, nil, nil, err
	}

	// Checks whether need to get next range descriptor. If so, returns true
	// and the key to look up, depending on whether we're in reverse mode.
	needAnother := func(desc *proto.RangeDescriptor, isReverse bool) (proto.Key, bool) {
		if isReverse {
			return desc.StartKey, from.Less(desc.StartKey)
		}
		return desc.EndKey, desc.EndKey.Less(to)
	}

	var descNext *proto.RangeDescriptor
	// If the request accesses keys beyond the end of this range,
	// get the descriptor of the adjacent range to address next.
	if nextKey, ok := needAnother(desc, options.useReverseScan); ok {
		// This next lookup is likely for free since we've read the
		// previous descriptor and range lookups use cache
		// prefetching.
		descNext, err = ds.rangeCache.LookupRangeDescriptor(nextKey, options)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	evict := func() {
		ds.rangeCache.EvictCachedRangeDescriptor(descKey, desc, options.useReverseScan)
	}
	return desc, descNext, evict, nil
}

// truncate restricts all contained requests to the given key range.
// Even on error, the returned closure must be executed; it undoes any
// truncations performed.
// First, the boundaries of the truncation are obtained: This is the
// intersection between [from,to) and the descriptor's range.
// Secondly, all requests contained in the batch are "truncated" to
// the resulting range, inserting NoopRequest appropriately to
// replace requests which are left without a key range to operate on.
// The closure returned must be executed to undo the truncation, even
// in case of an error.
// TODO(tschottdorf): cannot (yet) handle a range-request which does not
// intersect the current key range. The change should be straightforward.
// TODO(tschottdorf): we might want to refactor this closure-based undoing
// stuff after this hits master. It works fine, but maybe it's too much
// magic.
func truncate(br *proto.BatchRequest, desc *proto.RangeDescriptor, from, to proto.Key) (func(), error) {
	if !desc.ContainsKey(keys.KeyAddress(from)) {
		from = desc.StartKey
	}
	if !desc.ContainsKeyRange(desc.StartKey, keys.KeyAddress(to)) || to == nil {
		to = desc.EndKey
	}
	truncateOne := func(args proto.Request) (bool, []func(), error) {
		header := args.Header()
		// The BatchRequest hack is necessary as long as BatchRequest itself
		// is a "Request" which is expected to cover a key range.
		if _, ok := args.(*proto.BatchRequest); !ok && !proto.IsRange(args) {
			if len(header.EndKey) > 0 {
				return false, nil, util.Errorf("%T is not a range command, but EndKey is set", args)
			}
			if !desc.ContainsKey(keys.KeyAddress(args.Header().Key)) {
				// TODO(tschottdorf): erroring here prevents merges to go
				// through until batches are properly chunked. We can get away
				// with ignoring this as long as all requests go to the same
				// store - apparently mostly true in tests which span ranges.
				//return false, nil, util.Errorf("batch chopping not yet supported: %s on %q outside of [%s,%s)", args.Method(), args.Header().Key, desc.StartKey, desc.EndKey)
				return true, nil, nil
			}
			return false, nil, nil
		}
		var undo []func()
		if key := args.Header().Key; key != nil && keys.KeyAddress(key).Less(keys.KeyAddress(from)) {
			undo = append(undo, func() { args.Header().Key = key })
			args.Header().Key = from
		}
		// It is illegal to send EndKey on commands which do not operate on
		// key ranges, so don't do it in that case.
		if endKey := args.Header().EndKey; endKey != nil &&
			!keys.KeyAddress(endKey).Less(keys.KeyAddress(to)) {
			undo = append(undo, func() { args.Header().EndKey = endKey })
			args.Header().EndKey = to
		}
		return false, undo, nil
	}

	var fns []func()
	gUndo := func() {
		for _, f := range fns {
			f()
		}
	}

	for pos, arg := range br.Requests {
		omit, undo, err := truncateOne(arg.GetValue().(proto.Request))
		if omit {
			nReq := &proto.RequestUnion{}
			nReq.SetValue(&proto.NoopRequest{})
			oReq := br.Requests[pos]
			br.Requests[pos] = *nReq
			posCpy := pos // for closure
			undo = append(undo, func() {
				br.Requests[posCpy] = oReq
			})
		}
		fns = append(fns, undo...)
		if err != nil {
			return gUndo, err
		}
	}
	// TODO(tschottdorf): top-level header shouldn't hold keys any more.
	_, undo, err := truncateOne(br) // top-level header
	fns = append(fns, undo...)
	return gUndo, err
}

// sendAttempt temporarily truncates the arguments to match the descriptor,
// gathers and rearranges the replicas, and makes an RPC call.
func (ds *DistSender) sendAttempt(trace *tracer.Trace, ba *proto.BatchRequest, desc *proto.RangeDescriptor) (*proto.BatchResponse, error) {
	{
		// TODO(tschottdorf): provisional code to avoid sending noop-only batches.
		// Make nicer.
		var proceed bool
		for _, arg := range ba.Requests {
			if _, noop := arg.GetValue().(*proto.NoopRequest); noop {
				continue
			}
			proceed = true
		}
		// Batch may be empty now, and we don't want to send that (it will
		// break since you can't define the key range).
		if !proceed {
			br := &proto.BatchResponse{}
			for range ba.Requests {
				br.Add(&proto.NoopResponse{})
			}
			br.Txn = ba.Txn
			return br, nil
		}
		if ba.Key.Equal(proto.KeyMax) {
			panic(batch.Short(ba))
		}
	}
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
	if !(proto.IsReadOnly(ba) && ba.Header().ReadConsistency == proto.INCONSISTENT) &&
		leader.StoreID > 0 {
		if i := replicas.FindReplica(leader.StoreID); i >= 0 {
			replicas.MoveToFront(i)
			order = rpc.OrderStable
		}
	}

	resp, err := ds.sendRPC(trace, desc.RangeID, replicas, order, ba)
	if err != nil {
		return nil, err
	}
	// Untangle the error from the received response.
	br := resp.(*proto.BatchResponse)
	err = br.GoError()
	br.Error = nil
	return br, err
}

// SendBatch implements the batch.Sender interface. It subdivides
// the Batch into batches admissible for sending (preventing certain
// illegal mixtures of requests), executes each individual part
// (which may span multiple ranges), and recombines the response.
func (ds *DistSender) SendBatch(ctx context.Context, ba *proto.BatchRequest) (*proto.BatchResponse, error) {
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
	return batch.NewChunkingSender(ds.sendChunk).SendBatch(ctx, ba)
}

// sendChunk is in charge of sending an "admissible" piece of batch, i.e. one
// which doesn't need to be subdivided further before going to a range (so no
// mixing of forward and reverse scans, etc).
func (ds *DistSender) sendChunk(ctx context.Context, ba *proto.BatchRequest) (*proto.BatchResponse, error) {
	// TODO(tschottdorf): prepare for removing Key and EndKey from BatchRequest,
	// making sure that anything that relies on them goes bust.
	ba.Key, ba.EndKey = nil, nil

	isReverse := ba.IsReverse()

	// If this is a bounded request, we will change its bound as we receive
	// replies. This undoes that when we return.
	// TODO(tschottdorf): unimplemented for batch, so always false here.
	boundedArgs, argsBounded := proto.Bounded(nil), false // call.Args.(proto.Bounded)
	if argsBounded {
		defer func(bound int64) {
			boundedArgs.SetBound(bound)
		}(boundedArgs.GetBound())
	}

	trace := tracer.FromCtx(ctx)

	// The minimal key range encompassing all requests contained within.
	// Local addressing has already been resolved.
	from, to := batch.KeyRange(ba)
	var br *proto.BatchResponse
	// Send the request to one range per iteration.
	for {
		var curReply *proto.BatchResponse
		var desc, descNext *proto.RangeDescriptor
		var err error
		for r := retry.Start(ds.rpcRetryOptions); r.Next(); {
			// Get range descriptor (or, when spanning range, descriptors). Our
			// error handling below may clear them on certain errors, so we
			// refresh (likely from the cache) on every retry.
			descDone := trace.Epoch("meta descriptor lookup")
			var evictDesc func()

			// If the call contains a PushTxn, set ignoreIntents option as
			// necessary. This prevents a potential infinite loop; see the
			// comments in proto.RangeLookupRequest.
			options := lookupOptions{}
			// TODO(tschottdorf): awkward linear scan through the batch.
			if arg, ok := ba.GetArg(proto.PushTxn); ok {
				options.ignoreIntents = arg.(*proto.PushTxnRequest).RangeLookup
			}
			if isReverse {
				options.useReverseScan = true
			}
			desc, descNext, evictDesc, err = ds.getDescriptors(from, to, options)
			descDone()

			// getDescriptors may fail retryably if the first range isn't
			// available via Gossip.
			if err != nil {
				if rErr, ok := err.(retry.Retryable); ok && rErr.CanRetry() {
					if log.V(1) {
						log.Warning(err)
					}
					continue
				}
				break
			}

			// If there's no transaction and op spans ranges, possibly
			// re-run as part of a transaction for consistency. The
			// case where we don't need to re-run is if the read
			// consistency is not required.
			if descNext != nil && ba.Txn == nil && ba.IsRange() &&
				ba.ReadConsistency != proto.INCONSISTENT {
				return nil, &proto.OpRequiresTxnError{}
			}

			// TODO(tschottdorf): hacky way to prevent the following: A range-
			// spanning request hits a stale descriptor so that it misses parts
			// of the keys it's supposed to scan after it's truncated to match
			// the descriptor. Example revscan [a,g), first desc lookup for "g"
			// returns descriptor [c,d) -> [d,g) is never scanned.
			// This didn't happen prior to the refactor because the truncation
			// was more ad-hoc.
			if (isReverse && !desc.ContainsKeyRange(keys.KeyAddress(desc.StartKey), keys.KeyAddress(to))) || (!isReverse && !desc.ContainsKeyRange(keys.KeyAddress(from), keys.KeyAddress(desc.EndKey))) {
				evictDesc()
				continue
			}

			{
				// TODO(tschottdorf): check if this is already obsolete. If
				// not, work towards it.
				ba.Key, ba.EndKey = batch.KeyRange(ba)
				// Truncate the request to our current key range.
				untruncate, trErr := truncate(ba, desc, from, to)
				if trErr != nil {
					untruncate()
					return nil, trErr
				}
				curReply, err = ds.sendAttempt(trace, ba, desc)
				untruncate()
				ba.Key, ba.EndKey = nil, nil
			}

			if err == nil {
				break
			}

			if log.V(0) {
				log.Warningf("failed to invoke %s: %s", batch.Short(ba), err)
			}

			// If retryable, allow retry. For range not found or range
			// key mismatch errors, we don't backoff on the retry,
			// but reset the backoff loop so we can retry immediately.
			switch tErr := err.(type) {
			case *rpc.SendError:
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
				trace.Event(fmt.Sprintf("reply error: %T", err))
				// Range descriptor might be out of date - evict it.
				evictDesc()
				// On addressing errors, don't backoff; retry immediately.
				r.Reset()
				if log.V(1) {
					log.Warning(err)
				}
				continue
			case *proto.NotLeaderError:
				trace.Event(fmt.Sprintf("reply error: %T", err))
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
					newLeader = &proto.Replica{}
				}
				ds.updateLeaderCache(proto.RangeID(desc.RangeID), *newLeader)
				if log.V(1) {
					log.Warning(err)
				}
				r.Reset()
				continue
			case retry.Retryable:
				if tErr.CanRetry() {
					if log.V(1) {
						log.Warning(err)
					}
					trace.Event(fmt.Sprintf("reply error: %T", err))
					continue
				}
			}
			break
		}

		// Immediately return if querying a range failed non-retryably.
		if err != nil {
			return nil, err
		}

		if br == nil {
			// First response from a Range.
			br = curReply
		} else {
			// This was the second or later call in a cross-Range request.
			// Combine the new response with the existing one.
			if err := br.Combine(curReply); err != nil {
				panic(err)
				// TODO(tschottdorf): return nil, err
			}
		}

		// If this request has a bound, such as MaxResults in
		// ScanRequest, check whether enough rows have been retrieved.
		// TODO(tschottdorf): un-hackify this.
		if curReply.Header().GoError() == nil &&
			len(curReply.Responses) == len(ba.Requests) {
			for i, l := 0, len(ba.Requests); i < l; i++ {
				if boundedArg, ok := ba.Requests[i].GetValue().(proto.Bounded); ok {
					prevBound := boundedArg.GetBound()
					if cReply, ok := curReply.Responses[i].GetValue().(proto.Countable); ok && prevBound > 0 {
						if nextBound := prevBound - cReply.Count(); nextBound > 0 {
							defer func(c int64) {
								// Dirty way of undoing. The defers will pile up,
								// and execute so that the last one works.
								boundedArg.SetBound(c)
							}(prevBound)
							boundedArg.SetBound(nextBound)
						} else {
							descNext = nil
						}
					}
				}
			}
		}

		// If this was the last range accessed by this call, exit loop.
		if descNext == nil {
			return br, nil
		}

		if isReverse {
			// In next iteration, query previous range.
			// We use the StartKey of the current descriptor as opposed to the
			// EndKey of the previous one since that doesn't have bugs when
			// stale descriptors come into play.
			to = desc.StartKey
		} else {
			// In next iteration, query next range.
			// It's important that we use the EndKey of the current descriptor
			// as opposed to the StartKey of the next one: if the former is stale,
			// it's possible that the next range has since merged the subsequent
			// one, and unless both descriptors are stale, the next descriptor's
			// StartKey would move us to the beginning of the current range,
			// resulting in a duplicate scan.
			from = desc.EndKey
		}
		trace.Event("querying next range")
	}
}

// updateLeaderCache updates the cached leader for the given range,
// evicting any previous value in the process.
func (ds *DistSender) updateLeaderCache(rid proto.RangeID, leader proto.Replica) {
	oldLeader := ds.leaderCache.Lookup(rid)
	if leader.StoreID != oldLeader.StoreID {
		if log.V(1) {
			log.Infof("range %d: new cached leader store %d (old: %d)", rid, leader.StoreID, oldLeader.StoreID)
		}
		ds.leaderCache.Update(rid, leader)
	}
}
