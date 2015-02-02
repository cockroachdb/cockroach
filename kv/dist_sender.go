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
	"bytes"
	"fmt"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"

	gogoproto "github.com/gogo/protobuf/proto"
)

// Default constants for timeouts.
const (
	defaultSendNextTimeout = 1 * time.Second
	defaultRPCTimeout      = 15 * time.Second
	defaultClientTimeout   = 10 * time.Second
	retryBackoff           = 1 * time.Second
	maxRetryBackoff        = 30 * time.Second

	// Maximum number of ranges to return from an internal range lookup.
	// TODO(mrtracy): This value should be configurable.
	rangeLookupMaxRanges = 8
)

var rpcRetryOpts = util.RetryOptions{
	Backoff:     retryBackoff,
	MaxBackoff:  maxRetryBackoff,
	Constant:    2,
	MaxAttempts: 0, // retry indefinitely
}

// A firstRangeMissingError indicates that the first range has not yet
// been gossiped. This will be the case for a node which hasn't yet
// joined the gossip network.
type firstRangeMissingError struct{}

// Error implements the error interface.
func (f firstRangeMissingError) Error() string {
	return "the descriptor for the first range is not available via gossip"
}

// CanRetry implements the Retryable interface.
func (f firstRangeMissingError) CanRetry() bool { return true }

// A noNodesAvailError specifies that no node addresses in a replica set
// were available via the gossip network.
type noNodeAddrsAvailError struct{}

// Error implements the error interface.
func (n noNodeAddrsAvailError) Error() string {
	return "no replica node addresses available via gossip"
}

// CanRetry implements the Retryable interface.
func (n noNodeAddrsAvailError) CanRetry() bool { return true }

// A DistSender provides methods to access Cockroach's monolithic,
// distributed key value store. Each method invocation triggers a
// lookup or lookups to find replica metadata for implicated key
// ranges. RPCs are sent to one or more of the replicas to satisfy
// the method invocation.
type DistSender struct {
	// gossip provides up-to-date information about the start of the
	// key range, used to find the replica metadata for arbitrary key
	// ranges.
	gossip *gossip.Gossip
	// rangeCache caches replica metadata for key ranges.
	rangeCache *RangeDescriptorCache
}

// NewDistSender returns a client.KVSender instance which connects to the
// Cockroach cluster via the supplied gossip instance.
func NewDistSender(gossip *gossip.Gossip) *DistSender {
	ds := &DistSender{
		gossip: gossip,
	}
	ds.rangeCache = NewRangeDescriptorCache(ds)
	return ds
}

// verifyPermissions verifies that the requesting user (header.User)
// has permission to read/write (capabilities depend on method
// name). In the event that multiple permission configs apply to the
// key range implicated by the command, the lowest common denominator
// for permission. For example, if a scan crosses two permission
// configs, both configs must allow read permissions or the entire
// scan will fail.
func (ds *DistSender) verifyPermissions(method string, header *proto.RequestHeader) error {
	// The root user can always proceed.
	if header.User == storage.UserRoot {
		return nil
	}
	// Check for admin methods.
	if proto.NeedAdminPerm(method) {
		if header.User != storage.UserRoot {
			return util.Errorf("user %q cannot invoke admin command %s", header.User, method)
		}
		return nil
	}
	// Get permissions map from gossip.
	permMap, err := ds.gossip.GetInfo(gossip.KeyConfigPermission)
	if err != nil {
		return util.Errorf("permissions not available via gossip")
	}
	if permMap == nil {
		return util.Errorf("perm configs not available; cannot execute %s", method)
	}
	// Visit PermConfig(s) which apply to the method's key range.
	//   - For each, verify each PermConfig allows reads or writes as method requires.
	end := header.EndKey
	if end == nil {
		end = header.Key
	}
	return permMap.(storage.PrefixConfigMap).VisitPrefixes(
		header.Key, end, func(start, end proto.Key, config interface{}) error {
			perm := config.(*proto.PermConfig)
			if proto.NeedReadPerm(method) && !perm.CanRead(header.User) {
				return util.Errorf("user %q cannot invoke %s at %q; permissions: %+v",
					header.User, method, string(start), perm)
			}
			if proto.NeedWritePerm(method) && !perm.CanWrite(header.User) {
				return util.Errorf("user %q cannot invoke %s at %q; permissions: %+v",
					header.User, method, string(start), perm)
			}
			return nil
		})
}

// nodeIDToAddr uses the gossip network to translate from node ID
// to a host:port address pair.
func (ds *DistSender) nodeIDToAddr(nodeID proto.NodeID) (net.Addr, error) {
	nodeIDKey := gossip.MakeNodeIDGossipKey(nodeID)
	info, err := ds.gossip.GetInfo(nodeIDKey)
	if info == nil || err != nil {
		return nil, util.Errorf("Unable to lookup address for node: %d. Error: %s", nodeID, err)
	}
	return info.(net.Addr), nil
}

// internalRangeLookup dispatches an InternalRangeLookup request for the given
// metadata key to the replicas of the given range.
func (ds *DistSender) internalRangeLookup(key proto.Key, info *proto.RangeDescriptor) ([]proto.RangeDescriptor, error) {
	args := &proto.InternalRangeLookupRequest{
		RequestHeader: proto.RequestHeader{
			Key:  key,
			User: storage.UserRoot,
		},
		MaxRanges: rangeLookupMaxRanges,
	}
	reply := &proto.InternalRangeLookupResponse{}
	if err := ds.sendRPC(info, "InternalRangeLookup", args, reply); err != nil {
		return nil, err
	}
	if reply.Error != nil {
		return nil, reply.GoError()
	}
	return reply.Ranges, nil
}

// getFirstRangeDescriptor returns the RangeDescriptor for the first range on
// the cluster, which is retrieved from the gossip protocol instead of the
// datastore.
func (ds *DistSender) getFirstRangeDescriptor() (*proto.RangeDescriptor, error) {
	infoI, err := ds.gossip.GetInfo(gossip.KeyFirstRangeDescriptor)
	if err != nil {
		return nil, firstRangeMissingError{}
	}
	info := infoI.(proto.RangeDescriptor)
	return &info, nil
}

// getRangeDescriptor retrieves the descriptor for the range
// containing the given key from storage. This function returns a
// sorted slice of RangeDescriptors for a set of consecutive ranges,
// the first which must contain the requested key.  The additional
// RangeDescriptors are returned with the intent of pre-caching
// subsequent ranges which are likely to be requested soon by the
// current workload.
func (ds *DistSender) getRangeDescriptor(key proto.Key) ([]proto.RangeDescriptor, error) {
	var (
		// metadataKey is sent to InternalRangeLookup to find the
		// RangeDescriptor which contains key.
		metadataKey = engine.RangeMetaKey(key)
		// desc is the RangeDescriptor for the range which contains
		// metadataKey.
		desc *proto.RangeDescriptor
		err  error
	)
	if len(metadataKey) == 0 {
		// In this case, the requested key is stored in the cluster's first
		// range. Return the first range, which is always gossiped and not
		// queried from the datastore.
		rd, err := ds.getFirstRangeDescriptor()
		if err != nil {
			return nil, err
		}
		return []proto.RangeDescriptor{*rd}, nil
	}
	if bytes.HasPrefix(metadataKey, engine.KeyMeta1Prefix) {
		// In this case, desc is the cluster's first range.
		if desc, err = ds.getFirstRangeDescriptor(); err != nil {
			return nil, err
		}
	} else {
		// Look up desc from the cache, which will recursively call into
		// ds.getRangeDescriptor if it is not cached.
		desc, err = ds.rangeCache.LookupRangeDescriptor(metadataKey)
		if err != nil {
			return nil, err
		}
	}

	return ds.internalRangeLookup(metadataKey, desc)
}

// sendRPC sends one or more RPCs to replicas from the supplied
// proto.Replica slice. First, replicas which have gossiped
// addresses are corralled and then sent via rpc.Send, with requirement
// that one RPC to a server must succeed.
func (ds *DistSender) sendRPC(desc *proto.RangeDescriptor, method string, args proto.Request, reply proto.Response) error {
	if len(desc.Replicas) == 0 {
		return util.Errorf("%s: replicas set is empty", method)
	}

	// Build a slice of replica addresses (if gossiped).
	var addrs []net.Addr
	replicaMap := map[string]*proto.Replica{}
	for i := range desc.Replicas {
		addr, err := ds.nodeIDToAddr(desc.Replicas[i].NodeID)
		if err != nil {
			log.V(1).Infof("node %d address is not gossiped", desc.Replicas[i].NodeID)
			continue
		}
		addrs = append(addrs, addr)
		replicaMap[addr.String()] = &desc.Replicas[i]
	}
	if len(addrs) == 0 {
		return noNodeAddrsAvailError{}
	}

	// Set RPC opts with stipulation that one of N RPCs must succeed.
	rpcOpts := rpc.Options{
		N:               1,
		Ordering:        rpc.OrderRandom, // TODO(spencer): change this to order stable if we know leader
		SendNextTimeout: defaultSendNextTimeout,
		Timeout:         defaultRPCTimeout,
	}
	// getArgs clones the arguments on demand for all but the first replica.
	firstArgs := true
	getArgs := func(addr net.Addr) interface{} {
		var a proto.Request
		// Use the supplied args proto if this is our first address.
		if firstArgs {
			firstArgs = false
			a = args
		} else {
			// Otherwise, copy the args value and set the replica in the header.
			a = gogoproto.Clone(args).(proto.Request)
		}
		a.Header().Replica = *replicaMap[addr.String()]
		return a
	}
	firstReply := true
	getReply := func() interface{} {
		if firstReply {
			firstReply = false
			return reply
		}
		return gogoproto.Clone(reply)
	}
	_, err := rpc.Send(rpcOpts, "Node."+method, addrs, getArgs, getReply, ds.gossip.RPCContext)
	return err
}

// Send implements the clent.KVSender interface. It verifies
// permissions and looks up the appropriate range based on the
// supplied key and sends the RPC according to the specified
// options.
// If the request spans multiple ranges (which is possible for
// Scan or DeleteRange requests), Send sends requests to the
// individual ranges sequentially and combines the results
// transparently.
func (ds *DistSender) Send(call *client.Call) {
	// Verify permissions.
	if err := ds.verifyPermissions(call.Method, call.Args.Header()); err != nil {
		call.Reply.Header().SetGoError(err)
		return
	}

	// Retry logic for lookup of range by key and RPCs to range replicas.
	retryOpts := rpcRetryOpts
	retryOpts.Tag = fmt.Sprintf("routing %s rpc", call.Method)

	// responses and descNext are only used when executing across ranges.
	var responses []proto.Response
	var descNext *proto.RangeDescriptor
	// args will be changed to point to a copy of call.Args if the request
	// spans ranges since in that case we need to alter its contents.
	args := call.Args
	for {
		reply := call.Reply
		err := util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
			descNext = nil
			desc, err := ds.rangeCache.LookupRangeDescriptor(args.Header().Key)
			if err == nil {
				// If the request accesses keys beyond the end of this range,
				// get the descriptor of the adjacent range to address next.
				if desc.EndKey.Less(call.Args.Header().EndKey) {
					if _, ok := call.Reply.(proto.Combinable); !ok {
						return util.RetryBreak, util.Error("illegal cross-range operation", call)
					}
					if call.Args.Header().Txn == nil {
						return util.RetryBreak, &proto.OpRequiresTxnError{}
					}
					// This next lookup is likely for free since we've read the
					// previous descriptor and range lookups use cache
					// prefetching.
					descNext, err = ds.rangeCache.LookupRangeDescriptor(desc.EndKey)
					// If this is the first step in a multi-range operation,
					// additionally copy call.Args because we will have to
					// mutate it as we talk to the involved ranges.
					if len(responses) == 0 {
						args = gogoproto.Clone(call.Args).(proto.Request)
					}
					// Truncate the request to our current range.
					args.Header().EndKey = desc.EndKey
				}
			}
			// true if we're dealing with a range-spanning request.
			isMulti := len(responses) > 0 || descNext != nil
			if err == nil {
				if isMulti {
					// Make a new reply object for this call.
					reply = gogoproto.Clone(call.Reply).(proto.Response)
				}
				err = ds.sendRPC(desc, call.Method, args, reply)
			}

			if err != nil {
				log.Warningf("failed to invoke %s: %s", call.Method, err)
				// If retryable, allow retry. For range not found or range
				// key mismatch errors, we don't backoff on the retry,
				// but reset the backoff loop so we can retry immediately.
				switch err.(type) {
				case *proto.RangeNotFoundError, *proto.RangeKeyMismatchError:
					// Range descriptor might be out of date - evict it.
					ds.rangeCache.EvictCachedRangeDescriptor(args.Header().Key)
					// On addressing errors, don't backoff and retry immediately.
					return util.RetryReset, nil
				default:
					if retryErr, ok := err.(util.Retryable); ok && retryErr.CanRetry() {
						return util.RetryContinue, nil
					}
				}
			} else if isMulti {
				// If this request spans ranges, collect the replies.
				responses = append(responses, reply)
			}
			return util.RetryBreak, err
		})
		// Immediately return if querying a range failed non-retryably.
		// For multi-range requests, we return the failing range's reply.
		if err != nil {
			reply.Header().SetGoError(err)
			gogoproto.Merge(call.Reply, reply) // Only relevant in multi-range case.
			return
		}
		// If this was the last range accessed by this call, exit loop.
		if descNext == nil {
			break
		}
		// In next iteration, query next range.
		args.Header().Key = descNext.StartKey
		// "Untruncate" EndKey to original.
		args.Header().EndKey = call.Args.Header().EndKey
	}

	// If this was a multi-range request, aggregate the individual range
	// responses into one reply.
	if len(responses) > 0 {
		// We've already ascertained earlier that we're dealing with a
		// Combinable response type.
		firstReply := responses[0].(proto.Combinable)
		for _, r := range responses[1:] {
			firstReply.Combine(r)
		}
		gogoproto.Merge(call.Reply, responses[0])
	}
}

// Close implements the client.KVSender interface. It's a noop for the
// distributed sender.
func (ds *DistSender) Close() {}
