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

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
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

// A firstRangeMissingError indicates that the first range has not yet
// been gossipped. This will be the case for a node which hasn't yet
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

// A DistKV provides methods to access Cockroach's monolithic,
// distributed key value store. Each method invocation triggers a
// lookup or lookups to find replica metadata for implicated key
// ranges. RPCs are sent to one or more of the replicas to satisfy
// the method invocation.
type DistKV struct {
	// gossip provides up-to-date information about the start of the
	// key range, used to find the replica metadata for arbitrary key
	// ranges.
	gossip *gossip.Gossip
	// rangeCache caches replica metadata for key ranges.
	rangeCache *RangeDescriptorCache
}

// NewDistKV returns a key-value datastore client which connects to the
// Cockroach cluster via the supplied gossip instance.
func NewDistKV(gossip *gossip.Gossip) *DistKV {
	kv := &DistKV{
		gossip: gossip,
	}
	kv.rangeCache = NewRangeDescriptorCache(kv)
	return kv
}

// verifyPermissions verifies that the requesting user (header.User)
// has permission to read/write (capabilities depend on method
// name). In the event that multiple permission configs apply to the
// key range implicated by the command, the lowest common denominator
// for permission. For example, if a scan crosses two permission
// configs, both configs must allow read permissions or the entire
// scan will fail.
func (kv *DistKV) verifyPermissions(method string, header *proto.RequestHeader) error {
	// Check for admin methods.
	if storage.NeedAdminPerm(method) && header.User != storage.UserRoot {
		return util.Errorf("user %q cannot invoke admin command %s", header.User, method)
	}
	// Get permissions map from gossip.
	permMap, err := kv.gossip.GetInfo(gossip.KeyConfigPermission)
	if err != nil {
		return err
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
		header.Key, end, func(start, end engine.Key, config interface{}) error {
			perm := config.(*proto.PermConfig)
			if storage.NeedReadPerm(method) && !perm.CanRead(header.User) {
				return util.Errorf("user %q cannot invoke %s on range %q-%q; permissions: %+v",
					header.User, method, string(start), string(end), perm)
			}
			if storage.NeedWritePerm(method) && !perm.CanWrite(header.User) {
				return util.Errorf("user %q cannot invoke %s on range %q-%q; permissions: %+v",
					header.User, method, string(start), string(end), perm)
			}
			return nil
		})
}

// nodeIDToAddr uses the gossip network to translate from node ID
// to a host:port address pair.
func (kv *DistKV) nodeIDToAddr(nodeID int32) (net.Addr, error) {
	nodeIDKey := gossip.MakeNodeIDGossipKey(nodeID)
	info, err := kv.gossip.GetInfo(nodeIDKey)
	if info == nil || err != nil {
		return nil, util.Errorf("Unable to lookup address for node: %v. Error: %v", nodeID, err)
	}
	return info.(net.Addr), nil
}

// internalRangeLookup dispatches an InternalRangeLookup request for the given
// metadata key to the replicas of the given range.
func (kv *DistKV) internalRangeLookup(key engine.Key,
	info *proto.RangeDescriptor) ([]proto.RangeDescriptor, error) {
	args := &proto.InternalRangeLookupRequest{
		RequestHeader: proto.RequestHeader{
			Key:  key,
			User: storage.UserRoot,
		},
		MaxRanges: rangeLookupMaxRanges,
	}
	replyChan := make(chan *proto.InternalRangeLookupResponse, len(info.Replicas))
	if err := kv.sendRPC(info, "Node.InternalRangeLookup", args, replyChan); err != nil {
		return nil, err
	}
	reply := <-replyChan
	if reply.Error != nil {
		return nil, reply.GoError()
	}
	return reply.Ranges, nil
}

// getFirstRangeDescriptor returns the RangeDescriptor for the first range on
// the cluster, which is retrieved from the gossip protocol instead of the
// datastore.
func (kv *DistKV) getFirstRangeDescriptor() (*proto.RangeDescriptor, error) {
	infoI, err := kv.gossip.GetInfo(gossip.KeyFirstRangeDescriptor)
	if err != nil {
		return nil, firstRangeMissingError{}
	}
	info := infoI.(proto.RangeDescriptor)
	return &info, nil
}

// getRangeDescriptor retrieves descriptor for the range containing the given key
// from storage. This function returns a sorted slice of RangeDescriptors for a
// set of consecutive ranges, the first which must contain the requested key.
// The additional RangeDescriptors are returned with the intent of pre-caching
// subsequent ranges which are likely to be requested soon by the current
// workload.
func (kv *DistKV) getRangeDescriptor(key engine.Key) ([]proto.RangeDescriptor, error) {
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
		rd, err := kv.getFirstRangeDescriptor()
		if err != nil {
			return nil, err
		}
		return []proto.RangeDescriptor{*rd}, nil
	}
	if bytes.HasPrefix(metadataKey, engine.KeyMeta1Prefix) {
		// In this case, desc is the cluster's first range.
		if desc, err = kv.getFirstRangeDescriptor(); err != nil {
			return nil, err
		}
	} else {
		// Look up desc from the cache, which will recursively call into
		// kv.getRangeDescriptor if it is not cached.
		desc, err = kv.rangeCache.LookupRangeDescriptor(metadataKey)
		if err != nil {
			return nil, err
		}
	}

	return kv.internalRangeLookup(metadataKey, desc)
}

// sendRPC sends one or more RPCs to replicas from the supplied
// proto.Replica slice. First, replicas which have gossipped
// addresses are corraled and then sent via rpc.Send, with requirement
// that one RPC to a server must succeed.
func (kv *DistKV) sendRPC(desc *proto.RangeDescriptor, method string, args proto.Request, replyChan interface{}) error {
	if len(desc.Replicas) == 0 {
		return util.Errorf("%s: replicas set is empty", method)
	}
	// Build a map from replica address (if gossipped) to args struct
	// with replica set in header.
	argsMap := map[net.Addr]interface{}{}
	for _, replica := range desc.Replicas {
		addr, err := kv.nodeIDToAddr(replica.NodeID)
		if err != nil {
			log.V(1).Infof("node %d address is not gossipped", replica.NodeID)
			continue
		}
		// Copy the args value and set the replica in the header.
		argsCopy := gogoproto.Clone(args).(proto.Request)
		argsCopy.Header().Replica = replica
		argsMap[addr] = argsCopy
	}
	if len(argsMap) == 0 {
		return noNodeAddrsAvailError{}
	}
	rpcOpts := rpc.Options{
		N:               1,
		SendNextTimeout: defaultSendNextTimeout,
		Timeout:         defaultRPCTimeout,
	}
	return rpc.Send(argsMap, method, replyChan, rpcOpts, kv.gossip.RPCContext)
}

// ExecuteCmd verifies permissions and looks up the appropriate range
// based on the supplied key and sends the RPC according to the
// specified options. executeRPC sends asynchronously and returns a
// response value on the replyChan channel when the call is complete.
func (kv *DistKV) ExecuteCmd(method string, args proto.Request, replyChan interface{}) {
	// Augment method with "Node." prefix.
	method = "Node." + method

	// Verify permissions.
	if err := kv.verifyPermissions(method, args.Header()); err != nil {
		sendError(err, replyChan)
		return
	}

	// Retry logic for lookup of range by key and RPCs to range replicas.
	retryOpts := util.RetryOptions{
		Tag:         fmt.Sprintf("routing %s rpc", method),
		Backoff:     retryBackoff,
		MaxBackoff:  maxRetryBackoff,
		Constant:    2,
		MaxAttempts: 0, // retry indefinitely
	}
	err := util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
		desc, err := kv.rangeCache.LookupRangeDescriptor(args.Header().Key)
		if err == nil {
			err = kv.sendRPC(desc, method, args, replyChan)
		}
		if err != nil {
			log.Warningf("failed to invoke %s: %v", method, err)
			// If retryable, allow outer loop to retry. We treat a range not found
			// or range key mismatch errors special. In these cases, we don't want
			// to backoff on the retry, but reset the backoff loop so we can retry
			// immediately.
			switch err.(type) {
			case *proto.RangeNotFoundError, *proto.RangeKeyMismatchError:
				// Range descriptor might be out of date - evict it.
				kv.rangeCache.EvictCachedRangeDescriptor(args.Header().Key)
				// On addressing errors, don't backoff and retry immediately.
				return util.RetryReset, nil
			default:
				if retryErr, ok := err.(util.Retryable); ok && retryErr.CanRetry() {
					return util.RetryContinue, nil
				}
			}
		}
		return util.RetryBreak, err
	})
	if err != nil {
		sendError(err, replyChan)
	}
}

// Close is a noop for the distributed KV implementation.
func (kv *DistKV) Close() {}
