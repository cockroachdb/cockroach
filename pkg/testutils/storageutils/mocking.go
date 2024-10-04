// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageutils

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
)

// raftCmdIDAndIndex identifies a batch and a command within it.
type raftCmdIDAndIndex struct {
	IDKey kvserverbase.CmdIDKey
	Index int
}

func (r raftCmdIDAndIndex) String() string {
	return fmt.Sprintf("%s/%d", r.IDKey, r.Index)
}

// ReplayProtectionFilterWrapper wraps a CommandFilter and assures protection
// from Raft replays.
type ReplayProtectionFilterWrapper struct {
	syncutil.Mutex
	inFlight          *singleflight.Group
	processedCommands map[raftCmdIDAndIndex]*kvpb.Error
	filter            kvserverbase.ReplicaCommandFilter
}

// WrapFilterForReplayProtection wraps a filter into another one that adds Raft
// replay protection.
func WrapFilterForReplayProtection(
	filter kvserverbase.ReplicaCommandFilter,
) kvserverbase.ReplicaCommandFilter {
	wrapper := ReplayProtectionFilterWrapper{
		inFlight:          singleflight.NewGroup("replay-protection", "key"),
		processedCommands: make(map[raftCmdIDAndIndex]*kvpb.Error),
		filter:            filter,
	}
	return wrapper.run
}

// Errors are mutated on the Send path, so we must always return copies.
func shallowCloneErrorWithTxn(pErr *kvpb.Error) *kvpb.Error {
	if pErr != nil {
		pErrCopy := *pErr
		pErrCopy.SetTxn(pErrCopy.GetTxn())
		return &pErrCopy
	}

	return nil
}

// run executes the wrapped filter.
func (c *ReplayProtectionFilterWrapper) run(args kvserverbase.FilterArgs) *kvpb.Error {
	if !args.InRaftCmd() {
		return c.filter(args)
	}

	mapKey := raftCmdIDAndIndex{args.CmdID, args.Index}

	c.Lock()
	if pErr, ok := c.processedCommands[mapKey]; ok {
		c.Unlock()
		return shallowCloneErrorWithTxn(pErr)
	}

	// We use the singleflight.Group to coalesce replayed raft commands onto the
	// same filter call. This allows concurrent access to the filter for
	// different raft commands.
	future, _ := c.inFlight.DoChan(args.Ctx, mapKey.String(),
		singleflight.DoOpts{
			Stop:               nil,
			InheritCancelation: true,
		},
		func(ctx context.Context) (interface{}, error) {
			pErr := c.filter(args)

			c.Lock()
			defer c.Unlock()
			c.processedCommands[mapKey] = pErr
			return pErr, nil
		})
	c.Unlock()

	res := future.WaitForResult(args.Ctx)
	if res.Err != nil {
		return kvpb.NewError(res.Err)
	}
	return shallowCloneErrorWithTxn(res.Val.(*kvpb.Error))
}
