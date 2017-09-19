// Copyright 2016 The Cockroach Authors.
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

package storageutils

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
)

// raftCmdIDAndIndex identifies a batch and a command within it.
type raftCmdIDAndIndex struct {
	IDKey storagebase.CmdIDKey
	Index int
}

func (r raftCmdIDAndIndex) String() string {
	return fmt.Sprintf("%s/%d", r.IDKey, r.Index)
}

// ReplayProtectionFilterWrapper wraps a CommandFilter and assures protection
// from Raft replays.
type ReplayProtectionFilterWrapper struct {
	syncutil.Mutex
	inFlight          singleflight.Group
	processedCommands map[raftCmdIDAndIndex]*roachpb.Error
	filter            storagebase.ReplicaCommandFilter
}

// WrapFilterForReplayProtection wraps a filter into another one that adds Raft
// replay protection.
func WrapFilterForReplayProtection(
	filter storagebase.ReplicaCommandFilter,
) storagebase.ReplicaCommandFilter {
	wrapper := ReplayProtectionFilterWrapper{
		processedCommands: make(map[raftCmdIDAndIndex]*roachpb.Error),
		filter:            filter,
	}
	return wrapper.run
}

// Errors are mutated on the Send path, so we must always return copies.
func shallowCloneErrorWithTxn(pErr *roachpb.Error) *roachpb.Error {
	if pErr != nil {
		pErrCopy := *pErr
		txn := pErrCopy.GetTxn()
		if txn != nil {
			txnClone := txn.Clone()
			pErrCopy.SetTxn(&txnClone)
		}
		return &pErrCopy
	}

	return nil
}

// run executes the wrapped filter.
func (c *ReplayProtectionFilterWrapper) run(args storagebase.FilterArgs) *roachpb.Error {
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
	resC, _ := c.inFlight.DoChan(mapKey.String(), func() (interface{}, error) {
		pErr := c.filter(args)

		c.Lock()
		defer c.Unlock()
		c.processedCommands[mapKey] = pErr
		return pErr, nil
	})
	c.Unlock()

	res := <-resC
	return shallowCloneErrorWithTxn(res.Val.(*roachpb.Error))
}
