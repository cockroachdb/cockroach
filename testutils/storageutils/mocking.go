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
//
// Author: Andrei Matei (andreimatei1@gmail.com)

package storageutils

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/storagebase"
)

// FilterArgs groups the arguments to a ReplicaCommandFilter.
type FilterArgs struct {
	Ctx   context.Context
	CmdID storagebase.CmdIDKey
	Index int
	Sid   roachpb.StoreID
	Req   roachpb.Request
	Hdr   roachpb.Header
}

// raftCmdIDAndIndex identifies a batch and a command within it.
type raftCmdIDAndIndex struct {
	IDKey storagebase.CmdIDKey
	Index int
}

// InRaftCmd returns true if the filter is running in the context of a Raft
// command (it could be running outside of one, for example for a read).
func (f *FilterArgs) InRaftCmd() bool {
	return f.CmdID != ""
}

// ReplicaCommandFilter may be used in tests through the StorageTestingMocker to
// intercept the handling of commands and artificially generate errors. Return
// nil to continue with regular processing or non-nil to terminate processing
// with the returned error. Note that in a multi-replica test this filter will
// be run once for each replica and must produce consistent results each time.
type ReplicaCommandFilter func(args FilterArgs) *roachpb.Error

// ReplayProtectionFilterWrapper wraps a CommandFilter and assures protection
// from Raft replays.
type ReplayProtectionFilterWrapper struct {
	sync.RWMutex
	processedCommands map[raftCmdIDAndIndex]*roachpb.Error
	filter            ReplicaCommandFilter
}

// WrapFilterForReplayProtection wraps a filter into another one that adds Raft
// replay protection.
func WrapFilterForReplayProtection(
	filter ReplicaCommandFilter) ReplicaCommandFilter {
	wrapper := ReplayProtectionFilterWrapper{
		processedCommands: make(map[raftCmdIDAndIndex]*roachpb.Error),
		filter:            filter,
	}
	return wrapper.run
}

// run executes the wrapped filter.
func (c *ReplayProtectionFilterWrapper) run(args FilterArgs) *roachpb.Error {

	c.RLock()
	defer c.RUnlock()

	if args.InRaftCmd() {
		if err, found :=
			c.processedCommands[raftCmdIDAndIndex{args.CmdID, args.Index}]; found {
			// We've detected a replay.
			return err
		}
	}

	pErr := c.filter(args)

	if args.InRaftCmd() {
		c.processedCommands[raftCmdIDAndIndex{args.CmdID, args.Index}] = pErr
	}

	return pErr
}
