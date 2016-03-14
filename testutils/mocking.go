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

package testutils

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/util"
)

type savedError error

// ReplicaCommandFilter may be used in tests through the StorageTestingMocker to
// intercept the handling of commands and artificially generate errors. Return
// nil to continue with regular processing or non-nil to terminate processing
// with the returned error. Note that in a multi-replica test this filter will
// be run once for each replica and must produce consistent results each time.
type ReplicaCommandFilter func(
	context.Context, util.RaftCmdIDAndIndex, roachpb.StoreID, roachpb.Request, roachpb.Header) error

// ReplayProtectionFilterWrapper wraps a CommandFilter and assures protection
// from Raft replays.
type ReplayProtectionFilterWrapper struct {
	sync.RWMutex
	processedCommands map[util.RaftCmdIDAndIndex]savedError
	filter            ReplicaCommandFilter
}

// WrapFilterForReplayProtection wraps a filter into another one that adds Raft
// replay protection.
func WrapFilterForReplayProtection(
	filter ReplicaCommandFilter) ReplicaCommandFilter {
	wrapper := ReplayProtectionFilterWrapper{
		processedCommands: make(map[util.RaftCmdIDAndIndex]savedError),
		filter:            filter,
	}
	return wrapper.run
}

func (c *ReplayProtectionFilterWrapper) detectReplayLocked(
	cmdID util.RaftCmdIDAndIndex) (savedError, bool) {
	err, found := c.processedCommands[cmdID]
	// We've detected a replay.
	return err, found
}

// run executes the wrapped filter.
func (c *ReplayProtectionFilterWrapper) run(
	ctx context.Context, cmdID util.RaftCmdIDAndIndex, sid roachpb.StoreID,
	req roachpb.Request, hdr roachpb.Header) error {

	c.RLock()
	defer c.RUnlock()

	if cmdID.Initialized() {
		if err, found := c.detectReplayLocked(cmdID); found {
			return err.(error)
		}
	}

	err := c.filter(ctx, cmdID, sid, req, hdr)

	if cmdID.Initialized() {
		c.processedCommands[cmdID] = savedError(err)
	}

	return err
}
