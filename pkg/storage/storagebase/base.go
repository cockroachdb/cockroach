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

package storagebase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// CmdIDKey is a Raft command id.
type CmdIDKey string

// FilterArgs groups the arguments to a ReplicaCommandFilter.
type FilterArgs struct {
	Ctx   context.Context
	CmdID CmdIDKey
	Index int
	Sid   roachpb.StoreID
	Req   roachpb.Request
	Hdr   roachpb.Header
}

// ProposalFilterArgs groups the arguments to ReplicaProposalFilter.
type ProposalFilterArgs struct {
	Ctx   context.Context
	Cmd   RaftCommand
	CmdID CmdIDKey
	Req   roachpb.BatchRequest
}

// ApplyFilterArgs groups the arguments to a ReplicaApplyFilter.
type ApplyFilterArgs struct {
	ReplicatedEvalResult
	CmdID   CmdIDKey
	RangeID roachpb.RangeID
	StoreID roachpb.StoreID
}

// InRaftCmd returns true if the filter is running in the context of a Raft
// command (it could be running outside of one, for example for a read).
func (f *FilterArgs) InRaftCmd() bool {
	return f.CmdID != ""
}

// ReplicaRequestFilter can be used in testing to influence the error returned
// from a request before it is evaluated. Notably, the filter is run before the
// request is added to the CommandQueue, so blocking in the filter will not
// block interfering requests.
type ReplicaRequestFilter func(roachpb.BatchRequest) *roachpb.Error

// ReplicaCommandFilter may be used in tests through the StoreTestingKnobs to
// intercept the handling of commands and artificially generate errors. Return
// nil to continue with regular processing or non-nil to terminate processing
// with the returned error.
type ReplicaCommandFilter func(args FilterArgs) *roachpb.Error

// ReplicaProposalFilter can be used in testing to influence the error returned
// from proposals after a request is evaluated but before it is proposed.
type ReplicaProposalFilter func(args ProposalFilterArgs) *roachpb.Error

// A ReplicaApplyFilter can be used in testing to influence the error returned
// from proposals after they apply.
type ReplicaApplyFilter func(args ApplyFilterArgs) *roachpb.Error

// ReplicaResponseFilter is used in unittests to modify the outbound
// response returned to a waiting client after a replica command has
// been processed. This filter is invoked only by the command proposer.
type ReplicaResponseFilter func(roachpb.BatchRequest, *roachpb.BatchResponse) *roachpb.Error

// CommandQueueAction is an action taken by a BatchRequest's batchCmdSet on the
// CommandQueue.
type CommandQueueAction int

const (
	// CommandQueueWaitForPrereqs represents the state of a batchCmdSet when it
	// has just inserted itself into the CommandQueue and is beginning to wait
	// for prereqs to finish execution.
	CommandQueueWaitForPrereqs CommandQueueAction = iota
	// CommandQueueCancellation represents the state of a batchCmdSet when it
	// is canceled while waiting for prerequisites to finish and is forced to
	// remove itself from the CommandQueue without executing.
	CommandQueueCancellation
	// CommandQueueBeginExecuting represents the state of a batchCmdSet when it
	// has finished waiting for all prereqs to finish execution and is now free
	// to execute itself.
	CommandQueueBeginExecuting
	// CommandQueueFinishExecuting represents the state of a batchCmdSet when it
	// has finished executing and will remove itself from the CommandQueue.
	CommandQueueFinishExecuting
)
