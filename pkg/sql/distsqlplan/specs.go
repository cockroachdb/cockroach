// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlplan

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
)

var flowSpecPool = sync.Pool{
	New: func() interface{} {
		return &distsqlpb.FlowSpec{}
	},
}

// NewFlowSpec returns a new FlowSpec, which may have non-zero capacity in its
// slice fields.
func NewFlowSpec(flowID distsqlpb.FlowID, gateway roachpb.NodeID) *distsqlpb.FlowSpec {
	spec := flowSpecPool.Get().(*distsqlpb.FlowSpec)
	spec.FlowID = flowID
	spec.Gateway = gateway
	return spec
}

var trSpecPool = sync.Pool{
	New: func() interface{} {
		return &distsqlpb.TableReaderSpec{}
	},
}

// NewTableReaderSpec returns a new TableReaderSpec.
func NewTableReaderSpec() *distsqlpb.TableReaderSpec {
	return trSpecPool.Get().(*distsqlpb.TableReaderSpec)
}

// ReleaseSetupFlowRequest releases the resources of this SetupFlowRequest,
// putting them back into their respective object pools.
func ReleaseSetupFlowRequest(_ *distsqlpb.SetupFlowRequest) {
	// TODO(yuzefovich): figure out who incorrectly reuses the memory.
}
