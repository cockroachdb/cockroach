// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package physicalplan

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

var flowSpecPool = sync.Pool{
	New: func() interface{} {
		return &execinfrapb.FlowSpec{}
	},
}

// NewFlowSpec returns a new FlowSpec, which may have non-zero capacity in its
// slice fields.
func NewFlowSpec(flowID execinfrapb.FlowID, gateway roachpb.NodeID) *execinfrapb.FlowSpec {
	spec := flowSpecPool.Get().(*execinfrapb.FlowSpec)
	spec.FlowID = flowID
	spec.Gateway = gateway
	return spec
}

// ReleaseFlowSpec returns this FlowSpec back to the pool of FlowSpecs. It may
// not be used again after this call.
func ReleaseFlowSpec(spec *execinfrapb.FlowSpec) {
	for i := range spec.Processors {
		if tr := spec.Processors[i].Core.TableReader; tr != nil {
			releaseTableReaderSpec(tr)
		}
	}
	*spec = execinfrapb.FlowSpec{
		Processors: spec.Processors[:0],
	}
	flowSpecPool.Put(spec)
}

var trSpecPool = sync.Pool{
	New: func() interface{} {
		return &execinfrapb.TableReaderSpec{}
	},
}

// NewTableReaderSpec returns a new TableReaderSpec.
func NewTableReaderSpec() *execinfrapb.TableReaderSpec {
	return trSpecPool.Get().(*execinfrapb.TableReaderSpec)
}

// releaseTableReaderSpec puts this TableReaderSpec back into its sync pool. It
// may not be used again after Release returns.
func releaseTableReaderSpec(s *execinfrapb.TableReaderSpec) {
	*s = execinfrapb.TableReaderSpec{
		Spans: s.Spans[:0],
	}
	trSpecPool.Put(s)
}
