// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// createScrubPhysicalCheck generates a plan for running a physical
// check for an index. The plan consists of TableReaders, with IsCheck
// enabled, that scan an index span. By having IsCheck enabled, the
// TableReaders will only emit errors encountered during scanning
// instead of row data. The plan is finalized.
func (dsp *DistSQLPlanner) createScrubPhysicalCheck(
	planCtx *PlanningCtx,
	n *scanNode,
	desc sqlbase.TableDescriptor,
	indexDesc sqlbase.IndexDescriptor,
	spans []roachpb.Span,
	readAsOf hlc.Timestamp,
) (PhysicalPlan, error) {
	spec, _, err := initTableReaderSpec(n, planCtx, nil /* indexVarMap */)
	if err != nil {
		return PhysicalPlan{}, err
	}

	spanPartitions, err := dsp.PartitionSpans(planCtx, n.spans)
	if err != nil {
		return PhysicalPlan{}, err
	}

	var p PhysicalPlan
	stageID := p.NewStageID()
	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(spanPartitions))
	for i, sp := range spanPartitions {
		tr := &distsqlpb.TableReaderSpec{}
		*tr = *spec
		tr.Spans = make([]distsqlpb.TableReaderSpan, len(sp.Spans))
		for j := range sp.Spans {
			tr.Spans[j].Span = sp.Spans[j]
		}

		proc := distsqlplan.Processor{
			Node: sp.Node,
			Spec: distsqlpb.ProcessorSpec{
				Core:    distsqlpb.ProcessorCoreUnion{TableReader: tr},
				Output:  []distsqlpb.OutputRouterSpec{{Type: distsqlpb.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	// Set the plan's result types to be ScrubTypes.
	p.ResultTypes = distsqlrun.ScrubTypes
	p.PlanToStreamColMap = identityMapInPlace(make([]int, len(distsqlrun.ScrubTypes)))

	dsp.FinalizePlan(planCtx, &p)
	return p, nil
}
