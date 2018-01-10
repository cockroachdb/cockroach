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
	planCtx *planningCtx,
	n *scanNode,
	desc sqlbase.TableDescriptor,
	indexDesc sqlbase.IndexDescriptor,
	spans []roachpb.Span,
	readAsOf hlc.Timestamp,
) (physicalPlan, error) {
	spec, _, err := initTableReaderSpec(n, planCtx.EvalContext())
	if err != nil {
		return physicalPlan{}, err
	}

	spanPartitions, err := dsp.partitionSpans(planCtx, n.spans)
	if err != nil {
		return physicalPlan{}, err
	}

	var p physicalPlan
	stageID := p.NewStageID()
	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(spanPartitions))
	for i, sp := range spanPartitions {
		tr := &distsqlrun.TableReaderSpec{}
		*tr = spec
		tr.Spans = make([]distsqlrun.TableReaderSpan, len(sp.spans))
		for j := range sp.spans {
			tr.Spans[j].Span = sp.spans[j]
		}

		proc := distsqlplan.Processor{
			Node: sp.node,
			Spec: distsqlrun.ProcessorSpec{
				Core:    distsqlrun.ProcessorCoreUnion{TableReader: tr},
				Output:  []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	// Set the plan's result types to be ScrubTypes.
	p.ResultTypes = distsqlrun.ScrubTypes
	p.planToStreamColMap = identityMapInPlace(make([]int, len(distsqlrun.ScrubTypes)))

	dsp.FinalizePlan(planCtx, &p)
	return p, nil
}
