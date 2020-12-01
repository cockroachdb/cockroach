// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
)

// createScrubPhysicalCheck generates a plan for running a physical
// check for an index. The plan consists of TableReaders, with IsCheck
// enabled, that scan an index span. By having IsCheck enabled, the
// TableReaders will only emit errors encountered during scanning
// instead of row data. The plan is finalized.
func (dsp *DistSQLPlanner) createScrubPhysicalCheck(
	planCtx *PlanningCtx, n *scanNode,
) (*PhysicalPlan, error) {
	spec, _, err := initTableReaderSpec(n)
	if err != nil {
		return nil, err
	}

	spanPartitions, err := dsp.PartitionSpans(planCtx, n.spans)
	if err != nil {
		return nil, err
	}

	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(spanPartitions))
	for i, sp := range spanPartitions {
		tr := &execinfrapb.TableReaderSpec{}
		*tr = *spec
		tr.Spans = make([]execinfrapb.TableReaderSpan, len(sp.Spans))
		for j := range sp.Spans {
			tr.Spans[j].Span = sp.Spans[j]
		}

		corePlacement[i].NodeID = sp.Node
		corePlacement[i].Core.TableReader = tr
	}

	p := planCtx.NewPhysicalPlan()
	p.AddNoInputStage(corePlacement, execinfrapb.PostProcessSpec{}, rowexec.ScrubTypes, execinfrapb.Ordering{})
	p.PlanToStreamColMap = identityMapInPlace(make([]int, len(rowexec.ScrubTypes)))

	dsp.FinalizePlan(planCtx, p)
	return p, nil
}
