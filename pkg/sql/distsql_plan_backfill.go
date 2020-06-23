// Copyright 2016 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func initBackfillerSpec(
	backfillType backfillType,
	desc sqlbase.TableDescriptor,
	duration time.Duration,
	chunkSize int64,
	readAsOf hlc.Timestamp,
) (execinfrapb.BackfillerSpec, error) {
	ret := execinfrapb.BackfillerSpec{
		Table:     desc,
		Duration:  duration,
		ChunkSize: chunkSize,
		ReadAsOf:  readAsOf,
	}
	switch backfillType {
	case indexBackfill:
		ret.Type = execinfrapb.BackfillerSpec_Index
	case columnBackfill:
		ret.Type = execinfrapb.BackfillerSpec_Column
	default:
		return execinfrapb.BackfillerSpec{}, errors.Errorf("bad backfill type %d", backfillType)
	}
	return ret, nil
}

// createBackfiller generates a plan consisting of index/column backfiller
// processors, one for each node that has spans that we are reading. The plan is
// finalized.
func (dsp *DistSQLPlanner) createBackfiller(
	planCtx *PlanningCtx,
	backfillType backfillType,
	desc sqlbase.TableDescriptor,
	duration time.Duration,
	chunkSize int64,
	spans []roachpb.Span,
	readAsOf hlc.Timestamp,
) (PhysicalPlan, error) {
	spec, err := initBackfillerSpec(backfillType, desc, duration, chunkSize, readAsOf)
	if err != nil {
		return PhysicalPlan{}, err
	}

	spanPartitions, err := dsp.PartitionSpans(planCtx, spans)
	if err != nil {
		return PhysicalPlan{}, err
	}

	p := MakePhysicalPlan(dsp.gatewayNodeID)
	p.ResultRouters = make([]physicalplan.ProcessorIdx, len(spanPartitions))
	for i, sp := range spanPartitions {
		ib := &execinfrapb.BackfillerSpec{}
		*ib = spec
		ib.Spans = make([]execinfrapb.TableReaderSpan, len(sp.Spans))
		for j := range sp.Spans {
			ib.Spans[j].Span = sp.Spans[j]
		}

		proc := physicalplan.Processor{
			Node: sp.Node,
			Spec: execinfrapb.ProcessorSpec{
				Core:   execinfrapb.ProcessorCoreUnion{Backfiller: ib},
				Output: []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}
	dsp.FinalizePlan(planCtx, &p)
	return p, nil
}
