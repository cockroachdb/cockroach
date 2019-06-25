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
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

func initBackfillerSpec(
	backfillType backfillType,
	desc sqlbase.TableDescriptor,
	duration time.Duration,
	chunkSize int64,
	otherTables []sqlbase.TableDescriptor,
	readAsOf hlc.Timestamp,
) (distsqlpb.BackfillerSpec, error) {
	ret := distsqlpb.BackfillerSpec{
		Table:       desc,
		Duration:    duration,
		ChunkSize:   chunkSize,
		OtherTables: otherTables,
		ReadAsOf:    readAsOf,
	}
	switch backfillType {
	case indexBackfill:
		ret.Type = distsqlpb.BackfillerSpec_Index
	case columnBackfill:
		ret.Type = distsqlpb.BackfillerSpec_Column
	default:
		return distsqlpb.BackfillerSpec{}, errors.Errorf("bad backfill type %d", backfillType)
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
	otherTables []sqlbase.TableDescriptor,
	readAsOf hlc.Timestamp,
) (PhysicalPlan, error) {
	spec, err := initBackfillerSpec(backfillType, desc, duration, chunkSize, otherTables, readAsOf)
	if err != nil {
		return PhysicalPlan{}, err
	}

	spanPartitions, err := dsp.PartitionSpans(planCtx, spans)
	if err != nil {
		return PhysicalPlan{}, err
	}

	var p PhysicalPlan
	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(spanPartitions))
	for i, sp := range spanPartitions {
		ib := &distsqlpb.BackfillerSpec{}
		*ib = spec
		ib.Spans = make([]distsqlpb.TableReaderSpan, len(sp.Spans))
		for j := range sp.Spans {
			ib.Spans[j].Span = sp.Spans[j]
		}

		proc := distsqlplan.Processor{
			Node: sp.Node,
			Spec: distsqlpb.ProcessorSpec{
				Core:   distsqlpb.ProcessorCoreUnion{Backfiller: ib},
				Output: []distsqlpb.OutputRouterSpec{{Type: distsqlpb.OutputRouterSpec_PASS_THROUGH}},
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}
	dsp.FinalizePlan(planCtx, &p)
	return p, nil
}
