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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func initColumnBackfillerSpec(
	desc descpb.TableDescriptor, duration time.Duration, chunkSize int64, readAsOf hlc.Timestamp,
) (execinfrapb.BackfillerSpec, error) {
	return execinfrapb.BackfillerSpec{
		Table:     desc,
		Duration:  duration,
		ChunkSize: chunkSize,
		ReadAsOf:  readAsOf,
		Type:      execinfrapb.BackfillerSpec_Column,
	}, nil
}

func initIndexBackfillerSpec(
	desc descpb.TableDescriptor,
	writeAsOf, readAsOf hlc.Timestamp,
	chunkSize int64,
	indexesToBackfill []descpb.IndexID,
) (execinfrapb.BackfillerSpec, error) {
	return execinfrapb.BackfillerSpec{
		Table:             desc,
		WriteAsOf:         writeAsOf,
		ReadAsOf:          readAsOf,
		Type:              execinfrapb.BackfillerSpec_Index,
		ChunkSize:         chunkSize,
		IndexesToBackfill: indexesToBackfill,
	}, nil
}

// createBackfiller generates a plan consisting of index/column backfiller
// processors, one for each node that has spans that we are reading. The plan is
// finalized.
func (dsp *DistSQLPlanner) createBackfillerPhysicalPlan(
	planCtx *PlanningCtx, spec execinfrapb.BackfillerSpec, spans []roachpb.Span,
) (*PhysicalPlan, error) {
	spanPartitions, err := dsp.PartitionSpans(planCtx, spans)
	if err != nil {
		return nil, err
	}

	p := planCtx.NewPhysicalPlan()
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
				Core:        execinfrapb.ProcessorCoreUnion{Backfiller: ib},
				Output:      []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				ResultTypes: []*types.T{},
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}
	dsp.FinalizePlan(planCtx, p)
	return p, nil
}
