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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/errors"
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
	writeAtBatchTimestamp bool,
	chunkSize int64,
	indexesToBackfill []descpb.IndexID,
) (execinfrapb.BackfillerSpec, error) {
	return execinfrapb.BackfillerSpec{
		Table:                 desc,
		WriteAsOf:             writeAsOf,
		WriteAtBatchTimestamp: writeAtBatchTimestamp,
		ReadAsOf:              readAsOf,
		Type:                  execinfrapb.BackfillerSpec_Index,
		ChunkSize:             chunkSize,
		IndexesToBackfill:     indexesToBackfill,
	}, nil
}

func initIndexBackfillMergerSpec(
	desc descpb.TableDescriptor, addedIndexes []descpb.IndexID, temporaryIndexes []descpb.IndexID,
) (execinfrapb.IndexBackfillMergerSpec, error) {
	return execinfrapb.IndexBackfillMergerSpec{
		Table:            desc,
		AddedIndexes:     addedIndexes,
		TemporaryIndexes: temporaryIndexes,
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
		ib.InitialSplits = int32(len(spanPartitions))
		ib.Spans = sp.Spans

		proc := physicalplan.Processor{
			SQLInstanceID: sp.SQLInstanceID,
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

// createIndexBackfillerMergePhysicalPlan generates a plan consisting
// of index merger processors, one for each node that has spans that
// we are reading. The plan is finalized.
func (dsp *DistSQLPlanner) createIndexBackfillerMergePhysicalPlan(
	planCtx *PlanningCtx, spec execinfrapb.IndexBackfillMergerSpec, spans [][]roachpb.Span,
) (*PhysicalPlan, error) {

	var n int
	for _, sp := range spans {
		for range sp {
			n++
		}
	}
	indexSpans := make([]roachpb.Span, 0, n)
	spanIdxs := make([]spanAndIndex, 0, n)
	spanIdxTree := interval.NewTree(interval.ExclusiveOverlapper)
	for i := range spans {
		for j := range spans[i] {
			indexSpans = append(indexSpans, spans[i][j])
			spanIdxs = append(spanIdxs, spanAndIndex{Span: spans[i][j], idx: i})
			if err := spanIdxTree.Insert(&spanIdxs[len(spanIdxs)-1], true /* fast */); err != nil {
				return nil, err
			}

		}
	}
	spanIdxTree.AdjustRanges()
	getIndex := func(sp roachpb.Span) (idx int) {
		if !spanIdxTree.DoMatching(func(i interval.Interface) (done bool) {
			idx = i.(*spanAndIndex).idx
			return true
		}, sp.AsRange()) {
			panic(errors.AssertionFailedf("no matching index found for span: %s", sp))
		}
		return idx
	}

	spanPartitions, err := dsp.PartitionSpans(planCtx, indexSpans)
	if err != nil {
		return nil, err
	}

	p := planCtx.NewPhysicalPlan()
	p.ResultRouters = make([]physicalplan.ProcessorIdx, len(spanPartitions))
	for i, sp := range spanPartitions {
		ibm := &execinfrapb.IndexBackfillMergerSpec{}
		*ibm = spec

		ibm.Spans = sp.Spans
		for _, sp := range ibm.Spans {
			ibm.SpanIdx = append(ibm.SpanIdx, int32(getIndex(sp)))
		}

		proc := physicalplan.Processor{
			SQLInstanceID: sp.SQLInstanceID,
			Spec: execinfrapb.ProcessorSpec{
				Core:        execinfrapb.ProcessorCoreUnion{IndexBackfillMerger: ibm},
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

type spanAndIndex struct {
	roachpb.Span
	idx int
}

var _ interval.Interface = (*spanAndIndex)(nil)

func (si *spanAndIndex) Range() interval.Range { return si.AsRange() }
func (si *spanAndIndex) ID() uintptr           { return uintptr(unsafe.Pointer(si)) }
