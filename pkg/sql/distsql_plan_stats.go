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
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/pkg/errors"
)

type requestedStat struct {
	columns             []sqlbase.ColumnID
	histogram           bool
	histogramMaxBuckets int
	name                string
}

const histogramSamples = 10000
const histogramBuckets = 200

func (dsp *DistSQLPlanner) createStatsPlan(
	planCtx *planningCtx, desc *sqlbase.TableDescriptor, stats []requestedStat,
) (physicalPlan, error) {
	// Create the table readers; for this we initialize a dummy scanNode.
	scan := scanNode{desc: desc}
	err := scan.initDescDefaults(nil /* planDependencies */, publicColumnsCfg)
	if err != nil {
		return physicalPlan{}, err
	}
	scan.spans, err = unconstrainedSpans(desc, scan.index)
	if err != nil {
		return physicalPlan{}, err
	}

	// Calculate the relevant columns.
	scan.valNeededForCol = util.FastIntSet{}
	var sampledColumnIDs []sqlbase.ColumnID
	for _, s := range stats {
		for _, c := range s.columns {
			colIdx, ok := scan.colIdxMap[c]
			if !ok {
				return physicalPlan{}, errors.Errorf("unknown column ID %d", c)
			}
			if !scan.valNeededForCol.Contains(colIdx) {
				scan.valNeededForCol.Add(colIdx)
				sampledColumnIDs = append(sampledColumnIDs, c)
			}
		}
	}

	p, err := dsp.createTableReaders(planCtx, &scan, nil /* overrideResultColumns */)
	if err != nil {
		return physicalPlan{}, err
	}

	sketchSpecs := make([]distsqlrun.SketchSpec, len(stats))
	post := p.GetLastStagePost()
	for i, s := range stats {
		spec := distsqlrun.SketchSpec{
			SketchType:          distsqlrun.SketchType_HLL_PLUS_PLUS_V1,
			GenerateHistogram:   s.histogram,
			HistogramMaxBuckets: uint32(s.histogramMaxBuckets),
			Columns:             make([]uint32, len(s.columns)),
			StatName:            s.name,
		}
		for i, colID := range s.columns {
			colIdx, ok := scan.colIdxMap[colID]
			if !ok {
				panic("columns should have been checked already")
			}
			// The table readers can have a projection; we need to remap the column
			// index accordingly.
			if post.Projection {
				found := false
				for i, outColIdx := range post.OutputColumns {
					if int(outColIdx) == colIdx {
						// Column colIdx is the i-th output column.
						colIdx = i
						found = true
						break
					}
				}
				if !found {
					panic("projection should include all needed columns")
				}
			}
			spec.Columns[i] = uint32(colIdx)
		}

		sketchSpecs[i] = spec
	}

	// Set up the samplers.
	sampler := &distsqlrun.SamplerSpec{Sketches: sketchSpecs}
	for _, s := range stats {
		if s.histogram {
			sampler.SampleSize = histogramSamples
			break
		}
	}

	// The sampler outputs the original columns plus a rank column and four sketch columns.
	outTypes := make([]sqlbase.ColumnType, 0, len(p.ResultTypes)+5)
	outTypes = append(outTypes, p.ResultTypes...)
	// An INT column for the rank of each row.
	outTypes = append(outTypes, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT})
	// An INT column indicating the sketch index.
	outTypes = append(outTypes, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT})
	// An INT column indicating the number of rows processed.
	outTypes = append(outTypes, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT})
	// An INT column indicating the number of rows that have a NULL in any sketch
	// column.
	outTypes = append(outTypes, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT})
	// A BYTES column with the sketch data.
	outTypes = append(outTypes, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES})

	p.AddNoGroupingStage(
		distsqlrun.ProcessorCoreUnion{Sampler: sampler},
		distsqlrun.PostProcessSpec{},
		outTypes,
		distsqlrun.Ordering{},
	)

	// Set up the final SampleAggregator stage.
	agg := &distsqlrun.SampleAggregatorSpec{
		Sketches:         sketchSpecs,
		SampleSize:       sampler.SampleSize,
		SampledColumnIDs: sampledColumnIDs,
		TableID:          desc.ID,
	}
	// Plan the SampleAggregator on the gateway, unless we have a single Sampler.
	node := dsp.nodeDesc.NodeID
	if len(p.ResultRouters) == 1 {
		node = p.Processors[p.ResultRouters[0]].Node
	}
	p.AddSingleGroupStage(
		node,
		distsqlrun.ProcessorCoreUnion{SampleAggregator: agg},
		distsqlrun.PostProcessSpec{},
		[]sqlbase.ColumnType{},
	)
	return p, nil
}

func (dsp *DistSQLPlanner) createPlanForCreateStats(
	planCtx *planningCtx, n *createStatsNode,
) (physicalPlan, error) {

	stats := []requestedStat{
		{
			columns:             n.columns,
			histogram:           len(n.ColumnNames) == 1,
			histogramMaxBuckets: histogramBuckets,
			name:                string(n.Name),
		},
	}

	return dsp.createStatsPlan(planCtx, n.tableDesc, stats)
}
