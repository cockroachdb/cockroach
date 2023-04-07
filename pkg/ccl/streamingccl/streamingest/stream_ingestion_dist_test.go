// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestMeasurePlanChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	makeProc := func(dstID base.SQLInstanceID, srcIDs []int) physicalplan.Processor {
		partitionMap := map[string]execinfrapb.StreamIngestionPartitionSpec{}
		for _, id := range srcIDs {
			partitionMap[fmt.Sprint(id)] = execinfrapb.StreamIngestionPartitionSpec{}
		}
		return physicalplan.Processor{SQLInstanceID: dstID,
			Spec: execinfrapb.ProcessorSpec{
				Core: execinfrapb.ProcessorCoreUnion{
					StreamIngestionData: &execinfrapb.StreamIngestionDataSpec{
						PartitionSpecs: partitionMap}}}}
	}

	makePlan := func(procs ...physicalplan.Processor) sql.PhysicalPlan {
		plan := sql.PhysicalPlan{}
		plan.PhysicalInfrastructure = &physicalplan.PhysicalInfrastructure{Processors: procs}
		return plan
	}

	for _, tc := range []struct {
		name   string
		before sql.PhysicalPlan
		after  sql.PhysicalPlan
		frac   float64
	}{
		{
			name:   "same node",
			before: makePlan(makeProc(1, []int{1})),
			after:  makePlan(makeProc(1, []int{1})),
			frac:   0,
		},
		{
			name:   "same nodes; swapped order",
			before: makePlan(makeProc(1, []int{1}), makeProc(1, []int{2})),
			after:  makePlan(makeProc(1, []int{2}), makeProc(1, []int{1})),
			frac:   0,
		},
		{
			name:   "dropped and added dest node",
			before: makePlan(makeProc(1, []int{1})),
			after:  makePlan(makeProc(2, []int{1})),
			frac:   1,
		},
		{
			name:   "added src and dest node",
			before: makePlan(makeProc(1, []int{1})),
			after:  makePlan(makeProc(1, []int{1}), makeProc(2, []int{2})),
			frac:   1,
		},
		{
			name:   "dropped src and dest node",
			before: makePlan(makeProc(1, []int{1}), makeProc(2, []int{2})),
			after:  makePlan(makeProc(1, []int{1})),
			frac:   0.5,
		},
		{
			name:   "added dest node",
			before: makePlan(makeProc(1, []int{1, 2, 3})),
			after:  makePlan(makeProc(1, []int{1, 3}), makeProc(2, []int{2})),
			frac:   .25,
		},
		{
			name:   "dropped dest node",
			before: makePlan(makeProc(1, []int{1, 3}), makeProc(2, []int{2})),
			after:  makePlan(makeProc(1, []int{1, 2, 3})),
			frac:   .2,
		},
		{
			name:   "dropped and added src node",
			before: makePlan(makeProc(1, []int{1})),
			after:  makePlan(makeProc(1, []int{2})),
			frac:   1,
		},
		{
			name:   "new src node",
			before: makePlan(makeProc(1, []int{1})),
			after:  makePlan(makeProc(1, []int{1, 2})),
			frac:   0.5,
		},
		{
			name:   "dropped src node",
			before: makePlan(makeProc(1, []int{1, 2, 3})),
			after:  makePlan(makeProc(1, []int{1, 2})),
			frac:   .25,
		},
		{
			name:   "swapped dest node",
			before: makePlan(makeProc(1, []int{1}), makeProc(2, []int{2})),
			after:  makePlan(makeProc(1, []int{2}), makeProc(2, []int{1})),
			frac:   0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frac := measurePlanChange(&tc.before, &tc.after)
			require.Equal(t, tc.frac, frac)
		})
	}
}
