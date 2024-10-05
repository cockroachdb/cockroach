// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCalculatePlanGrowth(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name   string
		before []physicalplan.Processor
		after  []physicalplan.Processor
		added  int
		frac   float64
	}{
		{
			name:  "zero to zero",
			added: 0, frac: 0.0,
		},
		{
			name:   "down to zero",
			before: []physicalplan.Processor{{SQLInstanceID: 1}},
			after:  []physicalplan.Processor{},
			added:  0, frac: 0.0,
		},
		{
			name:   "added from zero",
			before: []physicalplan.Processor{},
			after:  []physicalplan.Processor{{SQLInstanceID: 1}},
			added:  1, frac: math.Inf(1),
		},
		{
			name:   "changed inputs to same node",
			before: []physicalplan.Processor{{SQLInstanceID: 1}},
			after: []physicalplan.Processor{{SQLInstanceID: 1,
				Spec: execinfrapb.ProcessorSpec{Core: execinfrapb.ProcessorCoreUnion{ChangeAggregator: &execinfrapb.ChangeAggregatorSpec{JobID: 1}}}}},
			added: 1, frac: 1.0,
		},
		{
			name:   "moved to new node",
			before: []physicalplan.Processor{{SQLInstanceID: 1}},
			after:  []physicalplan.Processor{{SQLInstanceID: 2}},
			added:  1, frac: 1.0,
		},
		{
			name:   "added node",
			before: []physicalplan.Processor{{SQLInstanceID: 1}, {SQLInstanceID: 2}},
			after:  []physicalplan.Processor{{SQLInstanceID: 1}, {SQLInstanceID: 2}, {SQLInstanceID: 3}},
			added:  1, frac: 0.5,
		},
		{
			name:   "removed node",
			before: []physicalplan.Processor{{SQLInstanceID: 1}, {SQLInstanceID: 2}, {SQLInstanceID: 3}},
			after:  []physicalplan.Processor{{SQLInstanceID: 1}, {SQLInstanceID: 2}},
			added:  0, frac: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := PhysicalPlan{}
			after := PhysicalPlan{}
			before.PhysicalInfrastructure = &physicalplan.PhysicalInfrastructure{Processors: tc.before}
			after.PhysicalInfrastructure = &physicalplan.PhysicalInfrastructure{Processors: tc.after}
			added, frac := calculatePlanGrowth(&before, &after)
			require.Equal(t, tc.added, added)
			require.Equal(t, tc.frac, frac)
		})
	}
}
