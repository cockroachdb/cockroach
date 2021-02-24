// Copyright 2021 The Cockroach Authors.
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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestMergeResultTypesForSetOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	empty := []*types.T{}
	null := []*types.T{types.Unknown}
	typeInt := []*types.T{types.Int}
	typeTuple := []*types.T{types.MakeTuple(typeInt)}

	testData := []struct {
		name     string
		left     []*types.T
		right    []*types.T
		expected *[]*types.T
		err      bool
	}{
		{"both empty", empty, empty, &empty, false},
		{"left empty", empty, typeInt, nil, true},
		{"right empty", typeInt, empty, nil, true},
		{"both null", null, null, &null, false},
		{"left null", null, typeInt, &typeInt, false},
		{"right null", typeInt, null, &typeInt, false},
		{"both int", typeInt, typeInt, &typeInt, false},
		{"left null, right tuple", null, typeTuple, &typeTuple, false},
		{"right null, left tuple", typeTuple, null, &typeTuple, false},
	}
	checkUnknownTypesUpdate := func(plan PhysicalPlan, orig, merged []*types.T) {
		for i, typ := range plan.GetResultTypes() {
			if orig[i].Family() == types.UnknownFamily {
				if typ.Family() == types.UnknownFamily && merged[i].Family() == types.TupleFamily {
					t.Fatal("should have updated types NULL to tuple type on the original plan")
				}
				if typ.Family() != types.UnknownFamily && merged[i].Family() != types.TupleFamily {
					t.Fatal("should have NOT updated types NULL to tuple type on the original plan")
				}
			}
		}
	}
	infra := physicalplan.MakePhysicalInfrastructure(uuid.FastMakeV4(), roachpb.NodeID(1))
	var leftPlan, rightPlan PhysicalPlan
	leftPlan.PhysicalInfrastructure = &infra
	rightPlan.PhysicalInfrastructure = &infra
	leftPlan.ResultRouters = []physicalplan.ProcessorIdx{infra.AddProcessor(physicalplan.Processor{})}
	rightPlan.ResultRouters = []physicalplan.ProcessorIdx{infra.AddProcessor(physicalplan.Processor{})}
	for _, td := range testData {
		t.Run(td.name, func(t *testing.T) {
			leftPlan.Processors[0].Spec.ResultTypes = td.left
			rightPlan.Processors[1].Spec.ResultTypes = td.right
			result, err := mergeResultTypesForSetOp(&leftPlan, &rightPlan)
			if td.err {
				if err == nil {
					t.Fatalf("expected error, got %+v", result)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if !reflect.DeepEqual(*td.expected, result) {
				t.Fatalf("expected %+v, got %+v", *td.expected, result)
			}
			checkUnknownTypesUpdate(leftPlan, td.left, result)
			checkUnknownTypesUpdate(rightPlan, td.right, result)
		})
	}
}
