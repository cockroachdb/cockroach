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

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestMergeResultTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	empty := []*types.T{}
	null := []*types.T{types.Unknown}
	typeInt := []*types.T{types.Int}

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
	}
	for _, td := range testData {
		t.Run(td.name, func(t *testing.T) {
			result, err := mergeResultTypes(td.left, td.right)
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
		})
	}
}
