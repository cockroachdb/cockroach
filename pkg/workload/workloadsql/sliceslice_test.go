// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workloadsql

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestSliceSliceInterfaceSort(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sorted := [][]interface{}{
		{},
		{``},
		{`a`},
		{`a`, -9223372036854775808},
		{`a`, 2},
		{`a`, 12},
		{`b`},
	}

	// Create a shuffled version of sorted.
	actual := make([][]interface{}, len(sorted))
	for i, v := range rand.Perm(len(actual)) {
		actual[v] = sorted[i]
	}

	sort.Sort(sliceSliceInterface(actual))
	if !reflect.DeepEqual(actual, sorted) {
		t.Fatalf(`got %v expected %v`, actual, sorted)
	}
}
