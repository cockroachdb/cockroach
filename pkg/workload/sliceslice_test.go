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

package workload

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
