// Copyright 2018 The Cockroach Authors.
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

package ts

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/kr/pretty"
)

func TestComputeRollup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		input    tspb.TimeSeriesData
		expected []roachpb.InternalTimeSeriesData
	}{
		{
			input: ts("test.metric",
				tsdp(10, 200),
				tsdp(20, 300),
				tsdp(40, 400),
				tsdp(80, 400),
				tsdp(97, 400),
				tsdp(201, 41234),
				tsdp(249, 423),
				tsdp(424, 123),
				tsdp(425, 342),
				tsdp(426, 643),
				tsdp(427, 835),
				tsdp(1023, 999),
				tsdp(1048, 888),
				tsdp(1123, 999),
				tsdp(1248, 888),
				tsdp(1323, 999),
				tsdp(1348, 888),
			),
			expected: []roachpb.InternalTimeSeriesData{
				makeInternalColumnData(0, 50, []dataSample{
					{10, 200},
					{20, 300},
					{40, 400},
					{80, 400},
					{97, 400},
					{201, 41234},
					{249, 423},
					{424, 123},
					{425, 342},
					{426, 643},
					{427, 835},
				}),
				makeInternalColumnData(1000, 50, []dataSample{
					{1023, 999},
					{1048, 888},
					{1123, 999},
					{1248, 888},
					{1323, 999},
					{1348, 888},
				}),
			},
		},
		{
			input: ts("test.metric",
				tsdp(1023, 999),
				tsdp(1048, 888),
				tsdp(1123, 999),
				tsdp(1248, 888),
			),
			expected: []roachpb.InternalTimeSeriesData{
				makeInternalColumnData(1000, 50, []dataSample{
					{1023, 999},
					{1048, 888},
					{1123, 999},
					{1248, 888},
				}),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			rollups := computeRollupsFromData(tc.input, 50)
			internal, err := rollups.toInternal(1000, 50)
			if err != nil {
				t.Fatal(err)
			}
			if a, e := internal, tc.expected; !reflect.DeepEqual(a, e) {
				for _, diff := range pretty.Diff(a, e) {
					t.Error(diff)
				}
			}
		})
	}
}

func TestRollupToInternal(t *testing.T) {
	defer leaktest.AfterTest(t)()
}
