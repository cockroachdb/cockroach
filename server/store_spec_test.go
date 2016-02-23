// Copyright 2016 The Cockroach Authors.
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
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package server

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestNewStoreSpec verifies that the --store arguments are correctly parsed
// into StoreSpecs.
func TestNewStoreSpec(t *testing.T) {
	defer leaktest.AfterTest(t)

	testCases := []struct {
		value       string
		expectedErr bool
		expected    StoreSpec
	}{
		// path
		{"path=/mnt/hda1", false, StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{",path=/mnt/hda1", false, StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"path=/mnt/hda1,", false, StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{",,,path=/mnt/hda1,,,", false, StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"/mnt/hda1", false, StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"path=", true, StoreSpec{}},
		{"path=/mnt/hda1,path=/mnt/hda2", true, StoreSpec{}},
		{"/mnt/hda1,path=/mnt/hda2", true, StoreSpec{}},

		// attributes
		{"path=/mnt/hda1,attr=ssd", false, StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{Attrs: []string{"ssd"}}}},
		{"path=/mnt/hda1,attr=ssd:hdd", false, StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{Attrs: []string{"hdd", "ssd"}}}},
		{"path=/mnt/hda1,attr=hdd:ssd", false, StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{Attrs: []string{"hdd", "ssd"}}}},
		{"attr=ssd:hdd,path=/mnt/hda1", false, StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{Attrs: []string{"hdd", "ssd"}}}},
		{"attr=hdd:ssd,path=/mnt/hda1,", false, StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{Attrs: []string{"hdd", "ssd"}}}},
		{"attr=hdd:ssd", true, StoreSpec{}},
		{"path=/mnt/hda1,attr=", true, StoreSpec{}},
		{"path=/mnt/hda1,attr=hdd:hdd", true, StoreSpec{}},
		{"path=/mnt/hda1,attr=hdd,attr=ssd", true, StoreSpec{}},

		// size
		{"path=/mnt/hda1,size=671088640", false, StoreSpec{"/mnt/hda1", 671088640, 0, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"path=/mnt/hda1,size=20GB", false, StoreSpec{"/mnt/hda1", 20000000000, 0, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"size=20GiB,path=/mnt/hda1", false, StoreSpec{"/mnt/hda1", 21474836480, 0, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"size=0.1TiB,path=/mnt/hda1", false, StoreSpec{"/mnt/hda1", 109951162777, 0, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"path=/mnt/hda1,size=.1TiB", false, StoreSpec{"/mnt/hda1", 109951162777, 0, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"path=/mnt/hda1,size=123TB", false, StoreSpec{"/mnt/hda1", 123000000000000, 0, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"path=/mnt/hda1,size=123TiB", false, StoreSpec{"/mnt/hda1", 135239930216448, 0, false, roachpb.Attributes{Attrs: []string(nil)}}},
		// %
		{"path=/mnt/hda1,size=50.5%", false, StoreSpec{"/mnt/hda1", 0, 50.5, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"path=/mnt/hda1,size=100%", false, StoreSpec{"/mnt/hda1", 0, 100, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"path=/mnt/hda1,size=1%", false, StoreSpec{"/mnt/hda1", 0, 1, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"path=/mnt/hda1,size=0.999999%", true, StoreSpec{}},
		{"path=/mnt/hda1,size=100.0001%", true, StoreSpec{}},
		// 0.xxx
		{"path=/mnt/hda1,size=0.99", false, StoreSpec{"/mnt/hda1", 0, 99, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"path=/mnt/hda1,size=0.5000000", false, StoreSpec{"/mnt/hda1", 0, 50, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"path=/mnt/hda1,size=0.01", false, StoreSpec{"/mnt/hda1", 0, 1, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"path=/mnt/hda1,size=0.009999", true, StoreSpec{}},
		// .xxx
		{"path=/mnt/hda1,size=.999", false, StoreSpec{"/mnt/hda1", 0, 99.9, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"path=/mnt/hda1,size=.5000000", false, StoreSpec{"/mnt/hda1", 0, 50, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"path=/mnt/hda1,size=.01", false, StoreSpec{"/mnt/hda1", 0, 1, false, roachpb.Attributes{Attrs: []string(nil)}}},
		{"path=/mnt/hda1,size=.009999", true, StoreSpec{}},
		// errors
		{"path=/mnt/hda1,size=0", true, StoreSpec{}},
		{"path=/mnt/hda1,size=abc", true, StoreSpec{}},
		{"path=/mnt/hda1,size=", true, StoreSpec{}},
		{"size=20GiB,path=/mnt/hda1,size=20GiB", true, StoreSpec{}},
		{"size=123TB", true, StoreSpec{}},

		// type
		{"type=mem,size=20GiB", false, StoreSpec{"", 21474836480, 0, true, roachpb.Attributes{Attrs: []string(nil)}}},
		{"size=20GiB,type=mem", false, StoreSpec{"", 21474836480, 0, true, roachpb.Attributes{Attrs: []string(nil)}}},
		{"size=20.5GiB,type=mem", false, StoreSpec{"", 22011707392, 0, true, roachpb.Attributes{Attrs: []string(nil)}}},
		{"size=20GiB,type=mem,attr=mem", false, StoreSpec{"", 21474836480, 0, true, roachpb.Attributes{Attrs: []string{"mem"}}}},
		{"type=mem,size=20", true, StoreSpec{}},
		{"type=mem,size=", true, StoreSpec{}},
		{"type=mem,attr=ssd", true, StoreSpec{}},
		{"path=/mnt/hda1,type=mem", true, StoreSpec{}},
		{"path=/mnt/hda1,type=other", true, StoreSpec{}},
		{"path=/mnt/hda1,type=mem,size=20GiB", true, StoreSpec{}},

		// all together
		{"path=/mnt/hda1,attr=hdd:ssd,size=20GiB", false, StoreSpec{"/mnt/hda1", 21474836480, 0, false, roachpb.Attributes{Attrs: []string{"hdd", "ssd"}}}},
		{"type=mem,attr=hdd:ssd,size=20GiB", false, StoreSpec{"", 21474836480, 0, true, roachpb.Attributes{Attrs: []string{"hdd", "ssd"}}}},

		// other error cases
		{"", true, StoreSpec{}},
		{",", true, StoreSpec{}},
		{",,,", true, StoreSpec{}},
		{"path=/mnt/hda1,something=abc", true, StoreSpec{}},
		{"something=abc", true, StoreSpec{}},
		{"type=mem,other=abc", true, StoreSpec{}},
	}

	for i, testCase := range testCases {
		storeSpec, err := newStoreSpec(testCase.value)
		if err != nil {
			if !testCase.expectedErr {
				t.Errorf("%d: %s", i, err)
			}
			continue
		}
		if testCase.expectedErr {
			t.Errorf("%d: expected error but there was none", i)
			continue
		}
		if !reflect.DeepEqual(testCase.expected, storeSpec) {
			t.Errorf("%d: actual doesn't match expected\nactual:   %+v\nexpected: %+v\n", i, storeSpec,
				testCase.expected)
		}

		// Now test the String() to make sure the result can be parsed.
		storeSpecString := storeSpec.String()
		storeSpec2, err := newStoreSpec(storeSpecString)
		if err != nil {
			t.Errorf("%d: error parsing String() result: %s", i, err)
			continue
		}
		// Compare strings to deal with floats not matching exactly.
		if !reflect.DeepEqual(storeSpecString, storeSpec2.String()) {
			t.Errorf("%d: actual doesn't match expected\nactual:   %#+v\nexpected: %#+v\n", i, storeSpec,
				storeSpec2)
		}
	}
}
