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
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestNewStoreSpec verifies that the --store arguments are correctly parsed
// into StoreSpecs.
func TestNewStoreSpec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		value       string
		expectedErr string
		expected    StoreSpec
	}{
		// path
		{"path=/mnt/hda1", "", StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{}}},
		{",path=/mnt/hda1", "", StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{}}},
		{"path=/mnt/hda1,", "", StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{}}},
		{",,,path=/mnt/hda1,,,", "", StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{}}},
		{"/mnt/hda1", "", StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{}}},
		{"path=", "no value specified for path", StoreSpec{}},
		{"path=/mnt/hda1,path=/mnt/hda2", "path field was used twice in store definition", StoreSpec{}},
		{"/mnt/hda1,path=/mnt/hda2", "path field was used twice in store definition", StoreSpec{}},

		// attributes
		{"path=/mnt/hda1,attrs=ssd", "", StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{Attrs: []string{"ssd"}}}},
		{"path=/mnt/hda1,attrs=ssd:hdd", "", StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{Attrs: []string{"hdd", "ssd"}}}},
		{"path=/mnt/hda1,attrs=hdd:ssd", "", StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{Attrs: []string{"hdd", "ssd"}}}},
		{"attrs=ssd:hdd,path=/mnt/hda1", "", StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{Attrs: []string{"hdd", "ssd"}}}},
		{"attrs=hdd:ssd,path=/mnt/hda1,", "", StoreSpec{"/mnt/hda1", 0, 0, false, roachpb.Attributes{Attrs: []string{"hdd", "ssd"}}}},
		{"attrs=hdd:ssd", "no path specified", StoreSpec{}},
		{"path=/mnt/hda1,attrs=", "no value specified for attrs", StoreSpec{}},
		{"path=/mnt/hda1,attrs=hdd:hdd", "duplicate attribute given for store: hdd", StoreSpec{}},
		{"path=/mnt/hda1,attrs=hdd,attrs=ssd", "attrs field was used twice in store definition", StoreSpec{}},

		// size
		{"path=/mnt/hda1,size=671088640", "", StoreSpec{"/mnt/hda1", 671088640, 0, false, roachpb.Attributes{}}},
		{"path=/mnt/hda1,size=20GB", "", StoreSpec{"/mnt/hda1", 20000000000, 0, false, roachpb.Attributes{}}},
		{"size=20GiB,path=/mnt/hda1", "", StoreSpec{"/mnt/hda1", 21474836480, 0, false, roachpb.Attributes{}}},
		{"size=0.1TiB,path=/mnt/hda1", "", StoreSpec{"/mnt/hda1", 109951162777, 0, false, roachpb.Attributes{}}},
		{"path=/mnt/hda1,size=.1TiB", "", StoreSpec{"/mnt/hda1", 109951162777, 0, false, roachpb.Attributes{}}},
		{"path=/mnt/hda1,size=123TB", "", StoreSpec{"/mnt/hda1", 123000000000000, 0, false, roachpb.Attributes{}}},
		{"path=/mnt/hda1,size=123TiB", "", StoreSpec{"/mnt/hda1", 135239930216448, 0, false, roachpb.Attributes{}}},
		// %
		{"path=/mnt/hda1,size=50.5%", "", StoreSpec{"/mnt/hda1", 0, 50.5, false, roachpb.Attributes{}}},
		{"path=/mnt/hda1,size=100%", "", StoreSpec{"/mnt/hda1", 0, 100, false, roachpb.Attributes{}}},
		{"path=/mnt/hda1,size=1%", "", StoreSpec{"/mnt/hda1", 0, 1, false, roachpb.Attributes{}}},
		{"path=/mnt/hda1,size=0.999999%", "store size (0.999999%) must be between 1% and 100%", StoreSpec{}},
		{"path=/mnt/hda1,size=100.0001%", "store size (100.0001%) must be between 1% and 100%", StoreSpec{}},
		// 0.xxx
		{"path=/mnt/hda1,size=0.99", "", StoreSpec{"/mnt/hda1", 0, 99, false, roachpb.Attributes{}}},
		{"path=/mnt/hda1,size=0.5000000", "", StoreSpec{"/mnt/hda1", 0, 50, false, roachpb.Attributes{}}},
		{"path=/mnt/hda1,size=0.01", "", StoreSpec{"/mnt/hda1", 0, 1, false, roachpb.Attributes{}}},
		{"path=/mnt/hda1,size=0.009999", "store size (0.009999) must be between 1% and 100%", StoreSpec{}},
		// .xxx
		{"path=/mnt/hda1,size=.999", "", StoreSpec{"/mnt/hda1", 0, 99.9, false, roachpb.Attributes{}}},
		{"path=/mnt/hda1,size=.5000000", "", StoreSpec{"/mnt/hda1", 0, 50, false, roachpb.Attributes{}}},
		{"path=/mnt/hda1,size=.01", "", StoreSpec{"/mnt/hda1", 0, 1, false, roachpb.Attributes{}}},
		{"path=/mnt/hda1,size=.009999", "store size (.009999) must be between 1% and 100%", StoreSpec{}},
		// errors
		{"path=/mnt/hda1,size=0", "store size (0) must be larger than 640 MiB", StoreSpec{}},
		{"path=/mnt/hda1,size=abc", "could not parse store size (abc) strconv.ParseFloat: parsing \"\": invalid syntax", StoreSpec{}},
		{"path=/mnt/hda1,size=", "no value specified for size", StoreSpec{}},
		{"size=20GiB,path=/mnt/hda1,size=20GiB", "size field was used twice in store definition", StoreSpec{}},
		{"size=123TB", "no path specified", StoreSpec{}},

		// type
		{"type=mem,size=20GiB", "", StoreSpec{"", 21474836480, 0, true, roachpb.Attributes{}}},
		{"size=20GiB,type=mem", "", StoreSpec{"", 21474836480, 0, true, roachpb.Attributes{}}},
		{"size=20.5GiB,type=mem", "", StoreSpec{"", 22011707392, 0, true, roachpb.Attributes{}}},
		{"size=20GiB,type=mem,attrs=mem", "", StoreSpec{"", 21474836480, 0, true, roachpb.Attributes{Attrs: []string{"mem"}}}},
		{"type=mem,size=20", "store size (20) must be larger than 640 MiB", StoreSpec{}},
		{"type=mem,size=", "no value specified for size", StoreSpec{}},
		{"type=mem,attrs=ssd", "size must be specified for an in memory store", StoreSpec{}},
		{"path=/mnt/hda1,type=mem", "path specified for in memory store", StoreSpec{}},
		{"path=/mnt/hda1,type=other", "other is not a valid store type", StoreSpec{}},
		{"path=/mnt/hda1,type=mem,size=20GiB", "path specified for in memory store", StoreSpec{}},

		// all together
		{"path=/mnt/hda1,attrs=hdd:ssd,size=20GiB", "", StoreSpec{"/mnt/hda1", 21474836480, 0, false, roachpb.Attributes{Attrs: []string{"hdd", "ssd"}}}},
		{"type=mem,attrs=hdd:ssd,size=20GiB", "", StoreSpec{"", 21474836480, 0, true, roachpb.Attributes{Attrs: []string{"hdd", "ssd"}}}},

		// other error cases
		{"", "no value specified", StoreSpec{}},
		{",", "no path specified", StoreSpec{}},
		{",,,", "no path specified", StoreSpec{}},
		{"path=/mnt/hda1,something=abc", "something is not a valid store field", StoreSpec{}},
		{"something=abc", "something is not a valid store field", StoreSpec{}},
		{"type=mem,other=abc", "other is not a valid store field", StoreSpec{}},
	}

	for i, testCase := range testCases {
		storeSpec, err := newStoreSpec(testCase.value)
		if err != nil {
			if len(testCase.expectedErr) == 0 {
				t.Errorf("%d(%s): no expected error, got %s", i, testCase.value, err)
			}
			if testCase.expectedErr != fmt.Sprint(err) {
				t.Errorf("%d(%s): expected error \"%s\" does not match actual \"%s\"", i, testCase.value,
					testCase.expectedErr, err)
			}
			continue
		}
		if len(testCase.expectedErr) > 0 {
			t.Errorf("%d(%s): expected error %s but there was none", i, testCase.value, testCase.expectedErr)
			continue
		}
		if !reflect.DeepEqual(testCase.expected, storeSpec) {
			t.Errorf("%d(%s): actual doesn't match expected\nactual:   %+v\nexpected: %+v", i,
				testCase.value, storeSpec, testCase.expected)
		}

		// Now test String() to make sure the result can be parsed.
		storeSpecString := storeSpec.String()
		storeSpec2, err := newStoreSpec(storeSpecString)
		if err != nil {
			t.Errorf("%d(%s): error parsing String() result: %s", i, testCase.value, err)
			continue
		}
		// Compare strings to deal with floats not matching exactly.
		if !reflect.DeepEqual(storeSpecString, storeSpec2.String()) {
			t.Errorf("%d(%s): actual doesn't match expected\nactual:   %#+v\nexpected: %#+v", i, testCase.value,
				storeSpec, storeSpec2)
		}
	}
}
