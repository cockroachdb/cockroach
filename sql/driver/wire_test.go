// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package driver

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestDatumString(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		value    interface{}
		expected string
	}{
		{nil, "NULL"},
		{false, "false"},
		{true, "true"},
		{int64(-2), "-2"},
		{float64(4.5), "4.5"},
		{[]byte("6"), "6"},
		{"hello", "hello"},
		{time.Date(2015, 9, 6, 2, 19, 36, 342, time.UTC), "2015-09-06 02:19:36.000000342 +0000 UTC"},
	}
	for i, d := range testData {
		datum, err := makeDatum(d.value)
		if err != nil {
			t.Fatal(err)
		}
		s := datum.String()
		if d.expected != s {
			t.Errorf("%d: expected %s, but got %s", i, d.expected, s)
		}
	}
}
