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
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package status

import (
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestRuntimeStatRecorder(t *testing.T) {
	defer leaktest.AfterTest(t)
	manual := hlc.NewManualClock(100)
	recorder := NewRuntimeStatRecorder(proto.NodeID(1), hlc.NewClock(manual.UnixNano))

	data := recorder.GetTimeSeriesData()
	if a, e := len(data), 10; a != e {
		t.Fatalf("Expected %d series generated, got %d", a, e)
	}
}
