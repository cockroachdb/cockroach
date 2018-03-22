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

package rate

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestRateMath(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := NewRate(1, time.Minute)
	if r.Per(time.Hour) != 60 {
		t.Errorf("expecting %f, got %f", 60.0, r.Per(time.Hour))
	}

	if NewRate(1, time.Hour) >= NewRate(1, time.Second) {
		t.Errorf("comparison incorrect")
	}
}
