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

package memo_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

func TestCostLess(t *testing.T) {
	testLess := func(left, right memo.Cost, expected bool) {
		if left.Less(right) != expected {
			t.Errorf("expected %v.Less(%v) to be %v, got %v", left, right, expected, !expected)
		}
	}

	testLess(memo.Cost(0.0), memo.Cost(1.0), true)
	testLess(memo.Cost(0.0), memo.Cost(0.0), false)
	testLess(memo.Cost(1.0), memo.Cost(0.0), false)
}

func TestCostSub(t *testing.T) {
	testSub := func(left, right memo.Cost, expected memo.Cost) {
		actual := left.Sub(right)
		if actual != expected {
			t.Errorf("expected %v.Sub(%v) to be %v, got %v", left, right, expected, actual)
		}
	}

	testSub(memo.Cost(10.0), memo.Cost(3.0), memo.Cost(7.0))
	testSub(memo.Cost(3.0), memo.Cost(10.0), memo.Cost(-7.0))
	testSub(memo.Cost(10.0), memo.Cost(10.0), memo.Cost(0.0))
}
