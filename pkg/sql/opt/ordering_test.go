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

package opt_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
)

func TestOrdering(t *testing.T) {
	// Add Ordering props.
	ordering := opt.Ordering{1, 5}

	if ordering.Empty() {
		t.Error("ordering not empty")
	}

	if !ordering.Provides(ordering) {
		t.Error("ordering should provide itself")
	}

	if !ordering.Provides(opt.Ordering{1}) {
		t.Error("ordering should provide the prefix ordering")
	}

	if (opt.Ordering{}).Provides(ordering) {
		t.Error("empty ordering should not provide ordering")
	}

	if !ordering.Provides(opt.Ordering{}) {
		t.Error("ordering should provide the empty ordering")
	}

	if !ordering.Equals(ordering) {
		t.Error("ordering should be equal with itself")
	}

	if ordering.Equals(opt.Ordering{}) {
		t.Error("ordering should not equal the empty ordering")
	}

	if (opt.Ordering{}).Equals(ordering) {
		t.Error("empty ordering should not equal ordering")
	}
}
