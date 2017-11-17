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

package tests

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
)

// CreateTestServerParams creates a set of params suitable for SQL tests.
// It enables some EndTransaction sanity checking and installs a flexible
// TestingEvalFilter.
// TODO(andrei): this function is not used consistently by SQL tests. Figure out
// if the EndTransaction checks are important.
func CreateTestServerParams() (base.TestServerArgs, *CommandFilters) {
	var cmdFilters CommandFilters
	cmdFilters.AppendFilter(CheckEndTransactionTrigger, true)
	params := base.TestServerArgs{}
	params.Knobs.Store = &storage.StoreTestingKnobs{
		EvalKnobs: batcheval.TestingKnobs{
			TestingEvalFilter: cmdFilters.RunFilters,
		},
	}
	return params, &cmdFilters
}
