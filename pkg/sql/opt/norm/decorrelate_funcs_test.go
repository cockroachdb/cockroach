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

package norm_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestAllAggsIgnoreNullsOrNullOnEmpty verifies the assumption made in
// TranslateNonIgnoreAggs, that every aggregate will be handled somehow by it.
// If that restriction is lifted this test can be deleted.
func TestAllAggsIgnoreNullsOrNullOnEmpty(t *testing.T) {
	for op := range opt.AggregateOpReverseMap {
		if op == opt.CountRowsOp {
			// CountRows is the only aggregate allowed to break this rule.
			continue
		}
		if !opt.AggregateIgnoresNulls(op) && !opt.AggregateIsNullOnEmpty(op) {
			panic(pgerror.NewAssertionErrorf(
				"%s does not ignore nulls and is not null on empty", log.Safe(op),
			))
		}
	}
}
