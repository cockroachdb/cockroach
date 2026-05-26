// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package norm_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
			panic(errors.AssertionFailedf(
				"%s does not ignore nulls and is not null on empty", redact.Safe(op),
			))
		}
	}
}
