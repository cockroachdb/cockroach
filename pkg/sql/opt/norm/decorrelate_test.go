// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package norm_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// TestAllAggsIgnoreNullsOrNullOnEmpty verifies the assumption made in
// TranslateNonIgnoreAggs, that every aggregate will be handled somehow by it.
// If that restriction is lifted this test can be deleted.
func TestAllAggsIgnoreNullsOrNullOnEmpty(t *testing.T) {
	for op := range opt.AggregateOpReverseMap {
		switch op {
		case opt.CorrOp, opt.CountRowsOp:
			continue
		}
		if !opt.AggregateIgnoresNulls(op) && !opt.AggregateIsNullOnEmpty(op) {
			panic(errors.AssertionFailedf(
				"%s does not ignore nulls and is not null on empty", log.Safe(op),
			))
		}
	}
}
