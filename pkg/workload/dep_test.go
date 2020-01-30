// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workload

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDepWhitelist(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We want workload to be lightweight. If you need to add a package to this
	// set of deps, run it by danhhz first.
	buildutil.VerifyTransitiveWhitelist(t, "github.com/cockroachdb/cockroach/pkg/workload",
		[]string{
			`github.com/cockroachdb/cockroach/pkg/col/coldata`,
			`github.com/cockroachdb/cockroach/pkg/col/coltypes`,
			`github.com/cockroachdb/cockroach/pkg/util/arith`,
			`github.com/cockroachdb/cockroach/pkg/util/bufalloc`,
			`github.com/cockroachdb/cockroach/pkg/util/duration`,
			`github.com/cockroachdb/cockroach/pkg/util/encoding/csv`,
			`github.com/cockroachdb/cockroach/pkg/util/stacktrace`,
			`github.com/cockroachdb/cockroach/pkg/util/syncutil`,
			`github.com/cockroachdb/cockroach/pkg/util/timeutil`,
			`github.com/cockroachdb/cockroach/pkg/workload/histogram`,
			// TODO(dan): These really shouldn't be used in util packages, but the
			// payoff of fixing it is not worth it right now.
			`github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode`,
			`github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror`,
		},
	)
}
