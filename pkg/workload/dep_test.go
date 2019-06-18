// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
			`github.com/cockroachdb/cockroach/pkg/sql/exec/coldata`,
			`github.com/cockroachdb/cockroach/pkg/sql/exec/types`,
			`github.com/cockroachdb/cockroach/pkg/util/bufalloc`,
			`github.com/cockroachdb/cockroach/pkg/util/encoding/csv`,
			`github.com/cockroachdb/cockroach/pkg/util/syncutil`,
			`github.com/cockroachdb/cockroach/pkg/util/timeutil`,
			`github.com/cockroachdb/cockroach/pkg/workload/histogram`,
		},
	)
}
