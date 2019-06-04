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

package main

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestNoLinkForbidden(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We want workload to be lightweight. Ensure it doesn't depend on c-deps,
	// which are slow to compile and bloat the binary.
	buildutil.VerifyNoImports(t, "github.com/cockroachdb/cockroach/pkg/cmd/workload",
		true /* cgo */, []string{"c-deps"}, nil /* forbiddenPrefixes */)
}
