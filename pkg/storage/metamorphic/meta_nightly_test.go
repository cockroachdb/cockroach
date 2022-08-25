// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build nightly
// +build nightly

package metamorphic

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestPebbleEquivalenceNightly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	if *opCount < 1000000 {
		oldOpCount := *opCount
		// Override number of operations to at least 1 million.
		*opCount = 1000000

		defer func() {
			*opCount = oldOpCount
		}()
	}

	runPebbleEquivalenceTest(t)
}
