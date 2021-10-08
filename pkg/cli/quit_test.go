// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestQuit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	skip.UnderShort(t)

	c := NewCLITest(TestCLIParams{T: t})
	defer c.Cleanup()

	c.Run("quit")
	// Wait until this async command cleanups the server.
	<-c.Stopper().IsStopped()
}
