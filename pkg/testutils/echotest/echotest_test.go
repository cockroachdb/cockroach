// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package echotest

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
)

func TestWalk(t *testing.T) {
	w := NewWalker(t, datapathutils.TestDataPath(t))

	// NB: a desirable property here is that in Goland you can click on the
	// subtest to navigate to the entry in the slice. This requires that `t.Run`
	// is invoked directly with the test name (this is a trial and error finding).
	for _, test := range []struct{ name string }{
		{name: "foo"},
		{name: "bar"},
	} {
		t.Run(test.name, w.Run(t, test.name, func(t *testing.T) string {
			return fmt.Sprintf("hello, %s", test.name)
		}))
	}
}
