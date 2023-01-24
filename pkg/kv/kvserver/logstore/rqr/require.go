// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rqr

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// NB: auto-gen the signatures from stretchr/testify/require.go.
// NB: if !buildutils.IsCrdbTest, use no-op impls for all of these.
// NB: also generate EqualInt, etc, to avoid heap allocs. Maybe it's
// fine if there are heap allocs as long as they don't occur without crdb_test.

var fatalerPool = sync.Pool{
	New: func() interface{} { return &testingT{} },
}

var fatalfDepth = log.FatalfDepth

type testingT struct {
	ctx context.Context
	buf redact.StringBuilder
}

func (t *testingT) Errorf(format string, args ...interface{}) {
	t.buf.Printf(format, args...)
}

func (t *testingT) FailNow() {
	// Depth 3 because need to walk up three frames: FailNow <-- require.X <-- X <-- caller.
	fatalfDepth(t.ctx, 3, "%s", t.buf)
}

// Equal ...
func Equal(ctx context.Context, expected, actual interface{}, msgAndArgs ...interface{}) {
	t := fatalerPool.Get().(*testingT)
	*t = testingT{ctx: ctx}
	require.Equal(t, expected, actual, msgAndArgs...)
	*t = testingT{}
	fatalerPool.Put(t)
}
