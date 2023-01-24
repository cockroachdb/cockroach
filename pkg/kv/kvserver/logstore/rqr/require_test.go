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
	"testing"

	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func capture(f func()) redact.RedactableString {
	old := fatalfDepth
	defer func() {
		fatalfDepth = old
	}()
	var s redact.RedactableString
	fatalfDepth = func(ctx context.Context, depth int, format string, args ...interface{}) {
		s = redact.Sprintf(format, args...)
	}
	f()
	return s
}

func TestEqual(t *testing.T) {
	ctx := logtags.AddTag(context.Background(), "hello", "world")
	require.Empty(t, capture(func() {
		Equal(ctx, 12, 12)
	}))
	require.Contains(t, capture(func() {
		Equal(ctx, 12, 13)
	}), "Not equal")
}
