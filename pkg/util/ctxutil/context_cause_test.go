// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build go1.20

package ctxutil

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestWhenDoneCause(t *testing.T) {
	parent, cancelParent := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	require.NoError(t, WhenDoneCause(parent, func(err error, cause error) { done <- cause }))

	expectErr := errors.New("blarg")
	cancelParent(expectErr)
	select {
	case err := <-done:
		require.True(t, errors.Is(err, expectErr))
	case <-time.After(30 * time.Second):
		t.Fatal("timeout")
	}
}
