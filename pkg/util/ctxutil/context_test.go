// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ctxutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWhenDone(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	done := make(chan struct{})
	require.NoError(t, WhenDone(parent, func(err error) { close(done) }))
	cancelParent()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("timeout")
	}
}
