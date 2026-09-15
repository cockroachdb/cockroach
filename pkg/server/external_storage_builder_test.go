// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestExternalStorageBuilderNotReady checks that building external storage
// before init is a retryable condition rather than an assertion failure: these
// factories are reachable over RPC during startup, and CDC treats assertion
// failures as terminal.
func TestExternalStorageBuilderNotReady(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var b externalStorageBuilder

	_, err := b.makeExternalStorage(ctx, cloudpb.ExternalStorage{})
	require.ErrorIs(t, err, cloud.ErrNotReady)
	require.False(t, errors.HasAssertionFailure(err))

	_, err = b.makeExternalStorageFromURI(ctx, "nodelocal://1/foo", username.RootUserName())
	require.ErrorIs(t, err, cloud.ErrNotReady)
	require.False(t, errors.HasAssertionFailure(err))

	// The startup-path check is a genuine invariant.
	require.True(t, errors.HasAssertionFailure(b.assertInitComplete()))
}
