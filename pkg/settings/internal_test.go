// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package settings

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

var b = RegisterBoolSetting(SystemOnly, "b", "desc", true)

func TestIgnoreDefaults(t *testing.T) {
	ctx := context.Background()
	sv := &Values{}
	sv.Init(ctx, TestOpaque)

	ignoreAllUpdates = true
	defer func() { ignoreAllUpdates = false }()
	u := NewUpdater(sv)
	require.NoError(t, u.Set(ctx, b.InternalKey(), EncodedValue{Value: EncodeBool(false), Type: "b"}))
	require.Equal(t, true, b.Get(sv))

	ignoreAllUpdates = false
	u = NewUpdater(sv)
	require.NoError(t, u.Set(ctx, b.InternalKey(), EncodedValue{Value: EncodeBool(false), Type: "b"}))
	require.Equal(t, false, b.Get(sv))
}
