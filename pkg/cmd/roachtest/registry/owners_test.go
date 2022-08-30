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

package registry

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestOwnedError(t *testing.T) {
	ctx := context.Background()
	err := errors.DecodeError(ctx, errors.EncodeError(
		ctx, WrapWithOwner(context.Canceled, OwnerTestEng),
	))
	t.Log(err)
	require.True(t, errors.HasType(err, (*ownedError)(nil)))
	require.True(t, errors.Is(err, context.Canceled))
	{
		owner, ok := OwnerFromErr(err)
		require.True(t, ok)
		require.Equal(t, OwnerTestEng, owner)
	}

	// Last owner wins.
	err = WrapWithOwner(err, OwnerKV)
	{
		owner, ok := OwnerFromErr(err)
		require.True(t, ok)
		require.Equal(t, OwnerKV, owner)
	}
}
