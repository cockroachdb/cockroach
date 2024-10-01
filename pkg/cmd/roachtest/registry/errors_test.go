// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package registry

import (
	"io/fs"
	"testing"

	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestErrorWithOwner(t *testing.T) {
	originalErr := fs.ErrExist
	newErr := ErrorWithOwner(OwnerTestEng, originalErr)
	require.True(t, errors.Is(newErr, fs.ErrExist))

	originalErr = rperrors.NewSSHError(errors.New("oops"))
	newErr = ErrorWithOwner(OwnerTestEng, originalErr)

	// Make sure that we are still able to detect transient errors when
	// the error is assigned an ownership later.
	var transient rperrors.TransientError
	require.True(t, errors.As(newErr, &transient))
	require.True(t, rperrors.IsTransient(newErr))
}
