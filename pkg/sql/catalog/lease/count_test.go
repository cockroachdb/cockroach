// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestHandleRegionLivenessErrorsSkipsMissingRegion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	skip, err := handleRegionLivenessErrors(ctx, nil /* prober */, "us-east2", types.EnumValueNotYetPublicError)
	require.NoError(t, err)
	require.True(t, skip)
}
