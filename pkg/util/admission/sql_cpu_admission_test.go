// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContextWithSQLCPUAdmissionHandle(t *testing.T) {
	var h SQLCPUAdmissionHandle
	ctx := ContextWithSQLCPUAdmissionHandle(context.Background(), &h)
	h2 := SQLCPUAdmissionHandleFromContext(ctx)
	require.Equal(t, &h, h2)
}
