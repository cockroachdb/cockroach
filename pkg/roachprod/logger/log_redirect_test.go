// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logger

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestLogRedirect(t *testing.T) {
	l := NewMockLogger(t)
	TestingCRDBLogConfig(l.Logger)
	ctx := context.Background()

	log.Infof(ctx, "[simple test]")
	RequireLine(t, l, "[simple test]")
	require.Equal(t, 1, len(l.writer.lines))
}
