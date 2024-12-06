// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package profiler

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func addMonitor(
	t *testing.T,
	ctx context.Context,
	st *cluster.Settings,
	name redact.SafeString,
	parent *mon.BytesMonitor,
	usedBytes int64,
	reservedBytes int64,
) *mon.BytesMonitor {
	m := mon.NewMonitor(mon.Options{
		Name:      mon.MakeMonitorName(name),
		Increment: 1,
		Settings:  st,
	})
	m.Start(ctx, parent, mon.NewStandaloneBudget(reservedBytes))
	if usedBytes != 0 {
		acc := m.MakeBoundAccount()
		if err := acc.Grow(ctx, usedBytes); err != nil {
			t.Fatal(err)
		}
	}
	return m
}

type testWriter struct {
	buf []byte
}

var _ io.Writer = &testWriter{}

func (w *testWriter) Write(p []byte) (n int, err error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func TestGetMonitorStateCb(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	root := addMonitor(t, ctx, st, "root", nil /* parent */, 0 /* usedBytes */, 1<<30 /* reservedBytes */)
	// Monitor with usage that counts towards its parent.
	child1 := addMonitor(t, ctx, st, "child1", root, 1<<20 /* usedBytes */, 0 /* reservedBytes */)
	// Monitor with usage that counts towards its reserved account.
	addMonitor(t, ctx, st, "child2", root, 1<<10 /* usedBytes */, 2<<10 /* reservedBytes */)
	// Monitor with no usage and no reservation should be omitted.
	addMonitor(t, ctx, st, "child3", root, 0 /* usedBytes */, 0 /* reservedBytes */)
	// Monitor at a deeper level.
	addMonitor(t, ctx, st, "grandchild1", child1, 1 /* usedBytes */, 0 /* reservedBytes */)

	var w testWriter
	require.NoError(t, root.TraverseTree(getMonitorStateCb(&w)))

	expected := `
root 1.0 MiB (1.0 GiB / 0 B)
    child2 1.0 KiB (2.0 KiB / 0 B)
    child1 1.0 MiB
        grandchild1 1 B
`
	// Note that we prepend a newline symbol for prettier expected string.
	require.Equal(t, expected, fmt.Sprintf("\n%s", w.buf))
}
