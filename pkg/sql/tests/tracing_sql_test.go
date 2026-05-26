// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestSetTraceSpansVerbosityBuiltin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	si, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer si.Stopper().Stop(context.Background())
	r := sqlutils.MakeSQLRunner(db)
	tr := si.ApplicationLayer().Tracer()

	// Try to toggle the verbosity of a trace that doesn't exist, returns false.
	// NB: Technically this could return true in the unlikely scenario that there
	// is a trace with ID of 0.
	r.CheckQueryResults(
		t,
		"SELECT * FROM crdb_internal.set_trace_verbose(0, true)",
		[][]string{{`false`}},
	)

	root := tr.StartSpan("root", tracing.WithForceRealSpan())
	defer root.Finish()
	require.False(t, root.IsVerbose())

	child := tr.StartSpan("root.child", tracing.WithParent(root))
	defer child.Finish()
	require.False(t, child.IsVerbose())

	childChild := tr.StartSpan("root.child.child", tracing.WithParent(child))
	defer childChild.Finish()
	require.False(t, childChild.IsVerbose())

	// Toggle the trace's verbosity and confirm all spans are verbose.
	traceID := root.TraceID()
	query := fmt.Sprintf(
		"SELECT * FROM crdb_internal.set_trace_verbose(%d, true)",
		traceID,
	)
	r.CheckQueryResults(
		t,
		query,
		[][]string{{`true`}},
	)

	require.True(t, root.IsVerbose())
	require.True(t, child.IsVerbose())
	require.True(t, childChild.IsVerbose())

	// New child of verbose child span should also be verbose by default.
	newChild := tr.StartSpan("root.child.newchild", tracing.WithParent(root))
	defer newChild.Finish()
	require.True(t, newChild.IsVerbose())

	// Toggle the trace's verbosity and confirm none of the spans are verbose.
	query = fmt.Sprintf(
		"SELECT * FROM crdb_internal.set_trace_verbose(%d, false)",
		traceID,
	)
	r.CheckQueryResults(
		t,
		query,
		[][]string{{`true`}},
	)

	require.False(t, root.IsVerbose())
	require.False(t, child.IsVerbose())
	require.False(t, childChild.IsVerbose())
	require.False(t, newChild.IsVerbose())
}
