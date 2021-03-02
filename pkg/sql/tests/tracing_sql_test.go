package tests

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestSetSpanVerbosityBuiltin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	r := sqlutils.MakeSQLRunner(db)

	tr := s.Tracer().(*tracing.Tracer)

	root := tr.StartSpan("root", tracing.WithForceRealSpan())
	defer root.Finish()
	require.False(t, root.IsVerbose())

	child := tr.StartSpan("root.child", tracing.WithParentAndAutoCollection(root))
	defer child.Finish()
	require.False(t, child.IsVerbose())

	childChild := tr.StartSpan("root.child.child", tracing.WithParentAndAutoCollection(child))
	defer child.Finish()
	require.False(t, childChild.IsVerbose())

	// Toggle the root span's verbosity to true and confirm its descendants are
	// verbose too.
	rootID := root.GetSpanID()
	r.Exec(
		t,
		"SELECT * FROM crdb_internal.set_verbosity_of_root_span_and_descendants($1, true)",
		rootID,
	)
	require.True(t, root.IsVerbose())
	require.True(t, child.IsVerbose())
	require.True(t, childChild.IsVerbose())

	// New child of verbose child span should also be verbose by default.
	childNewChild := tr.StartSpan("root.child.newchild", tracing.WithParentAndAutoCollection(child))
	defer childChild.Finish()
	require.True(t, childNewChild.IsVerbose())

	// Toggle root verbosity to false and confirm that descendants are toggled too.
	r.Exec(t,
		"SELECT * FROM crdb_internal.set_verbosity_of_root_span_and_descendants($1, false)",
		rootID,
	)
	require.False(t, root.IsVerbose())
	require.False(t, child.IsVerbose())
	require.False(t, childChild.IsVerbose())
	require.False(t, childNewChild.IsVerbose())
}
