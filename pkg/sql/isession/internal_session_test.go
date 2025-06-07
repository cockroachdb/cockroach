package isession

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestInternalSessionPrepare(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	server := s.SQLServer().(*sql.Server)
	metrics := sql.MemoryMetrics{}
	config := s.ExecutorConfig().(sql.ExecutorConfig)

	session, err := NewInternalSession(ctx, "test-session", server, metrics, &config)
	require.NoError(t, err)
	defer session.Close(ctx)

	stmt, err := parser.ParseOne("SELECT 1")
	require.NoError(t, err)

	prepared, err := session.Prepare(ctx, "test-stmt", stmt, nil)
	require.NoError(t, err)
	require.NotNil(t, prepared)

	for i := 0; i < 10; i++ {
		rows, err := session.Execute(ctx, prepared, nil)
		require.NoError(t, err)
		require.Equal(t, 1, rows)
	}
}

func TestInternalSessionInsertAndVerify(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a test table using a regular SQL client
	db := s.SQLConn(t)
	_, err := db.Exec("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	require.NoError(t, err)

	// Create an internal session
	server := s.SQLServer().(*sql.Server)
	metrics := sql.MemoryMetrics{}
	config := s.ExecutorConfig().(sql.ExecutorConfig)

	session, err := NewInternalSession(ctx, "test-session", server, metrics, &config)
	require.NoError(t, err)
	defer session.Close(ctx)

	// Prepare an insert statement
	stmt, err := parser.ParseOne("INSERT INTO test VALUES ($1, $2)")
	require.NoError(t, err)

	prepared, err := session.Prepare(ctx, "insert-stmt", stmt, []*types.T{types.Int, types.Int})
	require.NoError(t, err)
	require.NotNil(t, prepared)

	// Execute the insert multiple times
	for i := 1; i <= 3; i++ {
		rows, err := session.Execute(ctx, prepared, tree.Datums{
			tree.NewDInt(tree.DInt(i)),
			tree.NewDInt(tree.DInt(i * 10)),
		})
		require.NoError(t, err)
		require.Equal(t, 1, rows)
	}

	// Verify the data using a regular SQL client
	results := sqlutils.MakeSQLRunner(db).QueryStr(t, "SELECT id, val FROM test ORDER BY id")
	require.Equal(t, [][]string{
		{"1", "10"},
		{"2", "20"},
		{"3", "30"},
	}, results)
}
