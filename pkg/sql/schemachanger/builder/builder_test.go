package builder_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/builder"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestBuilder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	// Make ourselves a mock resolver.
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// TODO (lucy): Move this into the test cases.
	tdb.Exec(t, "CREATE DATABASE db")
	tdb.Exec(t, "CREATE TABLE db.public.foo (i INT PRIMARY KEY)")

	var tableID descpb.ID
	tdb.QueryRow(t, "SELECT 'db.public.foo'::regclass::int").Scan(&tableID)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	ip, cleanup := sql.NewInternalPlanner(
		"foo",
		kv.NewTxn(context.Background(), s.DB(), s.NodeID()),
		security.RootUserName(),
		&sql.MemoryMetrics{},
		&execCfg,
		sessiondatapb.SessionData{},
	)
	defer cleanup()
	planner := ip.(interface {
		resolver.SchemaResolver
		SemaCtx() *tree.SemaContext
		EvalContext() *tree.EvalContext
	})
	b := builder.NewBuilder(
		planner, planner.SemaCtx(), planner.EvalContext(),
	)

	for _, tc := range []struct {
		name     string
		stmt     string
		expected []targets.TargetState
	}{
		{
			"add column",
			"ALTER TABLE db.public.foo ADD COLUMN j INT",
			[]targets.TargetState{
				{
					&targets.AddColumn{
						TableID: tableID,
						Column: descpb.ColumnDescriptor{
							ID:   2,
							Name: "j",
						},
					},
					targets.State_ABSENT,
				},
				{
					&targets.AddPrimaryIndex{
						TableID: tableID,
						Index: descpb.IndexDescriptor{
							ID:   2,
							Name: "new_primary_key",
						},
						PrimaryIndex:     1,
						ReplacementFor:   1,
						StoreColumnIDs:   []descpb.ColumnID{2},
						StoreColumnNames: []string{"j"},
					},
					targets.State_ABSENT,
				},
				{
					&targets.DropPrimaryIndex{
						TableID: tableID,
						Index: descpb.IndexDescriptor{
							ID:   1,
							Name: "primary",
						},
						ReplacedBy:       2,
						StoreColumnIDs:   nil,
						StoreColumnNames: nil,
					},
					targets.State_PUBLIC,
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := parser.Parse(tc.stmt)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			alter := stmts[0].AST.(*tree.AlterTable)

			ts, err := b.AlterTable(ctx, nil, alter)
			require.NoError(t, err)
			for i := range ts {
				t.Logf("TargetStates[%d]: %+v %+v", i, ts[i].Target, ts[i].State)
			}

			require.Len(t, ts, len(tc.expected))
			for i := range ts {
				exp := tc.expected[i]
				actual := ts[i]
				require.IsType(t, exp.Target, actual.Target)
				require.Equal(t, exp.State, actual.State)
				switch target := actual.Target.(type) {
				case *targets.AddColumn:
					e := exp.Target.(*targets.AddColumn)
					require.Equal(t, e.TableID, target.TableID)
					require.Equal(t, e.Column.ID, target.Column.ID)
					require.Equal(t, e.Column.Name, target.Column.Name)
				case *targets.AddPrimaryIndex:
					e := exp.Target.(*targets.AddPrimaryIndex)
					require.Equal(t, e.TableID, target.TableID)
					require.Equal(t, e.Index.ID, target.Index.ID)
					require.Equal(t, e.Index.Name, target.Index.Name)
					require.Equal(t, e.PrimaryIndex, target.PrimaryIndex)
					require.Equal(t, e.ReplacementFor, target.ReplacementFor)
					require.Equal(t, e.StoreColumnIDs, target.StoreColumnIDs)
					require.Equal(t, e.StoreColumnNames, target.StoreColumnNames)
				case *targets.DropPrimaryIndex:
					e := exp.Target.(*targets.DropPrimaryIndex)
					require.Equal(t, e.TableID, target.TableID)
					require.Equal(t, e.Index.ID, target.Index.ID)
					require.Equal(t, e.Index.Name, target.Index.Name)
					require.Equal(t, e.ReplacedBy, target.ReplacedBy)
					require.Equal(t, e.StoreColumnIDs, target.StoreColumnIDs)
					require.Equal(t, e.StoreColumnNames, target.StoreColumnNames)
				default:
					t.Fatalf("unsupported type for now")
				}
			}
		})
	}
}
