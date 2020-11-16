package schemachanger

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

var BuilderMaker = func(s serverutils.TestServerInterface) (*Builder, func()) {
	return nil, nil
}

func TestBuilder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	// Make ourselves a mock resolver.
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)

	tdb.Exec(t, "CREATE DATABASE db")
	tdb.Exec(t, "CREATE TABLE db.public.foo (i INT PRIMARY KEY)")

	var tableID descpb.ID
	tdb.QueryRow(t, "SELECT 'db.public.foo'::regclass::int").Scan(&tableID)

	builder, cleanup := BuilderMaker(s)
	defer cleanup()
	require.NotNil(t, builder)

	stmts, err := parser.Parse("ALTER TABLE db.public.foo ADD COLUMN j INT NOT NULL DEFAULT 1")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	alter := stmts[0].AST.(*tree.AlterTable)

	require.NoError(t, builder.AlterTable(ctx, alter))
	sc, err := builder.Build()
	require.NoError(t, err)
	require.Equal(t, []element{
		&addColumn{
			statementID: 0,
			tableID:     tableID,
			columnID:    2,
			state:       elemDeleteOnly,
		},
	}, sc.state.elements)
}
