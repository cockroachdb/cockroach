package migrations_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration/migrations"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestFixCastForStyle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.DateStyleIntervalStyleEnabled - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)
	sqlDB := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	var semaCtx tree.SemaContext

	tdb.Exec(t, `CREATE TABLE ds (
it interval,
s string,
t timestamp,
c string  AS ((it + interval '2 minutes')::string) STORED,
c2 interval AS ((s)::interval) STORED,
c3 string AS (t::string) STORED
)`)
	tdb.Exec(t, `CREATE INDEX rw ON ds ((it::text))`)
	tdb.Exec(t, `CREATE INDEX partial ON ds(it) WHERE (it::text) > 'abc';`)

	tdb.Exec(t, `CREATE TABLE ds2 (
	ch char,
	d date,
	c string AS (d::string) STORED,
	c1 string AS (lower(d::STRING)) STORED,
	c2 interval AS (ch::interval) STORED
)`)

	migrations.Migrate(
		t,
		sqlDB,
		clusterversion.DateStyleIntervalStyleEnabled,
		nil,
		false,
	)
	err := sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
		var visitor sql.FixCastForStyleVisitor
		var targetDescs []catalog.Descriptor

		cat, err := col.GetCatalogUnvalidated(ctx, txn)
		if err != nil {
			return err
		}

		for _, desc := range cat.OrderedDescriptors() {
			if desc.GetName() == "ds" || desc.GetName() == "ds2" {
				targetDescs = append(targetDescs, desc)
			}
		}

		for _, desc := range targetDescs {
			tDesc, err := catalog.AsTableDescriptor(desc)
			if err != nil {
				return err
			}

			table := tDesc.TableDesc()
			cols := table.Columns
			idxs := table.Indexes
			for _, col := range cols {
				if col.IsComputed() {
					expr, err := parser.ParseExpr(*col.ComputeExpr)
					if err != nil {
						return err
					}
					visitor = sql.MakeFixCastForStyleVisitor(ctx, &semaCtx, table, col.Type, err)
					_, changed := tree.WalkExpr(&visitor, expr)
					if changed {
						return errors.Newf(
							"The computed columns in table %s were not successfully rewritten",
							table.Name,
						)
					}
				}
			}
			for _, idx := range idxs {
				if idx.IsPartial() {
					var typedExpr tree.TypedExpr

					expr, err := parser.ParseExpr(idx.Predicate)
					replacedExpr, err := schemaexpr.MakeDummyColForTypeCheck(ctx, tDesc, expr, tree.NewUnqualifiedTableName(tree.Name(tDesc.GetName())))

					if cExpr, ok := replacedExpr.(*tree.ComparisonExpr); ok {
						typedExpr, err = cExpr.Left.TypeCheck(ctx, &semaCtx, types.Any)
					} else {
						typedExpr, err = replacedExpr.TypeCheck(ctx, &semaCtx, types.Any)
					}
					if err == nil {
						typ := typedExpr.ResolvedType()
						visitor = sql.MakeFixCastForStyleVisitor(ctx, &semaCtx, table, typ, err)
						_, changed := tree.WalkExpr(&visitor, expr)
						if changed {
							return errors.Newf(
								"The partial indexes in table %s were not successfully rewritten",
								table.Name,
							)
						}
					}
				}
			}
		}
		return nil
	})
	require.NoError(t, err)
}
