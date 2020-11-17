package schemachanger

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

var BuilderMaker = func(s serverutils.TestServerInterface) (*Builder, interface{}, func()) {
	return nil, nil, nil
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

	builder, _, cleanup := BuilderMaker(s)
	defer cleanup()
	require.NotNil(t, builder)

	stmts, err := parser.Parse("ALTER TABLE db.public.foo ADD COLUMN j INT NOT NULL DEFAULT 1")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	alter := stmts[0].AST.(*tree.AlterTable)

	require.NoError(t, builder.AlterTable(ctx, alter))
	sc, err := builder.Build()
	require.NoError(t, err)
	err = descs.Txn(ctx, s.ClusterSettings(), s.LeaseManager().(*lease.Manager), s.InternalExecutor().(sqlutil.InternalExecutor), s.DB(),
		func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			for _, d := range builder.modifiedDescriptors {
				if err := descriptors.WriteDesc(ctx, false /* kvTrace */, d, txn); err != nil {
					return err
				}
			}
			return nil
		})
	require.NoError(t, err)
	require.Equal(t, []element{
		&addColumn{
			statementID: 0,
			tableID:     tableID,
			columnID:    2,
			state:       elemDeleteOnly,
		},
	}, sc.state.elements)

	err = descs.Txn(ctx, s.ClusterSettings(), s.LeaseManager().(*lease.Manager), s.InternalExecutor().(sqlutil.InternalExecutor), s.DB(),
		func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			return sc.Run(ctx, runDependenciesTesting{
				descriptors: descriptors,
				txn:         txn,
			})
		})
	require.NoError(t, err)

	time.Sleep(time.Second)
	tdb.Exec(t, `INSERT INTO db.public.foo VALUES (1, 2)`)
}

type runDependenciesTesting struct {
	descriptors *descs.Collection
	txn         *kv.Txn
}

func (r runDependenciesTesting) GetDesc(
	ctx context.Context, id descpb.ID,
) (catalog.Descriptor, error) {
	return r.descriptors.GetMutableDescriptorByID(ctx, id, r.txn)

}

func (r runDependenciesTesting) GetDescs(
	ctx context.Context, reqs []descpb.ID,
) ([]catalog.Descriptor, error) {
	ret := make([]catalog.Descriptor, len(reqs))
	for i, id := range reqs {
		ret[i], _ = r.GetDesc(ctx, id)
	}
	return ret, nil
}

func (r runDependenciesTesting) WriteDesc(
	ctx context.Context, desc catalog.MutableDescriptor,
) error {
	return r.descriptors.WriteDesc(ctx, false /* kvTrace */, desc, r.txn)
}

func (r runDependenciesTesting) withDescriptorMutationDeps(
	ctx context.Context, f func(ctx2 context.Context, deps depsForDescriptorMutation) error,
) error {
	return f(ctx, r)
}

var _ runDependencies = (*runDependenciesTesting)(nil)
var _ depsForDescriptorMutation = (*runDependenciesTesting)(nil)
