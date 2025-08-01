// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func newCrudBatchHandler(
	t *testing.T, s serverutils.ApplicationLayerInterface, tableName string,
) (BatchHandler, catalog.TableDescriptor) {
	ctx := context.Background()
	desc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), tree.Name(tableName))
	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "" /* opName */)
	handler, err := newCrudSqlWriter(
		ctx,
		&execinfra.ServerConfig{
			DB:           s.InternalDB().(descs.DB),
			Codec:        s.Codec(),
			LeaseManager: s.LeaseManager(),
			Settings:     s.ClusterSettings(),
		},
		&eval.Context{
			Codec:            s.Codec(),
			Settings:         s.ClusterSettings(),
			SessionDataStack: sessiondata.NewStack(sd),
		},
		sd,
		jobspb.LogicalReplicationDetails_DiscardNothing,
		map[descpb.ID]sqlProcessorTableConfig{
			desc.GetID(): {
				srcDesc: desc,
			},
		},
		0, // jobID
	)
	require.NoError(t, err)
	return handler, desc
}

func TestBatchHandlerFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	// Create a test table with an index
	runner.Exec(t, `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			value STRING
		)
	`)

	handler, desc := newCrudBatchHandler(t, s, "test_table")
	defer handler.ReleaseLeases(ctx)
	eb := newKvEventBuilder(t, desc.TableDesc())

	// Apply a batch with inserts to populate some data
	_, err := handler.HandleBatch(ctx, []streampb.StreamEvent_KV{
		eb.insertEvent(s.Clock().Now(), []tree.Datum{
			tree.NewDInt(tree.DInt(1)),
			tree.NewDString("foo"),
		}),
		eb.insertEvent(s.Clock().Now(), []tree.Datum{
			tree.NewDInt(tree.DInt(2)),
			tree.NewDString("bar"),
		}),
	})
	require.NoError(t, err)
	// TODO(jeffswenson): fix stats
	//require.Equal(t, batchStats{inserts: 2}, stats)
	runner.CheckQueryResults(t, `SELECT id, value FROM test_table`, [][]string{
		{"1", "foo"},
		{"2", "bar"},
	})

	// Apply a batch with updates and deletes for the previously written rows
	_, err = handler.HandleBatch(ctx, []streampb.StreamEvent_KV{
		eb.updateEvent(s.Clock().Now(),
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("foo-update")},
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("foo")},
		),
		eb.deleteEvent(s.Clock().Now(), []tree.Datum{
			tree.NewDInt(tree.DInt(2)),
			tree.NewDString("bar"),
		}),
	})
	require.NoError(t, err)
	// TODO(jeffswenson): fix stats
	//require.Equal(t, batchStats{updates: 1, deletes: 1}, stats)
	runner.CheckQueryResults(t, `SELECT id, value FROM test_table`, [][]string{
		{"1", "foo-update"},
	})
}

func TestBatchHandlerSlowPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})

	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	// Create a test table with an index
	runner.Exec(t, `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			value STRING
		)
	`)

	handler, desc := newCrudBatchHandler(t, s, "test_table")
	defer handler.ReleaseLeases(ctx)
	eb := newKvEventBuilder(t, desc.TableDesc())

	runner.Exec(t, `
		INSERT INTO test_table (id, value) VALUES 
		(1, 'alpha'), (2, 'beta'), (3, 'gamma');
	`)

	_, err := handler.HandleBatch(ctx, []streampb.StreamEvent_KV{
		eb.insertEvent(s.Clock().Now(), []tree.Datum{
			tree.NewDInt(tree.DInt(1)),
			tree.NewDString("alpha-update"),
		}),
		eb.updateEvent(s.Clock().Now(),
			[]tree.Datum{tree.NewDInt(tree.DInt(2)), tree.NewDString("beta-update")},
			[]tree.Datum{tree.NewDInt(tree.DInt(2)), tree.NewDString("wrong")},
		),
		eb.deleteEvent(s.Clock().Now(), []tree.Datum{
			tree.NewDInt(tree.DInt(3)),
			tree.NewDString("wrong"),
		}),
		eb.deleteEvent(s.Clock().Now(), []tree.Datum{
			tree.NewDInt(tree.DInt(4)),
			tree.NewDString("never-existed"),
		}),
		eb.updateEvent(s.Clock().Now(),
			[]tree.Datum{tree.NewDInt(tree.DInt(5)), tree.NewDString("omega-insert")},
			[]tree.Datum{tree.NewDInt(tree.DInt(5)), tree.NewDString("never-existed")},
		),
	})
	require.NoError(t, err)
	// TODO(jeffswenson): fix stats
	//require.Equal(t,
	//	batchStats{inserts: 1, updates: 2, deletes: 1, tombstoneUpdates: 1, refreshedRows: 5},
	//	stats)

	runner.CheckQueryResults(t, `SELECT id, value FROM test_table`, [][]string{
		{"1", "alpha-update"}, // 1 was processed as an update
		{"2", "beta-update"},  // 2 was processed as an update
		// 3 is deleted, so it should not be in the result
		// 4 has an update tombstone
		{"5", "omega-insert"}, // 5 was processed as an insert
	})
}

func TestBatchHandlerDuplicateBatchEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})

	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	// Create a test table with an index
	runner.Exec(t, `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			value STRING
		)
	`)

	handler, desc := newCrudBatchHandler(t, s, "test_table")
	defer handler.ReleaseLeases(ctx)
	eb := newKvEventBuilder(t, desc.TableDesc())

	before := s.Clock().Now()
	after := s.Clock().Now()

	// TODO(jeffswenson): think about the different weird grouped batches that
	// could exist. Is it sensitive to order? What happens when an update chain
	// hits a refresh?
	_, err := handler.HandleBatch(ctx, []streampb.StreamEvent_KV{
		eb.updateEvent(before,
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("insert-followed-by-delete")},
			[]tree.Datum{tree.NewDInt(tree.DInt(1)), tree.NewDString("wrong-value")},
		),
		eb.deleteEvent(after, []tree.Datum{
			tree.NewDInt(tree.DInt(1)),
			tree.NewDString("insert-followed-by-delete"),
		}),
	})
	// The update causes a refresh and then after the refresh, the delete ends up
	// taking the update tombstone path because there is no local row, but the
	// cput fails because it observes the insert/delete.
	require.ErrorContains(t, err, "unexpected value")
}
