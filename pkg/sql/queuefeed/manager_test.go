package queuefeed

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func NewTestManager(t *testing.T, a serverutils.ApplicationLayerInterface) *Manager {
	ctx := context.Background()
	db := a.InternalDB().(isql.DB)
	m := NewManager(ctx, db, a.RangeFeedFactory().(*rangefeed.Factory), a.Codec(), a.LeaseManager().(*lease.Manager))
	require.NotNil(t, m.codec)
	return m
}

func TestFeedCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	// expect an error when trying to read from a queue that doesn't exist
	qm := NewTestManager(t, srv.ApplicationLayer())
	_, err := qm.GetOrInitReader(context.Background(), "test")
	require.ErrorContains(t, err, "queue feed not found")

	// expect no error when creating a queue
	db := sqlutils.MakeSQLRunner(conn)
	db.Exec(t, `CREATE TABLE t (a string)`)
	// get table id
	var tableID int64
	db.QueryRow(t, "SELECT id FROM system.namespace where name = 't'").Scan(&tableID)
	require.NoError(t, qm.CreateQueue(context.Background(), "test", tableID))

	// now we can read from the queue
	reader, err := qm.GetOrInitReader(context.Background(), "test")
	require.NoError(t, err)
	require.NotNil(t, reader)
	reader.(*Reader).cancel(errors.New("test shutdown"))
}

func TestQueuefeedCtxCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(conn)
	db.Exec(t, `CREATE TABLE t (a string)`)
	// get table id
	var tableID int64
	db.QueryRow(t, "SELECT id FROM system.namespace where name = 't'").Scan(&tableID)
	db.Exec(t, `SELECT crdb_internal.create_queue_feed('hi', $1)`, tableID)

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	_, err := db.DB.QueryContext(ctx, `SELECT crdb_internal.select_from_queue_feed('hi', 1)`)
	require.Error(t, err)
}
