package queuefeed

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestFeedCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := srv.ApplicationLayer().InternalDB().(isql.DB)
	// expect an error when trying to read from a queue that doesn't exist
	qm := NewManager(db)
	_, err := qm.GetOrInitReader(context.Background(), "test")
	require.ErrorContains(t, err, "queue feed not found")

	// expect no error when creating a queue
	require.NoError(t, qm.CreateQueue(context.Background(), "test", 104))

	// now we can read from the queue
	reader, err := qm.GetOrInitReader(context.Background(), "test")
	require.NoError(t, err)
	require.NotNil(t, reader)
	reader.cancel()
}
