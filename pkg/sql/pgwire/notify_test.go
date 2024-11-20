package pgwire

import (
	"context"
	gosql "database/sql"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestListenNotify(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	pgURL, cleanup := sqlutils.PGUrl(t, s.AdvSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()

	db, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer db.Close()

	conn, err := pgx.Connect(ctx, pgURL.String())
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "LISTEN A")
	require.NoError(t, err)

	_, err = db.Exec("NOTIFY A, 'P'")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	notif, err := conn.WaitForNotification(ctx)
	require.NoError(t, err)
	assert.Equal(t, "a", notif.Channel)
	assert.Equal(t, "P", notif.Payload)
	assert.NotZero(t, notif.PID)
}

func TestListenNotifyLoad(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	pgURL, cleanup := sqlutils.PGUrl(t, s.AdvSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()

	conn, err := pgx.Connect(ctx, pgURL.String())
	require.NoError(t, err)
	defer conn.Close(ctx)

	conn2, err := pgx.Connect(ctx, pgURL.String())
	require.NoError(t, err)
	defer conn2.Close(ctx)

	_, err = conn.Exec(ctx, "LISTEN A")
	require.NoError(t, err)

	// generate a large set of data to stream back
	ctx, cancel := context.WithCancel(ctx)
	rows, err := conn.Query(ctx, "SELECT generate_series(1, 1000000)")
	require.NoError(t, err)
	defer rows.Close()

	// send lots of notifications too on the other conn
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			_, err := conn2.Exec(ctx, "NOTIFY A, 'P'")
			if err != nil {
				if ctx.Err() == nil {
					require.NoError(t, err)
				}
			}
		}
	}()

	// stream rows, while there are notifications happening
	for rows.Next() {
		var i int
		if err := rows.Scan(&i); err != nil {
			if ctx.Err() == nil {
				require.NoError(t, err)
			}
			break
		}
		assert.Positive(t, i)
	}

	cancel()
	wg.Wait()
}
