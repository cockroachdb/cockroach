package sql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotify(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	pgURL, cleanup := sqlutils.PGUrl(t, s.AdvSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()

	listenConn, err := pgx.Connect(ctx, pgURL.String())
	require.NoError(t, err)
	defer listenConn.Close(ctx)

	notifyConn, err := pgx.Connect(ctx, pgURL.String())
	require.NoError(t, err)
	defer notifyConn.Close(ctx)

	notifyPid := notifyConn.PgConn().PID()
	// paranoia check
	p2r := notifyConn.QueryRow(ctx, "SELECT pg_backend_pid()")
	var pid2 int
	require.NoError(t, p2r.Scan(&pid2))
	require.Equal(t, notifyPid, uint32(pid2))

	_, err = listenConn.Exec(ctx, "LISTEN A")
	require.NoError(t, err)

	const n = 1000

	done := make(chan struct{})
	go func() {
		defer close(done)
		for next := 0; next < n; next++ {
			notif, err := listenConn.WaitForNotification(ctx)
			require.NoError(t, err)

			num, err := strconv.Atoi(notif.Payload)
			require.NoError(t, err)

			assert.Equal(t, "a", notif.Channel)
			assert.Equal(t, next, num)
			assert.Equal(t, notifyPid, notif.PID)
		}
	}()

	for i := 0; i < n; i++ {
		_, err = notifyConn.Exec(ctx, fmt.Sprintf("NOTIFY A, '%d'", i))
		require.NoError(t, err)
	}
	<-done
}

func TestNotifyNoPayload(t *testing.T) {
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

	notifyNoPayload := func() {
		_, err = db.Exec("NOTIFY A")
		require.NoError(t, err)
	}
	notifyPayload := func() {
		_, err = db.Exec("NOTIFY A, 'payload'")
		require.NoError(t, err)
	}
	getNotification := func() string {
		ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		notif, err := conn.WaitForNotification(ctx)
		require.NoError(t, err)
		return notif.Payload
	}

	// Test a sequence of notifications with and without payloads to try and elicit errors.

	notifyNoPayload()
	require.Empty(t, getNotification())

	notifyNoPayload()
	require.Empty(t, getNotification())

	notifyPayload()
	require.Equal(t, "payload", getNotification())

	notifyPayload()
	require.Equal(t, "payload", getNotification())

	notifyNoPayload()
	require.Empty(t, getNotification())
}

func TestNotifyUnlisten(t *testing.T) {
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

	_, err = db.Exec("NOTIFY A, 'hi'")
	require.NoError(t, err)

	_, err = conn.WaitForNotification(ctx)
	require.NoError(t, err)

	_, err = conn.Exec(ctx, "UNLISTEN A")
	require.NoError(t, err)

	_, err = db.Exec("NOTIFY A, 'hi'")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	_, err = conn.WaitForNotification(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func BenchmarkNotify(b *testing.B) {
	defer leaktest.AfterTest(b)()
	ctx := context.Background()

	s, _, _ := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	pgURL, cleanup := sqlutils.PGUrl(b, s.AdvSQLAddr(), b.Name(), url.User(username.RootUser))
	defer cleanup()

	db, err := gosql.Open("postgres", pgURL.String())
	require.NoError(b, err)
	defer db.Close()

	conn, err := pgx.Connect(ctx, pgURL.String())
	require.NoError(b, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "LISTEN A")
	require.NoError(b, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for count := 0; count < b.N; count++ {
			_, err := conn.WaitForNotification(ctx)
			require.NoError(b, err)
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = db.Exec(fmt.Sprintf("NOTIFY A, '%d'", i))
		require.NoError(b, err)
	}
	<-done
}
