package pgwire

import (
	"context"
	gosql "database/sql"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestListenNotify(t *testing.T) {
	defer leaktest.AfterTest(t)()
	args := base.TestServerArgs{DefaultTestTenant: base.TODOTestTenantDisabled} // TODO: fix tenantness
	s, _, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(context.Background())

	pgURL, cleanup := sqlutils.PGUrl(t, s.AdvSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}

	listener := pq.NewListener("listener", 1*time.Second, 1, func(event pq.ListenerEventType, err error) {
		if err != nil {
			t.Fatal(err)
		}
	})

	require.NoError(t, listener.Listen("A"))

	_, err = db.Exec("NOTIFY A, 'P'")
	require.NoError(t, err)

	select {
	case n := <-listener.Notify:
		require.Equal(t, "A", n.Channel)
		require.Equal(t, "P", n.Extra)
	case <-time.After(5 * time.Second):
		t.Fatal("notification timeout")
	}
}

func TestListenNotifyLoad(t *testing.T) {
	t.Skip()
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	pgURL, cleanup := sqlutils.PGUrl(t, s.AdvSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()

	db, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)

	db2, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)

	listener := pq.NewListener("listener", 1*time.Second, 1, func(event pq.ListenerEventType, err error) {
		if err != nil {
			t.Fatal(err)
		}
	})

	require.NoError(t, listener.Listen("A"))

	// generate a large set of data to stream back
	ctx, cancel := context.WithCancel(context.Background())
	rows, err := db.QueryContext(ctx, "SELECT generate_series(1, 10000000)")
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	// stream rows in the background
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer rows.Close()
		var i int
		for rows.Next() {
			if err := rows.Scan(&i); err != nil {
				if ctx.Err() == nil {
					require.NoError(t, err)
				}
			}
		}
	}()

	// send lots of notifications too on the other conn
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			_, err := db2.ExecContext(ctx, "NOTIFY A 'P'")
			if err != nil {
				if ctx.Err() == nil {
					require.NoError(t, err)
				}
			}
		}
	}()

	notifications := 0
	for ctx.Err() == nil && notifications < 1_000_000 {
		select {
		case n := <-listener.Notify:
			require.Equal(t, "A", n.Channel)
			require.Equal(t, "P", n.Extra)
			notifications++
		case <-ctx.Done():
			t.Fatal("notification timeout")
		}
	}
	cancel()
	wg.Wait()
}
