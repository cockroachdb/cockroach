// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	pgx "github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// Tests in this file have a linter exception against casting to *testServer.

func TestPanicRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.ApplicationLayer()

	hs := ts.HTTPServer().(*httpServer)

	// Enable a test-only endpoint that induces a panic.
	hs.mux.Handle("/panic", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("induced panic for testing")
	}))

	// Create a request.
	req, err := http.NewRequest(http.MethodGet, ts.AdminURL().WithPath("/panic").String(), nil /* body */)
	require.NoError(t, err)

	// Create a ResponseRecorder to record the response.
	rr := httptest.NewRecorder()
	require.NotPanics(t, func() {
		hs.baseHandler(rr, req)
	})

	// Check that the status code is correct.
	require.Equal(t, http.StatusInternalServerError, rr.Code)

	// Check that the panic has been reported.
	entries, err := log.FetchEntriesFromFiles(
		0, /* startTimestamp */
		math.MaxInt64,
		10000, /* maxEntries */
		regexp.MustCompile("a panic has occurred!"),
		log.WithMarkedSensitiveData)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "no log entries matching the regexp")
}

// TestSocketAutoNumbering checks that a socket name
// ending with `.0` in the input config gets auto-assigned
// the actual TCP port number.
func TestSocketAutoNumbering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	socketName := "foo.0"
	// We need a temp directory in which we'll create the unix socket.
	// On BSD, binding to a socket is limited to a path length of 104 characters
	// (including the NUL terminator). In glibc, this limit is 108 characters.
	// macOS has a tendency to produce very long temporary directory names, so
	// we are careful to keep all the constants involved short.
	baseTmpDir := os.TempDir()
	if len(baseTmpDir) >= 104-1-len(socketName)-1-4-len("TestSocketAutoNumbering")-10 {
		t.Logf("temp dir name too long: %s", baseTmpDir)
		t.Logf("using /tmp instead.")
		// Note: /tmp might fail in some systems, that's why we still prefer
		// os.TempDir() if available.
		baseTmpDir = "/tmp"
	}
	tempDir, err := os.MkdirTemp(baseTmpDir, "TestSocketAutoNumbering")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tempDir) }()

	socketFile := filepath.Join(tempDir, socketName)

	ctx := context.Background()

	params := base.TestServerArgs{
		Insecure:   true,
		SocketFile: socketFile,
	}
	s := serverutils.StartServerOnly(t, params)
	defer s.Stopper().Stop(ctx)

	_, expectedPort, err := addr.SplitHostPort(s.SQLAddr(), "")
	require.NoError(t, err)

	srv := s.SystemLayer().(*testServer)
	if socketPath := srv.Cfg.SocketFile; !strings.HasSuffix(socketPath, "."+expectedPort) {
		t.Errorf("expected unix socket ending with port %q, got %q", expectedPort, socketPath)
	}
}

// Test that connections using the internal SQL loopback listener work.
func TestInternalSQL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	ts := s.ApplicationLayer()

	conf, err := pgx.ParseConfig("")
	require.NoError(t, err)
	conf.User = "root"
	// Configure pgx to connect on the loopback listener.
	conf.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return ts.SQLLoopbackListener().(*netutil.LoopbackListener).Connect(ctx)
	}
	conn, err := pgx.ConnectConfig(ctx, conf)
	require.NoError(t, err)
	// Run a random query to check that it all works.
	r := conn.QueryRow(ctx, "SELECT count(*) FROM system.sqlliveness")
	var count int
	require.NoError(t, r.Scan(&count))
}
