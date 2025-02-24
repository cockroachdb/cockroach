// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/stretchr/testify/require"
)

func runClusterInit(ctx context.Context, t test.Test, c cluster.Cluster) {
	// We start all nodes with the same join flags and then issue an "init"
	// command to one of the nodes. We do this twice, since roachtest has some
	// special casing for the first node in a cluster (the join flags of all nodes
	// default to just the first node) and we want to make sure that we're not
	// relying on it.
	startOpts := option.DefaultStartOpts()

	// We don't want roachprod to auto-init this cluster.
	startOpts.RoachprodOpts.SkipInit = true

	// We need to point all nodes at all other nodes. By default,
	// roachprod will point all nodes at the first node, but this
	// won't allow init'ing any but the first node - we require
	// that all nodes can discover the init'ed node (transitively)
	// via the join targets.
	startOpts.RoachprodOpts.JoinTargets = c.All()

	// Start the cluster in insecure mode to allow it to test both
	// authenticated and unauthenticated code paths.
	settings := install.MakeClusterSettings(install.SecureOption(false))

	for _, initNode := range []int{2, 1} {
		c.Wipe(ctx)
		t.L().Printf("starting test with init node %d", initNode)
		c.Start(ctx, t.L(), startOpts, settings)

		urlMap := make(map[int]string)
		adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.All())
		if err != nil {
			t.Fatal(err)
		}
		for i, addr := range adminUIAddrs {
			urlMap[i+1] = `http://` + addr
		}

		t.L().Printf("waiting for the servers to bind their ports")
		if err := retry.ForDuration(10*time.Second, func() error {
			for i := 1; i <= c.Spec().NodeCount; i++ {
				resp, err := httputil.Get(ctx, urlMap[i]+"/health")
				if err != nil {
					return err
				}
				resp.Body.Close()
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		t.L().Printf("all nodes started, establishing SQL connections")

		var dbs []*gosql.DB
		for i := 1; i <= c.Spec().NodeCount; i++ {
			db := c.Conn(ctx, t.L(), i)
			//nolint:deferloop TODO(#137605)
			defer db.Close()
			dbs = append(dbs, db)
		}

		// Initially, we can connect to any node, but queries issued will hang.
		t.L().Printf("checking that the SQL conns are not failing immediately")
		errCh := make(chan error, len(dbs))
		for _, db := range dbs {
			t.Go(func(taskCtx context.Context, _ *logger.Logger) error {
				var val int
				errCh <- db.QueryRowContext(taskCtx, "SELECT 1").Scan(&val)
				return nil
			})
		}

		// Give them time to get a "connection refused" or similar error if
		// the server isn't listening.
		time.Sleep(time.Second)
		select {
		case err := <-errCh:
			t.Fatalf("query finished prematurely with err %v", err)
		default:
		}

		// Check that the /health endpoint is functional even before cluster init,
		// whereas other debug endpoints return an appropriate error.
		httpTests := []struct {
			endpoint       string
			expectedStatus int
		}{
			{"/health", http.StatusOK},
			{"/health?ready=1", http.StatusServiceUnavailable},
			{"/_status/nodes", http.StatusNotFound},
		}
		for _, tc := range httpTests {
			for _, withCookie := range []bool{false, true} {
				t.L().Printf("checking for HTTP endpoint %q, using authentication = %v", tc.endpoint, withCookie)
				req, err := http.NewRequest("GET", urlMap[1]+tc.endpoint, nil /* body */)
				if err != nil {
					t.Fatalf("unexpected error while constructing request for %s: %s", tc.endpoint, err)
				}
				if withCookie {
					// Prevent regression of #25771 by also sending authenticated
					// requests, like would be sent if an admin UI were open against
					// this node while it booted.
					cookie, err := authserver.EncodeSessionCookie(&serverpb.SessionCookie{
						// The actual contents of the cookie don't matter; the presence of
						// a valid encoded cookie is enough to trigger the authentication
						// code paths.
					}, false /* forHTTPSOnly - cluster is insecure */)
					if err != nil {
						t.Fatal(err)
					}
					req.AddCookie(cookie)
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("unexpected error hitting %s endpoint: %v", tc.endpoint, err)
				}
				//nolint:deferloop TODO(#137605)
				defer resp.Body.Close()
				if resp.StatusCode != tc.expectedStatus {
					bodyBytes, _ := io.ReadAll(resp.Body)
					t.Fatalf("unexpected response code %d (expected %d) hitting %s endpoint: %v",
						resp.StatusCode, tc.expectedStatus, tc.endpoint, string(bodyBytes))
				}
			}

		}

		t.L().Printf("sending init command to node %d", initNode)
		c.Run(ctx, option.WithNodes(c.Node(initNode)), `./cockroach init --url={pgurl:1}`)

		// This will only succeed if 3 nodes joined the cluster.
		err = roachtestutil.WaitFor3XReplication(ctx, t.L(), dbs[0])
		require.NoError(t, err)

		execCLI := func(runNode int, extraArgs ...string) (string, error) {
			args := []string{"./cockroach"}
			args = append(args, extraArgs...)
			args = append(args, "--url={pgurl:1}")
			result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Node(runNode)), args...)
			combinedOutput := result.Stdout + result.Stderr
			t.L().Printf("%s\n", combinedOutput)
			return combinedOutput, err
		}

		{
			t.L().Printf("checking that double init fails")
			// Make sure that running init again returns the expected error message and
			// does not break the cluster. We have to use ExecCLI rather than OneShot in
			// order to actually get the output from the command.
			if output, err := execCLI(initNode, "init"); err == nil {
				t.Fatalf("expected error running init command on initialized cluster\n%s", output)
			} else if !strings.Contains(output, "cluster has already been initialized") {
				t.Fatalf("unexpected output when running init command on initialized cluster: %v\n%s",
					err, output)
			}
		}

		// Once initialized, the queries we started earlier will finish.
		t.L().Printf("waiting for original SQL queries to complete now cluster is initialized")
		deadline := time.After(10 * time.Second)
		for i := 0; i < len(dbs); i++ {
			select {
			case err := <-errCh:
				if err != nil {
					t.Fatalf("querying node %d: %s", i, err)
				}
			case <-deadline:
				t.Fatalf("timed out waiting for query %d", i)
			}
		}

		t.L().Printf("testing new SQL queries")
		for i, db := range dbs {
			var val int
			if err := db.QueryRow("SELECT 1").Scan(&val); err != nil {
				t.Fatalf("querying node %d: %s", i, err)
			}
		}

		t.L().Printf("test complete")
	}
}
