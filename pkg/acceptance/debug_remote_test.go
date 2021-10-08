// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package acceptance

import (
	"context"
	gosql "database/sql"
	"net/http"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDebugRemote(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)
	// TODO(knz): This test can probably move to a unit test inside the
	// server package
	RunDocker(t, testDebugRemote)
}

func testDebugRemote(t *testing.T) {
	cfg := cluster.TestConfig{
		Name:     "TestDebugRemote",
		Duration: *flagDuration,
		Nodes:    []cluster.NodeConfig{{Stores: []cluster.StoreConfig{{}}}},
	}
	ctx := context.Background()
	l := StartCluster(ctx, t, cfg).(*cluster.DockerCluster)
	defer l.AssertAndStop(ctx, t)

	db, err := gosql.Open("postgres", l.PGUrl(ctx, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stdout, stderr, err := l.ExecCLI(ctx, 0, []string{"auth-session", "login", "root", "--only-cookie"})
	if err != nil {
		t.Fatalf("auth-session failed: %s\nstdout: %s\nstderr: %s\n", err, stdout, stderr)
	}
	cookie := strings.Trim(stdout, "\n")

	for i, url := range []string{
		"/debug/",
		"/debug/pprof",
		"/debug/requests",
		"/debug/range?id=1",
		"/debug/certificates",
		"/debug/logspy?duration=1ns",
	} {
		t.Run(url, func(t *testing.T) {
			req, err := http.NewRequest("GET", l.URL(ctx, 0)+url, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Cookie", cookie)
			resp, err := cluster.HTTPClient.Do(req)
			if err != nil {
				t.Fatalf("%d: %v", i, err)
			}
			resp.Body.Close()

			if http.StatusOK != resp.StatusCode {
				t.Fatalf("%d: expected %d, but got %d", i, http.StatusOK, resp.StatusCode)
			}
		})
	}
}
