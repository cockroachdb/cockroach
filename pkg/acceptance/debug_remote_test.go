// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package acceptance

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDebugRemote(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)
	// TODO(tschottdorf): hard to run this as RunLocal since we need to access
	// the ui endpoint from a non-local address.
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

	testCases := []struct {
		remoteDebug string
		status      int
		expectedErr string
	}{
		{"any", http.StatusOK, ""},
		{"ANY", http.StatusOK, ""},
		{"local", http.StatusForbidden, ""},
		{"off", http.StatusForbidden, ""},
		{"unrecognized", http.StatusForbidden, "invalid mode: 'unrecognized'"},
	}
	for _, c := range testCases {
		t.Run(c.remoteDebug, func(t *testing.T) {
			setStmt := fmt.Sprintf("SET CLUSTER SETTING server.remote_debugging.mode = '%s'",
				c.remoteDebug)
			if _, err := db.Exec(setStmt); !testutils.IsError(err, c.expectedErr) {
				t.Fatalf("expected \"%s\", but found %v", c.expectedErr, err)
			}
			for i, url := range []string{
				"/debug/",
				"/debug/pprof",
				"/debug/requests",
				"/debug/range?id=1",
				"/debug/certificates",
				"/debug/logspy?duration=1ns",
			} {
				t.Run(url, func(t *testing.T) {
					resp, err := cluster.HTTPClient.Get(l.URL(ctx, 0) + url)
					if err != nil {
						t.Fatalf("%d: %v", i, err)
					}
					resp.Body.Close()

					if c.status != resp.StatusCode {
						t.Fatalf("%d: expected %d, but got %d", i, c.status, resp.StatusCode)
					}
				})
			}
		})
	}
}
