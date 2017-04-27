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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package acceptance

import (
	gosql "database/sql"
	"fmt"
	"net/http"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDebugRemote(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	SkipUnlessLocal(t)
	cfg := cluster.TestConfig{
		Name:     "TestDebugRemote",
		Duration: *flagDuration,
		Nodes:    []cluster.NodeConfig{{Count: 1, Stores: []cluster.StoreConfig{{Count: 1}}}},
	}
	ctx := context.Background()
	l := StartCluster(ctx, t, cfg).(*cluster.LocalCluster)
	defer l.AssertAndStop(ctx, t)

	db, err := gosql.Open("postgres", l.PGUrl(ctx, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	testCases := []struct {
		remoteDebug string
		status      int
	}{
		{"any", http.StatusOK},
		{"TRUE", http.StatusOK},
		{"t", http.StatusOK},
		{"1", http.StatusOK},
		{"local", http.StatusForbidden},
		{"false", http.StatusForbidden},
		{"unrecognized", http.StatusForbidden},
	}
	for _, c := range testCases {
		t.Run(c.remoteDebug, func(t *testing.T) {
			setStmt := fmt.Sprintf("SET CLUSTER SETTING server.remote_debugging.mode = '%s'",
				c.remoteDebug)
			if _, err := db.Exec(setStmt); err != nil {
				t.Fatal(err)
			}

			resp, err := cluster.HTTPClient.Get(l.URL(ctx, 0) + "/debug/")
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()

			if c.status != resp.StatusCode {
				t.Fatalf("expected %d, but got %d", c.status, resp.StatusCode)
			}
		})
	}
}
