// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pprofui_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/server/debug/pprofui"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func init() {
	if bazel.BuiltWithBazel() {
		path, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		// dot wants HOME set.
		err = os.Setenv("HOME", path)
		if err != nil {
			panic(err)
		}
	}
}

func TestServer(t *testing.T) {
	storage := pprofui.NewMemStorage(1, 0)
	s := pprofui.NewServer(storage, nil, nil)

	for i := 0; i < 3; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			r := httptest.NewRequest("GET", "/heap/", nil)
			w := httptest.NewRecorder()
			s.ServeHTTP(w, r)

			if a, e := w.Code, http.StatusTemporaryRedirect; a != e {
				t.Fatalf("expected status code %d, got %d", e, a)
			}

			loc := w.Result().Header.Get("Location")

			if a, e := loc, fmt.Sprintf("/heap/%d/flamegraph", i+1); a != e {
				t.Fatalf("expected location header %s, but got %s", e, a)
			}

			r = httptest.NewRequest("GET", loc, nil)
			w = httptest.NewRecorder()

			s.ServeHTTP(w, r)

			if a, e := w.Code, http.StatusOK; a != e {
				t.Fatalf("expected status code %d, got %d", e, a)
			}

			if a, e := w.Body.String(), "pprof</a></h1>"; !strings.Contains(a, e) {
				t.Fatalf("body does not contain %q: %v", e, a)
			}
		})
		if a, e := len(storage.GetRecords()), 1; a != e {
			t.Fatalf("storage did not expunge records; have %d instead of %d", a, e)
		}
	}
}

func TestServerDifferentNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	thirdServer := testCluster.Server(2)

	storage := pprofui.NewMemStorage(1, 0)
	s3 := pprofui.NewServer(storage, thirdServer.GossipI().(*gossip.Gossip), thirdServer.RPCContext())

	for i := 1; i < 4; i++ {
		t.Run(fmt.Sprintf("requesting pprof from node %d", i), func(t *testing.T) {
			r := httptest.NewRequest("GET", fmt.Sprintf("/heap?node=%d", i), nil)
			w := httptest.NewRecorder()
			s3.ServeHTTP(w, r)

			if a, e := w.Code, http.StatusTemporaryRedirect; a != e {
				t.Fatalf("expected status code %d, got %d", e, a)
			}

			loc := w.Result().Header.Get("Location")

			if a, e := loc, fmt.Sprintf("/heap/%d/flamegraph?node=%d", i, i); a != e {
				t.Fatalf("expected location header %s, but got %s", e, a)
			}

			r = httptest.NewRequest("GET", loc, nil)
			w = httptest.NewRecorder()

			s3.ServeHTTP(w, r)

			if a, e := w.Code, http.StatusOK; a != e {
				t.Fatalf("expected status code %d, got %d", e, a)
			}

			if a, e := w.Body.String(), "pprof</a></h1>"; !strings.Contains(a, e) {
				t.Fatalf("body does not contain %q: %v", e, a)
			}
		})
		if a, e := len(storage.GetRecords()), 1; a != e {
			t.Fatalf("storage did not expunge records; have %d instead of %d", a, e)
		}
	}
}
