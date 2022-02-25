// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pprofui

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/stretchr/testify/require"
)

type ProfilerFunc func(ctx context.Context, req *serverpb.ProfileRequest) (*serverpb.JSONResponse, error)

func (p ProfilerFunc) Profile(
	ctx context.Context, req *serverpb.ProfileRequest,
) (*serverpb.JSONResponse, error) {
	return p(ctx, req)
}

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
	expectedNodeID := "local"

	mockProfile := func(ctx context.Context, req *serverpb.ProfileRequest) (*serverpb.JSONResponse, error) {
		require.Equal(t, expectedNodeID, req.NodeId)
		b, err := ioutil.ReadFile(testutils.TestDataPath(t, "heap.profile"))
		require.NoError(t, err)
		return &serverpb.JSONResponse{Data: b}, nil
	}

	storage := NewMemStorage(1, 0)
	s := NewServer(storage, ProfilerFunc(mockProfile))

	for i := 0; i < 3; i++ {
		t.Run(fmt.Sprintf("request local profile %d", i), func(t *testing.T) {
			r := httptest.NewRequest("GET", "/heap/", nil)
			w := httptest.NewRecorder()
			s.ServeHTTP(w, r)

			require.Equal(t, http.StatusTemporaryRedirect, w.Code)

			loc := w.Result().Header.Get("Location")
			require.Equal(t, fmt.Sprintf("/heap/%d/flamegraph", i+1), loc)

			r = httptest.NewRequest("GET", loc, nil)
			w = httptest.NewRecorder()

			s.ServeHTTP(w, r)

			require.Equal(t, http.StatusOK, w.Code)
			require.Contains(t, w.Body.String(), "pprof</a></h1>")
		})
		require.Equal(t, 1, len(storage.getRecords()),
			"storage did not expunge records")
	}

	t.Run("request profile from another node", func(t *testing.T) {
		expectedNodeID = "3"

		r := httptest.NewRequest("GET", "/heap/?node=3", nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)

		require.Equal(t, http.StatusTemporaryRedirect, w.Code)
		loc := w.Result().Header.Get("Location")
		require.Equal(t, "/heap/4/flamegraph?node=3", loc)
	})
}

func TestServerConcurrentAccess(t *testing.T) {
	expectedNodeID := "local"
	skip.UnderRace(t, "test fails under race due to known race condition with profiles")
	const (
		runsPerWorker = 1
		workers       = ProfileConcurrency
	)
	mockProfile := func(ctx context.Context, req *serverpb.ProfileRequest) (*serverpb.JSONResponse, error) {
		require.Equal(t, expectedNodeID, req.NodeId)
		fileName := "heap.profile"
		if req.Type == serverpb.ProfileRequest_CPU {
			fileName = "cpu.profile"
		}
		b, err := ioutil.ReadFile(testutils.TestDataPath(t, fileName))
		require.NoError(t, err)
		return &serverpb.JSONResponse{Data: b}, nil
	}

	s := NewServer(NewMemStorage(ProfileConcurrency, ProfileExpiry), ProfilerFunc(mockProfile))
	getProfile := func(profile string, t *testing.T) {
		t.Helper()

		r := httptest.NewRequest("GET", "/heap/", nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)

		require.Equal(t, http.StatusTemporaryRedirect, w.Code)

		loc := w.Result().Header.Get("Location")

		r = httptest.NewRequest("GET", loc, nil)
		w = httptest.NewRecorder()

		s.ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, w.Body.String(), "pprof</a></h1>")
	}
	var wg sync.WaitGroup
	profiles := [2]string{"/heap/", "/cpu"}
	runWorker := func() {
		defer wg.Done()
		for i := 0; i < runsPerWorker; i++ {
			time.Sleep(time.Microsecond)
			profileID := rand.Intn(len(profiles))
			getProfile(profiles[profileID], t)
		}
	}
	// Run the workers.
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go runWorker()
	}
	wg.Wait()
}
