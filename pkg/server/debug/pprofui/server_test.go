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
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
