// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pprofui

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
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

var supportedProfiles = []string{
	"cpu", "goroutine", "heap", "threadcreate", "block", "mutex", "allocs",
}

func TestServer(t *testing.T) {
	expectedNodeID := "local"
	withLabels := false

	mockProfile := func(ctx context.Context, req *serverpb.ProfileRequest) (*serverpb.JSONResponse, error) {
		require.Equal(t, expectedNodeID, req.NodeId)
		require.Equal(t, withLabels, req.Labels)
		b, err := os.ReadFile(datapathutils.TestDataPath(t, "heap.profile"))
		require.NoError(t, err)
		return &serverpb.JSONResponse{Data: b}, nil
	}

	storage := NewMemStorage(1, 0)
	s := NewServer(storage, ProfilerFunc(mockProfile))

	count := 1
	for i := 0; i < 3; i++ {
		for _, profileType := range supportedProfiles {
			t.Run(fmt.Sprintf("request local profile %s:%d", profileType, i), func(t *testing.T) {
				r := httptest.NewRequest("GET", fmt.Sprintf("/%s/", profileType), nil)
				w := httptest.NewRecorder()
				s.ServeHTTP(w, r)

				require.Equal(t, http.StatusTemporaryRedirect, w.Code)

				loc := w.Result().Header.Get("Location")
				require.Equal(t, fmt.Sprintf("/%s/%d/flamegraph", profileType, count), loc)
				count++

				r = httptest.NewRequest("GET", loc, nil)
				w = httptest.NewRecorder()

				s.ServeHTTP(w, r)

				require.Equal(t, http.StatusOK, w.Code)
				require.Contains(t, w.Body.String(), "pprof</a></h1>")
			})
		}
		require.Equal(t, 1, len(storage.GetRecords()),
			"storage did not expunge records")
	}

	t.Run("request profile from another node", func(t *testing.T) {
		expectedNodeID = "3"

		r := httptest.NewRequest("GET", "/heap/?node=3", nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)

		require.Equal(t, http.StatusTemporaryRedirect, w.Code)
		loc := w.Result().Header.Get("Location")
		require.Equal(t, fmt.Sprintf("/heap/%d/flamegraph?node=3", count), loc)
	})

	t.Run("request profile with labels", func(t *testing.T) {
		expectedNodeID = "3"
		withLabels = true
		defer func() {
			withLabels = false
		}()

		// Labels are only supported for CPU and GOROUTINE profiles.
		r := httptest.NewRequest("GET", "/heap/?node=3&labels=true", nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		count++
		require.Equal(t, http.StatusInternalServerError, w.Code)
		require.Equal(t, "profiling with labels is unsupported for HEAP\n", w.Body.String())

		r = httptest.NewRequest("GET", "/cpu/?node=3&labels=true", nil)
		w = httptest.NewRecorder()
		s.ServeHTTP(w, r)
		count++
		require.Equal(t, http.StatusTemporaryRedirect, w.Code)
		loc := w.Result().Header.Get("Location")
		require.Equal(t, fmt.Sprintf("/cpu/%d/flamegraph?node=3&labels=true", count), loc)

		// A GOROUTINE profile with a label will trigger a download since it is not
		// generated a Profile protobuf format.
		r = httptest.NewRequest("GET", "/goroutine/?node=3&labels=true", nil)
		w = httptest.NewRecorder()
		s.ServeHTTP(w, r)
		count++
		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "attachment; filename=goroutine_25.txt", w.Header().Get("Content-Disposition"))
		require.Equal(t, "text/plain", w.Header().Get("Content-Type"))
	})

	t.Run("request cluster-wide profiles", func(t *testing.T) {
		expectedNodeID = "all"

		// Cluster-wide profiles are only supported for CPU and GOROUTINE profiles.
		r := httptest.NewRequest("GET", "/heap/?node=all", nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		count++
		require.Equal(t, http.StatusInternalServerError, w.Code)
		require.Equal(t, "cluster-wide collection is unsupported for HEAP\n", w.Body.String())

		r = httptest.NewRequest("GET", "/cpu/?node=all", nil)
		w = httptest.NewRecorder()
		s.ServeHTTP(w, r)
		count++
		require.Equal(t, http.StatusTemporaryRedirect, w.Code)
		loc := w.Result().Header.Get("Location")
		require.Equal(t, fmt.Sprintf("/cpu/%d/flamegraph?node=all", count), loc)

		// A GOROUTINE profile with a label will trigger a download since it is not
		// generated a Profile protobuf format.
		r = httptest.NewRequest("GET", "/goroutine/?node=all", nil)
		w = httptest.NewRecorder()
		s.ServeHTTP(w, r)
		count++
		require.Equal(t, http.StatusTemporaryRedirect, w.Code)
		loc = w.Result().Header.Get("Location")
		require.Equal(t, fmt.Sprintf("/goroutine/%d/flamegraph?node=all", count), loc)
	})

	t.Run("request profile with label filters", func(t *testing.T) {
		expectedNodeID = "3"
		withLabels = true
		defer func() {
			withLabels = false
		}()

		// Labels are only supported for CPU and GOROUTINE profiles.
		r := httptest.NewRequest("GET", "/heap/?node=3&labels=true&labelfilter=foo:bar", nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		count++
		require.Equal(t, http.StatusInternalServerError, w.Code)
		require.Equal(t, "profiling with labels is unsupported for HEAP\n", w.Body.String())

		// A GOROUTINE profile with a label will trigger a download since it is not
		// generated a Profile protobuf format.
		r = httptest.NewRequest("GET", "/goroutine/?node=3&labelfilter=foo:bar", nil)
		w = httptest.NewRecorder()
		s.ServeHTTP(w, r)
		count++
		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "attachment; filename=goroutine_30.txt", w.Header().Get("Content-Disposition"))
		require.Equal(t, "text/plain", w.Header().Get("Content-Type"))
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
		b, err := os.ReadFile(datapathutils.TestDataPath(t, fileName))
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

func TestFilterStacksWithLabels(t *testing.T) {
	testStacks := `
Stacks for node 1:

8 @ 0x4a13d6 0x46b3bb 0x46aef8 0x12fd53f 0xe9bc43 0x12fd478 0x4d3101
# labels: {"foo":"baz", "bar":"biz"}
#       0x12fd53e       github.com/cockroachdb/pebble.(*fileCacheShard).releaseLoop.func1+0x9e github.com/cockroachdb/pebble/external/com_github_cockroachdb_pebble/file_cache.go:324

10 @ 0x4a13d6 0x4b131c 0x17969e6 0x4d3101
#       0x17969e5       github.com/cockroachdb/cockroach/pkg/util/admission.initWorkQueue.func2+0x85    github.com/cockroachdb/cockroach/pkg/util/admission/work_queue.go:388

8 @ 0x4a13d6 0x4b131c 0x1796e96 0x4d3101
# labels: {"bar":"biz"}
#       0x1796e95       github.com/cockroachdb/cockroach/pkg/util/admission.(*WorkQueue).startClosingEpochs.func1+0x1d5 github.com/cockroachdb/cockroach/pkg
/util/admission/work_queue.go:462

1 @ 0x100907fc4 0x100919c8c 0x100919c69 0x1009354d8 0x1009459c0 0x101e2015c 0x101ec4b08 0x101ec4fc8 0x101f0b78c 0x101f0addc 0x1015319fc 0x100939fb4
# labels: {"range_str":"12419/2:/Table/136/1/"{NHCH-…-PWN-a"}", "n":"1", "rangefeed":"sql-watcher-descriptor-rangefeed"}
#	0x1009354d7	sync.runtime_Semacquire+0x27									GOROOT/src/runtime/sema.go:62


`
	t.Run("empty filter", func(t *testing.T) {
		res := FilterStacksWithLabels([]byte(testStacks), "")
		require.Equal(t, `
Stacks for node 1:

8 @ 0x4a13d6 0x46b3bb 0x46aef8 0x12fd53f 0xe9bc43 0x12fd478 0x4d3101
# labels: {"foo":"baz", "bar":"biz"}
#       0x12fd53e       github.com/cockroachdb/pebble.(*fileCacheShard).releaseLoop.func1+0x9e github.com/cockroachdb/pebble/external/com_github_cockroachdb_pebble/file_cache.go:324

10 @ 0x4a13d6 0x4b131c 0x17969e6 0x4d3101
#       0x17969e5       github.com/cockroachdb/cockroach/pkg/util/admission.initWorkQueue.func2+0x85    github.com/cockroachdb/cockroach/pkg/util/admission/work_queue.go:388

8 @ 0x4a13d6 0x4b131c 0x1796e96 0x4d3101
# labels: {"bar":"biz"}
#       0x1796e95       github.com/cockroachdb/cockroach/pkg/util/admission.(*WorkQueue).startClosingEpochs.func1+0x1d5 github.com/cockroachdb/cockroach/pkg
/util/admission/work_queue.go:462

1 @ 0x100907fc4 0x100919c8c 0x100919c69 0x1009354d8 0x1009459c0 0x101e2015c 0x101ec4b08 0x101ec4fc8 0x101f0b78c 0x101f0addc 0x1015319fc 0x100939fb4
# labels: {"range_str":"12419/2:/Table/136/1/"{NHCH-…-PWN-a"}", "n":"1", "rangefeed":"sql-watcher-descriptor-rangefeed"}
#	0x1009354d7	sync.runtime_Semacquire+0x27									GOROOT/src/runtime/sema.go:62


`, string(res))
	})

	t.Run("bar-biz filter", func(t *testing.T) {
		res := FilterStacksWithLabels([]byte(testStacks), "\"bar\":\"biz\"")
		require.Equal(t, `
Stacks for node 1:

8 @ 0x4a13d6 0x46b3bb 0x46aef8 0x12fd53f 0xe9bc43 0x12fd478 0x4d3101
# labels: {"foo":"baz", "bar":"biz"}
#       0x12fd53e       github.com/cockroachdb/pebble.(*fileCacheShard).releaseLoop.func1+0x9e github.com/cockroachdb/pebble/external/com_github_cockroachdb_pebble/file_cache.go:324

8 @ 0x4a13d6 0x4b131c 0x1796e96 0x4d3101
# labels: {"bar":"biz"}
#       0x1796e95       github.com/cockroachdb/cockroach/pkg/util/admission.(*WorkQueue).startClosingEpochs.func1+0x1d5 github.com/cockroachdb/cockroach/pkg
/util/admission/work_queue.go:462

`, string(res))
	})

	t.Run("filter non-alphanumeric characters", func(t *testing.T) {
		res := FilterStacksWithLabels([]byte(testStacks), "\"{NHCH")
		require.Equal(t, `
Stacks for node 1:

1 @ 0x100907fc4 0x100919c8c 0x100919c69 0x1009354d8 0x1009459c0 0x101e2015c 0x101ec4b08 0x101ec4fc8 0x101f0b78c 0x101f0addc 0x1015319fc 0x100939fb4
# labels: {"range_str":"12419/2:/Table/136/1/"{NHCH-…-PWN-a"}", "n":"1", "rangefeed":"sql-watcher-descriptor-rangefeed"}
#	0x1009354d7	sync.runtime_Semacquire+0x27									GOROOT/src/runtime/sema.go:62

`, string(res))
	})
}
