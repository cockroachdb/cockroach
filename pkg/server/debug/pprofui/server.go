// Copyright 2018 The Cockroach Authors.
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

package pprofui

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/pprof"
	"path/filepath"
	"strconv"
	"time"

	runtimepprof "runtime/pprof"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/google/pprof/driver"
	"github.com/google/pprof/profile"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

func pprofCtx(ctx context.Context) context.Context {
	return log.WithLogTag(ctx, "pprof", nil)
}

// A Server serves up the pprof web ui, generating all of the
// requested profiles on demand. The supplied Args are passed
// to `pprof`; note that this cannot contain positional args
// or the --http flag.
type Server struct {
	Args []string
}

func (s *Server) ServeHTTP(origW http.ResponseWriter, r *http.Request) {
	w := &scriptInjectingWriter{ResponseWriter: origW}
	defer w.WriteReplaced(origW)

	profileMux := http.NewServeMux()

	// TODO(tschottdorf): embed the execution tracer UI as well (`go tool trace`),
	// for the `pprof.Trace` endpoint. Not sure how straightforward this is or
	// whether it's important enough to ever do.

	// Tricky: we can't actually use StripPrefix here because this will later
	// be passed a *http.Request made in `Fetcher()` which has only a trivial
	// path component.
	profileMux.Handle("/profile/", http.HandlerFunc(pprof.Profile))
	// We can't use the default ServeMux redirect behavior below because
	// this is what pprof hits, and it doesn't follow redirects.
	profileMux.Handle("/profile", http.HandlerFunc(pprof.Profile))

	// Register endpoints for heap, block, threadcreate, etc.
	for _, p := range runtimepprof.Profiles() {
		p := p // copy
		prefix := "/" + p.Name()
		f := func(w http.ResponseWriter, r *http.Request) {
			if err := p.WriteTo(w, 0 /* debug */); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
			}
		}
		profileMux.Handle(prefix, http.HandlerFunc(f))
		profileMux.Handle(prefix+"/", http.HandlerFunc(f))
	}

	fetchHandler, pat := profileMux.Handler(r)
	if pat == "" {
		w.Write([]byte("unknown profile type for " + r.URL.String()))
		return
	}

	server := func(args *driver.HTTPServerArgs) error {
		mux := http.NewServeMux()
		prefix := filepath.Dir(r.URL.Path)
		for pattern, handler := range args.Handlers {
			fullPattern := filepath.Join(prefix, pattern)
			mux.Handle(fullPattern, http.StripPrefix(prefix, handler))
			mux.Handle(fullPattern+"/", http.StripPrefix(prefix, handler))
		}
		mux.ServeHTTP(w, r)
		return nil
	}

	const profileDuration = 5 * time.Second
	args := append([]string(nil), s.Args...)
	if err := driver.PProf(&driver.Options{
		Flagset: &pprofFlags{
			FlagSet: pflag.NewFlagSet("pprof", pflag.ExitOnError),
			args: append(args,
				"--http", "localhost:0",
				"", // we inject our own target
			),
		},
		Fetch:      s.Fetcher(fetchHandler.ServeHTTP),
		UI:         &fakeUI{},
		HTTPServer: server,
	}); err != nil {
		w.Write([]byte(err.Error()))
	}
}

type fetcherFn func(src string, duration, timeout time.Duration) (*profile.Profile, string, error)

func (f fetcherFn) Fetch(
	src string, duration, timeout time.Duration,
) (*profile.Profile, string, error) {
	return f(src, duration, timeout)
}

func (*Server) Fetcher(Server http.HandlerFunc) driver.Fetcher {
	f := func(src string, duration, timeout time.Duration) (*profile.Profile, string, error) {
		rec := httptest.NewRecorder()
		req, err := http.NewRequest(
			http.MethodGet,
			"http://ignored/?seconds="+strconv.Itoa(int(duration/time.Second)),
			bytes.NewReader(nil),
		)

		if err != nil {
			return nil, "", err
		}
		Server(rec, req)
		if rec.Code != http.StatusOK {
			return nil, "", errors.New(rec.Body.String())
		}

		if rec.Body == nil || rec.Body.Len() == 0 {
			// profile.Parse likes to panic.
			return nil, "", errors.New("empty response from pprof handler")
		}

		// TODO(tschottdorf): we could also save the profiles to the log directory.
		p, err := profile.Parse(rec.Body)
		return p, "", err
	}
	return fetcherFn(f)
}
