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
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	runtimepprof "runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/google/pprof/driver"
	"github.com/google/pprof/profile"
	"github.com/spf13/pflag"
)

// A Server serves up the pprof web ui. A request to /<profiletype>
// generates a profile of the desired type and redirects to the UI for
// it at /<profiletype>/<id>. Valid profile types at the time of
// writing include `profile` (cpu), `goroutine`, `threadcreate`,
// `heap`, `block`, and `mutex`.
type Server struct {
	storage      Storage
	profileSem   syncutil.Mutex
	statusServer serverpb.StatusServer
}

// TODO(davidh): fix the below docstring to remove hook reference
// NewServer creates a new Server backed by the supplied Storage and optionally
// a hook which is called when a new profile is created. The closure passed to
// the hook will carry out the work involved in creating the profile and must
// be called by the hook. The intention is that hook will be a method such as
// this:
//
// func hook(profile string, do func()) {
// 	if profile == "profile" {
// 		something.EnableProfilerLabels()
// 		defer something.DisableProfilerLabels()
// 		do()
// 	}
// }
func NewServer(storage Storage, statusServer serverpb.StatusServer) *Server {
	s := &Server{
		storage:      storage,
		statusServer: statusServer,
	}

	return s
}

// parsePath turns /profile/123/flamegraph/banana into (profile, 123, /flamegraph/banana).
func (s *Server) parsePath(reqPath string) (profType string, id string, remainingPath string) {
	parts := strings.Split(path.Clean(reqPath), "/")
	if parts[0] == "" {
		// The path was absolute (the typical case), pretend it was
		// relative (to this handler's root).
		parts = parts[1:]
	}
	switch len(parts) {
	case 0:
		return "", "", "/"
	case 1:
		return parts[0], "", "/"
	default:
		return parts[0], parts[1], "/" + strings.Join(parts[2:], "/")
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	profileName, id, remainingPath := s.parsePath(r.URL.Path)

	if profileName == "" {
		// TODO(tschottdorf): serve an overview page.
		var names []string
		for _, p := range runtimepprof.Profiles() {
			names = append(names, p.Name())
		}
		sort.Strings(names)
		msg := fmt.Sprintf("Try %s for one of %s", path.Join(r.RequestURI, "<profileName>"), strings.Join(names, ", "))
		http.Error(w, msg, http.StatusNotFound)
		return
	}

	if id != "" {
		// Catch nonexistent IDs early or pprof will do a worse job at
		// giving an informative error.
		if err := s.storage.Get(id, func(io.Reader) error { return nil }); err != nil {
			msg := fmt.Sprintf("profile for id %s not found: %s", id, err)
			http.Error(w, msg, http.StatusNotFound)
			return
		}

		if r.URL.Query().Get("download") != "" {
			// TODO(tbg): this has zero discoverability.
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s_%s.pb.gz", profileName, id))
			w.Header().Set("Content-Type", "application/octet-stream")
			if err := s.storage.Get(id, func(r io.Reader) error {
				_, err := io.Copy(w, r)
				return err
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		server := func(args *driver.HTTPServerArgs) error {
			handler, ok := args.Handlers[remainingPath]
			if !ok {
				return errors.Errorf("unknown endpoint %s", remainingPath)
			}
			handler.ServeHTTP(w, r)
			return nil
		}

		storageFetcher := func(_ string, _, _ time.Duration) (*profile.Profile, string, error) {
			var p *profile.Profile
			if err := s.storage.Get(id, func(reader io.Reader) error {
				var err error
				p, err = profile.Parse(reader)
				return err
			}); err != nil {
				return nil, "", err
			}
			return p, "", nil
		}

		// Invoke the (library version) of `pprof` with a number of stubs.
		// Specifically, we pass a fake FlagSet that plumbs through the
		// given args, a UI that logs any errors pprof may emit, a fetcher
		// that simply reads the profile we downloaded earlier, and a
		// HTTPServer that pprof will pass the web ui handlers to at the
		// end (and we let it handle this client request).
		if err := driver.PProf(&driver.Options{
			Flagset: &pprofFlags{
				FlagSet: pflag.NewFlagSet("pprof", pflag.ExitOnError),
				args: []string{
					"--symbolize", "none",
					"--http", "localhost:0",
					"", // we inject our own target
				},
			},
			UI:         &fakeUI{},
			Fetch:      fetcherFn(storageFetcher),
			HTTPServer: server,
		}); err != nil {
			_, _ = w.Write([]byte(err.Error()))
		}

		return
	}

	// Create and save new profile, then redirect client to corresponding ui URL.

	id = s.storage.ID()

	if err := s.storage.Store(id, func(w io.Writer) error {
		req, err := http.NewRequest("GET", "/unused", bytes.NewReader(nil))
		if err != nil {
			return err
		}

		// TODO(davidh): Discrepancy between "CPU" and "profile" in the two implementations
		profileType, ok := serverpb.ProfileRequest_Type_value[strings.ToUpper(profileName)]
		if !ok && profileName != "profile" {
			return errors.Newf("unknown profile name: %s", profileName)
		}
		if profileName == "profile" {
			profileType = int32(serverpb.ProfileRequest_CPU)
		}
		var resp *serverpb.JSONResponse
		profileReq := &serverpb.ProfileRequest{
			NodeId: "local",
			Type:   serverpb.ProfileRequest_Type(profileType),
		}

		// Pass through any parameters. Most notably, allow ?seconds=10 for
		// CPU profiles.
		_ = r.ParseForm()
		req.Form = r.Form

		// TODO(davidh): do we need to support `&si=alloc_objects` for heap?

		if r.Form.Get("seconds") != "" {
			sec, err := strconv.ParseInt(r.Form.Get("seconds"), 10, 32)
			if err != nil {
				return err
			}
			profileReq.Seconds = int32(sec)
		}
		if r.Form.Get("node") != "" {
			profileReq.NodeId = r.Form.Get("node")
		}
		if r.Form.Get("labels") != "" {
			return errors.Newf("unsupported parameter 'labels': %s", r.Form.Get("labels"))
		}
		resp, err = s.statusServer.Profile(r.Context(), profileReq)
		if err != nil {
			return err
		}
		_, err = w.Write(resp.Data)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// NB: direct straight to the flamegraph. This is because `pprof`
	// shells out to `dot` for the default landing page and thus works
	// only on hosts that have graphviz installed. You can still navigate
	// to the dot page from there.
	origURL, err := url.Parse(r.RequestURI)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// If this is a request issued by `go tool pprof`, just return the profile
	// directly. This is convenient because it avoids having to expose the pprof
	// endpoints separately, and also allows inserting hooks around CPU profiles
	// in the future.
	isGoPProf := strings.Contains(r.Header.Get("User-Agent"), "Go-http-client")
	origURL.Path = path.Join(origURL.Path, id, "flamegraph")
	if !isGoPProf {
		http.Redirect(w, r, origURL.String(), http.StatusTemporaryRedirect)
	} else {
		_ = s.storage.Get(id, func(r io.Reader) error {
			_, err := io.Copy(w, r)
			return err
		})
	}
}

type fetcherFn func(_ string, _, _ time.Duration) (*profile.Profile, string, error)

func (f fetcherFn) Fetch(s string, d, t time.Duration) (*profile.Profile, string, error) {
	return f(s, d, t)
}
