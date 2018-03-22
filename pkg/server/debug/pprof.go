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

package debug

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strconv"
	"time"

	"github.com/google/pprof/driver"
	"github.com/google/pprof/profile"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

type pprofHandler struct {
	profileHandler http.HandlerFunc
}

func (ph *pprofHandler) Fetch(src string, duration, timeout time.Duration) (*profile.Profile, string, error) {
	rec := httptest.NewRecorder()
	req, err := http.NewRequest(
		http.MethodGet,
		"http://ignored?seconds="+strconv.Itoa(int(duration/time.Second)),
		bytes.NewReader(nil),
	)

	if err != nil {
		return nil, "", err
	}
	ph.profileHandler(rec, req)
	if rec.Code != http.StatusOK {
		return nil, "", errors.New(rec.Body.String())
	}

	if rec.Body == nil || rec.Body.Len() == 0 {
		// profile.Parse likes to panic.
		return nil, "", errors.New("empty response from pprof handler")
	}

	p, err := profile.Parse(rec.Body)
	return p, "", err
}

type goFlags struct {
	*pflag.FlagSet
}

func (goFlags) ExtraUsage() string {
	return ""
}

func (f goFlags) StringList(o, d, c string) *[]*string {
	return &[]*string{f.String(o, d, c)}
}

const profileDuration = 5 * time.Second

func (f goFlags) Parse(usage func()) []string {
	f.FlagSet.Usage = usage
	if err := f.FlagSet.Parse([]string{
		"--symbolize", "none",
		// TODO(tschottdorf): prevent this from opening a bogus browser window.
		"--http", "localhost:0",
		"--seconds", strconv.Itoa(int(profileDuration / time.Second)),
		"target/ignored/by/Fetch",
	}); err != nil {
		panic(err)
	}
	return f.FlagSet.Args()
}

func (ph *pprofHandler) Handle(w http.ResponseWriter, r *http.Request) {
	flags := &goFlags{
		FlagSet: pflag.NewFlagSet("pprof", pflag.ExitOnError),
	}

	served := make(chan struct{})
	server := func(args *driver.HTTPServerArgs) error {
		defer close(served)
		req, err := http.NewRequest(http.MethodGet, "<ignored>", bytes.NewReader(nil))
		if err != nil {
			return err
		}
		args.Handlers["/"].ServeHTTP(w, req)
		return nil
	}

	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		errCh <- func() error {
			return driver.PProf(&driver.Options{
				Flagset:    flags,
				Fetch:      ph,
				HTTPServer: server,
			})
		}()
	}()

	for err := range errCh {
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
	}
}
