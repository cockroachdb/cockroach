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

package server

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

type regexpType struct {
	re *regexp.Regexp
}

func (r regexpType) String() string {
	if r.re == nil {
		return ".*"
	}
	return r.re.String()
}

func (r *regexpType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	var err error
	(*r).re, err = regexp.Compile(s)
	return err
}

type intAsString int

func (i *intAsString) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	var err error
	*(*int)(i), err = strconv.Atoi(s)
	return err
}

type durationAsString time.Duration

func (d *durationAsString) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	var err error
	*(*time.Duration)(d), err = time.ParseDuration(s)
	return err
}

func (d durationAsString) String() string {
	return time.Duration(d).String()
}

const maxLogSpyDuration = durationAsString(time.Minute)

type logSpyOptions struct {
	Count    intAsString
	Duration durationAsString
	Message  regexpType
	FileLine regexpType
}

func logSpyOptionsFromValues(values url.Values) (logSpyOptions, error) {
	rawValues := map[string]string{}
	for k, vals := range values {
		if len(vals) > 0 {
			rawValues[k] = vals[0]
		}
	}
	data, err := json.Marshal(rawValues)
	if err != nil {
		return logSpyOptions{}, err
	}
	var opts logSpyOptions
	if err := json.Unmarshal(data, &opts); err != nil {
		return logSpyOptions{}, err
	}
	if opts.Duration == 0 {
		if opts.Count > 0 {
			opts.Duration = maxLogSpyDuration
		} else {
			opts.Duration = durationAsString(5 * time.Second)
		}
	} else if opts.Duration > maxLogSpyDuration {
		return logSpyOptions{}, errors.Errorf("duration %s is too large (limit is %s)", opts.Duration, maxLogSpyDuration)
	}

	if opts.Count == 0 {
		opts.Count = math.MaxInt32
	}
	return opts, nil
}

type logSpy struct {
	active       int32 // updated atomically between 0 and 1
	setIntercept func(ctx context.Context, f log.InterceptorFn)
}

func (spy *logSpy) handleDebugLogSpy(w http.ResponseWriter, r *http.Request) {
	opts, err := logSpyOptionsFromValues(r.URL.Query())
	if err != nil {
		http.Error(w, "while parsing options: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if swapped := atomic.CompareAndSwapInt32(&spy.active, 0, 1); !swapped {
		http.Error(w, "a log interception is already in progress", http.StatusInternalServerError)
		return
	}
	defer atomic.StoreInt32(&spy.active, 0)

	w.Header().Add("Content-type", "text/plain; charset=UTF-8")
	spy.run(r.Context(), w, opts)
}

func (spy *logSpy) run(ctx context.Context, w io.Writer, opts logSpyOptions) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(opts.Duration))
	defer cancel()

	// Note that in the code below, this channel is never closed. This is
	// because we don't know when that is safe. This is sketchy in general but
	// OK here since we don't have to guarantee that the channel is fully
	// consumed.
	entries := make(chan log.Entry, 4096)

	{
		f, l, _ := caller.Lookup(0)
		entries <- log.Entry{
			File:    f,
			Line:    int64(l),
			Message: fmt.Sprintf("%s: intercepting logs with options %+v", timeutil.Now(), opts),
		}
	}

	spy.setIntercept(ctx, func(entry log.Entry) {
		if re := opts.Message.re; re != nil && !re.MatchString(entry.Message) {
			return
		}

		if re := opts.FileLine.re; re != nil && !re.MatchString(entry.File+":"+strconv.FormatInt(entry.Line, 10)) {
			return
		}

		select {
		case entries <- entry:
		default:
			// Consumer fell behind, just drop the message.
		}
	})
	defer spy.setIntercept(ctx, nil)

	tBegin := timeutil.Now()
	format := func(entry log.Entry) string {
		return fmt.Sprintf("+%0.1fs %s:%d %s",
			timeutil.Since(tBegin).Seconds(),
			entry.File, entry.Line,
			strings.TrimSpace(entry.Message),
		)
	}

	const flushInterval = time.Second
	var flushTimer timeutil.Timer
	defer flushTimer.Stop()
	flushTimer.Reset(flushInterval)

	var count intAsString
	var done <-chan struct{} // set later; helps always send header message
	for {
		select {
		case <-done:
			return
		case entry := <-entries:
			fmt.Fprintln(w, format(entry))
			count++
			if count > opts.Count {
				return
			}
			if done == nil {
				done = ctx.Done()
			}
		case <-flushTimer.C:
			flushTimer.Read = true
			flushTimer.Reset(flushInterval)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
	}
}
