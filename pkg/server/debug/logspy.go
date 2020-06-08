// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package debug

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// regexpAsString wraps a *regexp.Regexp for better printing and
// JSON unmarshaling.
type regexpAsString struct {
	re *regexp.Regexp
	i  int64 // optional, populated if the regexp is an integer to match on goroutine ID
}

func (r regexpAsString) String() string {
	if r.re == nil {
		return ".*"
	}
	return r.re.String()
}

func (r *regexpAsString) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if i, err := strconv.ParseInt(s, 10, 64); err != nil {
		// Ignore.
	} else {
		r.i = i
	}
	var err error
	(*r).re, err = regexp.Compile(s)
	return err
}

// intAsString wraps an int that can be populated from a JSON string.
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

// durationAsString wraps a time.Duration that can be populated from a JSON
// string.
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

const (
	logSpyDefaultDuration = durationAsString(5 * time.Second)
	logSpyMaxCount        = 10000
	logSpyDefaultCount    = 1000
	logSpyChanCap         = 4096
)

type logSpyOptions struct {
	Count    intAsString
	Duration durationAsString
	Grep     regexpAsString
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
		opts.Duration = logSpyDefaultDuration
	}

	if opts.Count == 0 {
		opts.Count = logSpyDefaultCount
	} else if opts.Count > logSpyMaxCount {
		return logSpyOptions{}, errors.Errorf(
			"count %d is too large (limit is %d); consider restricting your filter",
			opts.Count, logSpyMaxCount,
		)
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
	ctx := r.Context()
	if err := spy.run(ctx, w, opts); err != nil {
		// This is likely a broken HTTP connection, so nothing too unexpected.
		log.Infof(ctx, "%v", err)
	}
}

func (spy *logSpy) run(ctx context.Context, w io.Writer, opts logSpyOptions) (err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(opts.Duration))
	defer cancel()

	var countDropped int32
	defer func() {
		if err == nil {
			if dropped := atomic.LoadInt32(&countDropped); dropped > 0 {
				entry := log.MakeEntry(
					ctx, log.Severity_WARNING, nil /* LogCounter */, 0 /* depth */, false, /* redactable */
					"%d messages were dropped", log.Safe(dropped))
				err = entry.Format(w) // modify return value
			}
		}
	}()

	// Note that in the code below, this channel is never closed. This is
	// because we don't know when that is safe. This is sketchy in general but
	// OK here since we don't have to guarantee that the channel is fully
	// consumed.
	entries := make(chan log.Entry, logSpyChanCap)

	{
		entry := log.MakeEntry(
			ctx, log.Severity_INFO, nil /* LogCounter */, 0 /* depth */, false, /* redactable */
			"intercepting logs with options %+v", opts)
		entries <- entry
	}

	spy.setIntercept(ctx, func(entry log.Entry) {
		if re := opts.Grep.re; re != nil {
			switch {
			case re.MatchString(entry.Tags):
			case re.MatchString(entry.Message):
			case re.MatchString(entry.File):
			case opts.Grep.i != 0 && opts.Grep.i == entry.Goroutine:
			default:
				return
			}
		}

		select {
		case entries <- entry:
		default:
			// Consumer fell behind, just drop the message.
			atomic.AddInt32(&countDropped, 1)
		}
	})

	defer spy.setIntercept(ctx, nil)

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
			if err := entry.Format(w); err != nil {
				return errors.Wrapf(err, "while writing entry %v", entry)
			}
			count++
			if count >= opts.Count {
				return nil
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
