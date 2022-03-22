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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// regexpAsString wraps a *regexp.Regexp for better printing and
// JSON unmarshaling.
type regexpAsString struct {
	re *regexp.Regexp
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
	logSpyDefaultCount    = 1000
	logSpyChanCap         = 4096
)

type logSpyOptions struct {
	Count          intAsString
	Grep           regexpAsString
	Flatten        intAsString
	vmoduleOptions `json:",inline"`
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
	if opts.Count == 0 {
		opts.Count = logSpyDefaultCount
	}
	opts.vmoduleOptions.setDefaults(values)
	return opts, nil
}

type logSpy struct {
	vsrv         *vmoduleServer
	setIntercept func(ctx context.Context, f log.Interceptor) func()
}

func (spy *logSpy) handleDebugLogSpy(w http.ResponseWriter, r *http.Request) {
	opts, err := logSpyOptionsFromValues(r.URL.Query())
	if err != nil {
		http.Error(w, "while parsing options: "+err.Error(), http.StatusInternalServerError)
		return
	}

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

	// Note that in the code below, the channel in interceptor.jsonEntries
	// is never closed. This is because we don't know when that is
	// safe. This is sketchy in general but OK here since we don't have
	// to guarantee that the channel is fully consumed.
	interceptor := newLogSpyInterceptor(opts)

	defer func() {
		if dropped := atomic.LoadInt32(&interceptor.countDropped); dropped > 0 {
			entry := log.MakeLegacyEntry(
				ctx, severity.WARNING, channel.DEV,
				0 /* depth */, true, /* redactable */
				"%d messages were dropped", redact.Safe(dropped))
			err = errors.CombineErrors(err, interceptor.outputEntry(w, entry))
		}
	}()

	cleanup := spy.setIntercept(ctx, interceptor)
	defer cleanup()

	// This log message will be served through the interceptor
	// AND it is also reported in other log sinks, so that
	// administrators know the logspy was used.
	log.Infof(ctx, "intercepting logs with options: %+v", opts)

	// Set up the temporary vmodule config, if requested.
	prevVModule := log.GetVModule()
	if opts.hasVModule {
		if err := spy.vsrv.lockVModule(ctx); err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return err
		}

		log.Infof(ctx, "previous vmodule configuration: %s", prevVModule)
		// Install the new configuration.
		if err := log.SetVModule(opts.VModule); err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return err
		}
		log.Infof(ctx, "new vmodule configuration (previous will be restored when logspy session completes): %s", redact.SafeString(opts.VModule))
		defer func() {
			// Restore the configuration.
			err := log.SetVModule(prevVModule)

			// Report the change in logs.
			log.Infof(ctx, "restoring vmodule configuration (%q): %v", redact.SafeString(prevVModule), err)
			spy.vsrv.unlockVModule(ctx)
		}()
	} else {
		log.Infof(ctx, "current vmodule setting: %s", redact.SafeString(prevVModule))
	}

	const flushInterval = time.Second
	var flushTimer timeutil.Timer
	defer flushTimer.Stop()
	flushTimer.Reset(flushInterval)

	numReportedEntries := 0
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				// Common case: timeout after the configured duration.
				return nil
			}
			return err

		case jsonEntry := <-interceptor.jsonEntries:
			if err := interceptor.outputJSONEntry(w, jsonEntry); err != nil {
				return errors.Wrapf(err, "while writing entry %s", jsonEntry)
			}
			numReportedEntries++
			if numReportedEntries >= int(opts.Count) {
				return nil
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

type logSpyInterceptor struct {
	opts         logSpyOptions
	countDropped int32
	jsonEntries  chan []byte
}

func newLogSpyInterceptor(opts logSpyOptions) *logSpyInterceptor {
	return &logSpyInterceptor{
		opts:        opts,
		jsonEntries: make(chan []byte, logSpyChanCap),
	}
}

func (i *logSpyInterceptor) Intercept(jsonEntry []byte) {
	if re := i.opts.Grep.re; re != nil {
		switch {
		case re.Match(jsonEntry):
		default:
			return
		}
	}

	// The log.Interceptor interface requires us to copy the buffer
	// before we can send it to a different goroutine.
	jsonCopy := make([]byte, len(jsonEntry))
	copy(jsonCopy, jsonEntry)

	select {
	case i.jsonEntries <- jsonCopy:
	default:
		// Consumer fell behind, just drop the message.
		atomic.AddInt32(&i.countDropped, 1)
	}
}

func (i *logSpyInterceptor) outputEntry(w io.Writer, entry logpb.Entry) error {
	if i.opts.Flatten > 0 {
		return log.FormatLegacyEntry(entry, w)
	}
	j, _ := json.Marshal(entry)
	return i.outputJSONEntry(w, j)
}

func (i *logSpyInterceptor) outputJSONEntry(w io.Writer, jsonEntry []byte) error {
	if i.opts.Flatten == 0 {
		_, err1 := w.Write(jsonEntry)
		_, err2 := w.Write([]byte("\n"))
		return errors.CombineErrors(err1, err2)
	}
	var legacyEntry logpb.Entry
	if err := json.Unmarshal(jsonEntry, &legacyEntry); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "interceptor API does not seem to provide valid Entry payloads")
	}
	return i.outputEntry(w, legacyEntry)
}
