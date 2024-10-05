// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	// tenantIDFilter filters entries based on the provided tenant ID.
	// If empty, no filtering is applied (only relevant for the system
	// tenant).
	tenantIDFilter string
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
	// tenantID is the tenantID assigned to the server that's servicing this
	// logSpy. If the value is any tenant other than the System tenant, we
	// filter logs to only include those specific to this tenantID. For the
	// system tenant however, we perform no such filtering.
	tenantID     roachpb.TenantID
	vsrv         *vmoduleServer
	setIntercept func(ctx context.Context, f log.Interceptor) func()
}

func (spy *logSpy) handleDebugLogSpy(w http.ResponseWriter, r *http.Request) {
	opts, err := logSpyOptionsFromValues(r.URL.Query())
	if err != nil {
		http.Error(w, "while parsing options: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if spy.tenantID != roachpb.SystemTenantID {
		opts.tenantIDFilter = spy.tenantID.String()
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
	interceptor := newLogSpyInterceptor(ctx, opts)

	defer func() {
		if dropped := atomic.LoadInt32(&interceptor.countDropped); dropped > 0 {
			entry := log.MakeLegacyEntry(
				ctx, severity.WARNING, channel.DEV,
				0 /* depth */, true, /* redactable */
				"%d messages were dropped", redact.Safe(dropped))
			entryBytes, _ := json.Marshal(entry)
			err = errors.CombineErrors(err, interceptor.outputEntry(w, logSpyInterceptorPayload{
				entryBytes: entryBytes,
				entry:      entry,
			}))
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

		case entryPayload := <-interceptor.jsonEntries:
			if entryPayload.err != nil {
				return err
			}
			if err := interceptor.outputEntry(w, entryPayload); err != nil {
				return errors.Wrapf(err, "while writing entry %s", entryPayload.entryBytes)
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
	ctx          context.Context
	opts         logSpyOptions
	countDropped int32
	jsonEntries  chan logSpyInterceptorPayload
}

// logSpyInterceptorPayload exists for convenience. When we first
// intercept, we need to perform some basic filtering that requires
// us to unmarshall the entryBytes. Instead of performing this
// operation multiple times, we store the entry in the payload
// for reuse.
type logSpyInterceptorPayload struct {
	// The raw log entry bytes.
	entryBytes []byte
	// The unmarshalled result from entryBytes.
	entry logpb.Entry
	// If an error occurred during the intercept, we place it here
	// to be processed by the logSpy consumer.
	err error
}

func newLogSpyInterceptor(ctx context.Context, opts logSpyOptions) *logSpyInterceptor {
	return &logSpyInterceptor{
		ctx:         ctx,
		opts:        opts,
		jsonEntries: make(chan logSpyInterceptorPayload, logSpyChanCap),
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
	var entry logpb.Entry
	if err := json.Unmarshal(jsonEntry, &entry); err != nil {
		// If we can't unmarshal the entry, send an error along to be handled by the consumer and return.
		select {
		case i.jsonEntries <- logSpyInterceptorPayload{
			err: errors.Wrapf(err, "logspy failed to unmarshal entry: %s", jsonEntry),
		}:
		default:
			// Consumer fell behind, just drop the message.
			atomic.AddInt32(&i.countDropped, 1)
		}
		return
	}
	if i.opts.tenantIDFilter != "" && i.opts.tenantIDFilter != entry.TenantID {
		return
	}

	// The log.Interceptor interface requires us to copy the buffer
	// before we can send it to a different goroutine.
	jsonCopy := make([]byte, len(jsonEntry))
	copy(jsonCopy, jsonEntry)

	select {
	case i.jsonEntries <- logSpyInterceptorPayload{
		entryBytes: jsonCopy,
		entry:      entry,
	}:
	default:
		// Consumer fell behind, just drop the message.
		atomic.AddInt32(&i.countDropped, 1)
	}
}

func (i *logSpyInterceptor) outputEntry(w io.Writer, entry logSpyInterceptorPayload) error {
	if i.opts.Flatten == 0 {
		_, err1 := w.Write(entry.entryBytes)
		_, err2 := w.Write([]byte("\n"))
		return errors.CombineErrors(err1, err2)
	}
	return log.FormatLegacyEntry(entry.entry, w)
}
