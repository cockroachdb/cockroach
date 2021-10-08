// Copyright 2021 The Cockroach Authors.
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
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type vmoduleOptions struct {
	hasVModule bool
	VModule    string
	Duration   durationAsString
}

func loadVmoduleOptionsFromValues(values url.Values) (vmoduleOptions, error) {
	rawValues := map[string]string{}
	for k, vals := range values {
		if len(vals) > 0 {
			rawValues[k] = vals[0]
		}
	}
	data, err := json.Marshal(rawValues)
	if err != nil {
		return vmoduleOptions{}, err
	}
	var opts vmoduleOptions
	if err := json.Unmarshal(data, &opts); err != nil {
		return vmoduleOptions{}, err
	}

	opts.setDefaults(values)
	return opts, nil
}

func (opts *vmoduleOptions) setDefaults(values url.Values) {
	if opts.Duration == 0 {
		opts.Duration = logSpyDefaultDuration
	}

	_, opts.hasVModule = values["vmodule"]
}

type vmoduleServer struct {
	lock uint32
}

func (s *vmoduleServer) lockVModule(ctx context.Context) error {
	if swapped := atomic.CompareAndSwapUint32(&s.lock, 0, 1); !swapped {
		return errors.New("another in-flight HTTP request is already managing vmodule")
	}
	return nil
}

func (s *vmoduleServer) unlockVModule(ctx context.Context) {
	atomic.StoreUint32(&s.lock, 0)
}

func (s *vmoduleServer) vmoduleHandleDebug(w http.ResponseWriter, r *http.Request) {
	opts, err := loadVmoduleOptionsFromValues(r.URL.Query())
	if err != nil {
		http.Error(w, "while parsing options: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-type", "text/plain; charset=UTF-8")
	ctx := r.Context()
	if err := s.vmoduleHandleDebugInternal(ctx, w, opts); err != nil {
		// This is likely a broken HTTP connection, so nothing too unexpected.
		log.Infof(ctx, "%v", err)
	}
}

func (s *vmoduleServer) vmoduleHandleDebugInternal(
	ctx context.Context, w http.ResponseWriter, opts vmoduleOptions,
) error {
	prevSettings := log.GetVModule()

	_, err := w.Write([]byte("previous vmodule configuration: " + prevSettings + "\n"))
	if err != nil {
		return err
	}
	if !opts.hasVModule {
		// Only retrieving the current options; nothing else to do.
		return nil
	}

	// If we are going to tweak the vmodule setting, ensure we have only
	// one such request in-flight.
	if err := s.lockVModule(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return nil //nolint:returnerrcheck
	}

	// Install the new configuration.
	if err := log.SetVModule(opts.VModule); err != nil {
		s.unlockVModule(ctx)
		http.Error(w, "setting vmodule: "+err.Error(), http.StatusInternalServerError)
		return nil //nolint:returnerrcheck
	}

	// Inform the HTTP client of the new config.
	_, err = w.Write([]byte("new vmodule configuration: " + opts.VModule + "\n"))
	if err != nil {
		s.unlockVModule(ctx)
		return err
	}

	// Report the change in logs.
	log.Infof(ctx, "configured vmodule: %q", redact.SafeString(opts.VModule))

	if opts.Duration <= 0 {
		s.unlockVModule(ctx)
		// The user did not request to restore the config after
		// a delay. nothing else to do.
		return nil
	}

	// Inform the HTTP client of the delayed restore.
	_, err = w.Write([]byte(fmt.Sprintf("will restore previous vmodule config after %s\n", opts.Duration)))
	if err != nil {
		s.unlockVModule(ctx)
		return err
	}

	go func() {
		// Wait for the configured duration.
		time.Sleep(time.Duration(opts.Duration))

		// Restore the configuration.
		err := log.SetVModule(prevSettings)
		// Report the change in logs.
		log.Infof(context.Background(), "restoring vmodule configuration (%q): %v", redact.SafeString(prevSettings), err)

		s.unlockVModule(context.Background())
	}()

	return nil
}
