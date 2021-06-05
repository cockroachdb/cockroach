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
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

type vmoduleOptions struct {
	hasSet   bool
	Set      string
	Duration durationAsString
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

	if opts.Duration == 0 {
		opts.Duration = logSpyDefaultDuration * 2
	}

	_, opts.hasSet = values["set"]

	return opts, nil
}

func vmoduleHandleDebug(w http.ResponseWriter, r *http.Request) {
	opts, err := loadVmoduleOptionsFromValues(r.URL.Query())
	if err != nil {
		http.Error(w, "while parsing options: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-type", "text/plain; charset=UTF-8")
	ctx := r.Context()
	if err := vmoduleHandleDebugInternal(ctx, w, opts); err != nil {
		// This is likely a broken HTTP connection, so nothing too unexpected.
		log.Infof(ctx, "%v", err)
	}
}

func vmoduleHandleDebugInternal(
	ctx context.Context, w http.ResponseWriter, opts vmoduleOptions,
) error {
	prevSettings := log.GetVModule()

	_, err := w.Write([]byte("previous configuration: " + prevSettings + "\n"))
	if err != nil {
		return err
	}
	if !opts.hasSet {
		// Only retrieving the current options; nothing else to do.
		return nil
	}

	// Install the new configuration.
	if err := log.SetVModule(opts.Set); err != nil {
		http.Error(w, "setting vmodule: "+err.Error(), http.StatusInternalServerError)
		return nil
	}

	// Inform the HTTP client of the new config.
	_, err = w.Write([]byte("new configuration: " + opts.Set + "\n"))
	if err != nil {
		return err
	}

	// Report the change in logs.
	log.Infof(ctx, "configured vmodule: %q", redact.SafeString(opts.Set))

	if opts.Duration <= 0 {
		// The user did not request to restore the config after
		// a delay. nothing else to do.
		return nil
	}

	// Inform the HTTP client of the delayed restore.
	_, err = w.Write([]byte(fmt.Sprintf("will restore previous config after %s\n", opts.Duration)))
	if err != nil {
		return err
	}

	go func() {
		// Wait for the configured duration.
		time.Sleep(time.Duration(opts.Duration))

		// Restore the configuration.
		err := log.SetVModule(prevSettings)

		// Report the change in logs.
		log.Infof(context.Background(), "restoring vmodule (%q): %v", redact.SafeString(prevSettings), err)
	}()

	return nil
}
