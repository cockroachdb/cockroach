// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package acl

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"gopkg.in/yaml.v2"
)

const (
	defaultPollingInterval = time.Minute
)

type FromFile interface {
	AccessController
	yaml.Unmarshaler
}

// Option allows configuration of an access control list service.
type Option func(*fileOptions)

type fileOptions struct {
	pollingInterval time.Duration
	timeSource      timeutil.TimeSource
}

// newAccessControllerFromFile returns a AccessController that automatically watches updates to a file.
// Note: this currently does not return an error. This is by design, since even if we trouble
// initiating an access controller with file, we can always update the file with correct content during
// runtime. We don't want sqlproxy fail to start just because there's something wrong with
// contents of an access control file.
// TODO(pj): Does the above still apply to the AllowList? Or was that specific to the DenyList?
func newAccessControllerFromFile[T FromFile](
	ctx context.Context, filename string, pollingInterval time.Duration,
) (AccessController, chan AccessController) {
	ret, err := readFile[T](ctx, filename)
	if err != nil {
		// don't return just yet; sqlproxy should be able to carry on without a proper access controller
		// and we still have a chance to recover.
		// TODO(ye): add monitoring for failed updates; we don't want silent failures
		log.Errorf(ctx, "error when reading from file %s: %v", filename, err)
	}

	return ret, watchForUpdate[T](ctx, filename, pollingInterval)
}

// Deserialize constructs a new T from reader.
// T is expected to be either a DenyList or an AllowList
// but this is just a utility method for unmarshaling YAML
// from an io.Reader.
func Deserialize[T any](reader io.Reader) (T, error) {
	decoder := yaml.NewDecoder(reader)
	var t T
	err := decoder.Decode(&t)
	if err != nil {
		return *new(T), err
	}
	return t, nil
}

// WithPollingInterval specifies interval between polling for config file
// changes.
func WithPollingInterval(d time.Duration) Option {
	return func(op *fileOptions) {
		op.pollingInterval = d
	}
}

// WithTimeSource overrides the time source used to check expiration times.
func WithTimeSource(t timeutil.TimeSource) Option {
	return func(op *fileOptions) {
		op.timeSource = t
	}
}

func readFile[T FromFile](ctx context.Context, filename string) (T, error) {
	handle, err := os.Open(filename)
	if err != nil {
		log.Errorf(ctx, "open file %s: %v", filename, err)
		return *new(T), err
	}
	defer handle.Close()

	f, err := Deserialize[T](handle)
	if err != nil {
		stat, _ := handle.Stat()
		if stat != nil {
			log.Errorf(ctx, "error updating access control list from file %s modified at %s: %v",
				filename, stat.ModTime(), err)
		} else {
			log.Errorf(ctx, "error updating access control list from file %s: %v",
				filename, err)
		}
		return *new(T), err
	}
	return f, nil
}

// WatchForUpdates periodically reloads the access control list file. The daemon is
// canceled on ctx cancellation.
func watchForUpdate[T FromFile](
	ctx context.Context, filename string, pollingInterval time.Duration,
) chan AccessController {
	result := make(chan AccessController)
	go func() {
		// TODO(ye): use notification via SIGHUP instead.
		// TODO(ye): use inotify or similar mechanism for watching file updates
		// instead of polling.
		t := timeutil.NewTimer()
		defer t.Stop()
		for {
			t.Reset(pollingInterval)
			select {
			case <-ctx.Done():
				close(result)
				log.Errorf(ctx, "WatchList daemon stopped: %v", ctx.Err())
				return
			case <-t.C:
				t.Read = true
				list, err := readFile[T](ctx, filename)
				if err != nil {
					// TODO(ye): add monitoring for update failures.
					continue
				}
				result <- list
			}
		}
	}()
	return result
}
