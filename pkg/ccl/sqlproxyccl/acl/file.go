// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package acl

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

type FromFile interface {
	AccessController
	yaml.Unmarshaler
}

// newAccessControllerFromFile returns a AccessController and a channel that can be used
// to automatically watch for updates to the controller.
func newAccessControllerFromFile[T FromFile](
	ctx context.Context,
	filename string,
	timeSource timeutil.TimeSource,
	pollingInterval time.Duration,
	errorCount *metric.Gauge,
	postRead func(c T),
) (AccessController, chan AccessController, error) {
	ret, err := readFile[T](ctx, filename, postRead)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error when creating access controller from file %s", filename)
	}

	return ret, watchForUpdate[T](ctx, filename, timeSource, pollingInterval, errorCount, postRead), nil
}

// Deserialize constructs a new T from reader.
// T is expected to be either a DenyList or an AllowList but this is just a utility method
// for unmarshaling YAML from an io.Reader.
func Deserialize[T any](reader io.Reader) (T, error) {
	decoder := yaml.NewDecoder(reader)
	var t T
	err := decoder.Decode(&t)
	if err != nil {
		return *new(T), err
	}
	return t, nil
}

func readFile[T FromFile](ctx context.Context, filename string, postRead func(c T)) (T, error) {
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
	if postRead != nil {
		postRead(f)
	}
	return f, nil
}

// watchForUpdate periodically reloads the access control list file. The daemon is
// canceled on ctx cancellation.
func watchForUpdate[T FromFile](
	ctx context.Context,
	filename string,
	timeSource timeutil.TimeSource,
	pollingInterval time.Duration,
	errorCount *metric.Gauge,
	postRead func(c T),
) chan AccessController {
	result := make(chan AccessController)
	go func() {
		// TODO(ye): use notification via SIGHUP instead.
		// TODO(ye): use inotify or similar mechanism for watching file updates
		// instead of polling.
		t := timeSource.NewTimer()
		defer t.Stop()
		hasError := false
		for {
			t.Reset(pollingInterval)
			select {
			case <-ctx.Done():
				close(result)
				log.Errorf(ctx, "WatchList daemon stopped: %v", ctx.Err())
				return
			case <-t.Ch():
				t.MarkRead()
				list, err := readFile[T](ctx, filename, postRead)
				if err != nil {
					if !hasError && errorCount != nil {
						hasError = true
						errorCount.Inc(1)
					}
					log.Errorf(ctx, "Could not read file %s for %T: %v", filename, list, err)
					continue
				}
				if hasError && errorCount != nil {
					errorCount.Dec(1)
				}
				result <- list
			}
		}
	}()
	return result
}
