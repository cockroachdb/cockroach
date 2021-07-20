// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package denylist

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"gopkg.in/yaml.v2"
)

const (
	defaultPollingInterval   = time.Minute
	defaultEmptyDenylistText = "SequenceNumber: 0"
)

// File represents a on-disk version of the denylist config.
// This also serves as a spec of expected yaml file format.
type File struct {
	Seq      int64        `yaml:"SequenceNumber"`
	Denylist []*DenyEntry `yaml:"denylist"`
}

// Deserialize constructs a new DenylistFile from reader.
func Deserialize(reader io.Reader) (*File, error) {
	decoder := yaml.NewDecoder(reader)
	var denylistFile File
	err := decoder.Decode(&denylistFile)
	if err != nil {
		return nil, err
	}
	return &denylistFile, nil
}

// Serialize a File into raw bytes.
func (dlf *File) Serialize() ([]byte, error) {
	return yaml.Marshal(dlf)
}

// DenyEntry records info about one denied entity,
// the reason and the expiration time.
// This also serves as spec for the yaml config format.
type DenyEntry struct {
	Entity     DenyEntity `yaml:"entity"`
	Expiration time.Time  `yaml:"expiration"`
	Reason     string     `yaml:"reason"`
}

// Denylist represents an in-memory cache for the current denylist.
// It also handles the logic of deciding what to be denied.
type Denylist struct {
	mu struct {
		entries map[DenyEntity]*DenyEntry
		*syncutil.RWMutex
	}
	pollingInterval time.Duration
	timeSource      timeutil.TimeSource

	ctx context.Context
}

// NewDenylistWithFile returns a new denylist that automatically watches updates to a file.
// Note: this currently does not return an error. This is by design, since even if we trouble
// initiating a denylist with file, we can always update the file with correct content during
// runtime. We don't want sqlproxy fail to start just because there's something wrong with
// contents of a denylist file.
func NewDenylistWithFile(ctx context.Context, filename string, opts ...Option) *Denylist {
	ret := &Denylist{
		pollingInterval: defaultPollingInterval,
		timeSource:      timeutil.DefaultTimeSource{},
		ctx:             ctx,
	}
	ret.mu.entries = make(map[DenyEntity]*DenyEntry)
	ret.mu.RWMutex = &syncutil.RWMutex{}

	for _, opt := range opts {
		opt(ret)
	}
	err := ret.update(filename)
	if err != nil {
		// don't return just yet; sqlproxy should be able to carry on without a proper denylist
		// and we still have a chance to recover.
		// TODO(ye): add monitoring for failed updates; we don't want silent failures
		log.Errorf(ctx, "error when reading from file %s: %v", filename, err)
	}

	ret.watchForUpdate(filename)

	return ret
}

// Option allows configuration of a denylist service.
type Option func(*Denylist)

// WithPollingInterval specifies interval between polling for config file changes.
func WithPollingInterval(d time.Duration) Option {
	return func(dl *Denylist) {
		dl.pollingInterval = d
	}
}

// update the Denylist with content of the file.
func (dl *Denylist) update(filename string) error {
	handle, err := os.Open(filename)
	if err != nil {
		log.Errorf(dl.ctx, "open file %s: %v", filename, err)
		return err
	}
	defer handle.Close()

	dlf, err := Deserialize(handle)
	if err != nil {
		stat, _ := handle.Stat()
		if stat != nil {
			log.Errorf(dl.ctx, "error updating denylist from file %s modified at %s: %v",
				filename, stat.ModTime(), err)
		} else {
			log.Errorf(dl.ctx, "error updating denylist from file %s: %v",
				filename, err)
		}
		return err
	}
	dl.updateWithDenylistFile(dlf)
	return nil
}

func (dl *Denylist) updateWithDenylistFile(dlf *File) {
	newEntries := make(map[DenyEntity]*DenyEntry)
	for _, entry := range dlf.Denylist {
		newEntries[entry.Entity] = entry
	}

	dl.mu.Lock()
	defer dl.mu.Unlock()

	dl.mu.entries = newEntries
}

// Denied implements the Service interface.
func (dl *Denylist) Denied(entity DenyEntity) (*Entry, error) {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if ent, ok := dl.mu.entries[entity]; ok && !ent.Expiration.Before(dl.timeSource.Now()) {
		return &Entry{ent.Reason}, nil
	}
	return nil, nil
}

// WatchForUpdates periodically reloads the denylist file. The daemon is
// canceled on ctx cancellation.
func (dl *Denylist) watchForUpdate(filename string) {
	go func() {
		// TODO(ye): use notification via SIGHUP instead.
		// TODO(ye): use inotify or similar mechanism for watching file updates instead of polling.
		t := timeutil.NewTimer()
		defer t.Stop()
		for {
			t.Reset(dl.pollingInterval)
			select {
			case <-dl.ctx.Done():
				log.Errorf(dl.ctx, "WatchList daemon stopped: %v", dl.ctx.Err())
				return
			case <-t.C:
				t.Read = true
				err := dl.update(filename)
				if err != nil {
					// TODO(ye): add monitoring for update failures.
					continue
				}
			}
		}
	}()
}
