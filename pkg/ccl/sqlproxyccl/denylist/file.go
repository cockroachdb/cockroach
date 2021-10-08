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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
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

// Option allows configuration of a denylist service.
type Option func(*fileOptions)

type fileOptions struct {
	pollingInterval time.Duration
	timeSource      timeutil.TimeSource
}

// newDenylistWithFile returns a new denylist that automatically watches updates to a file.
// Note: this currently does not return an error. This is by design, since even if we trouble
// initiating a denylist with file, we can always update the file with correct content during
// runtime. We don't want sqlproxy fail to start just because there's something wrong with
// contents of a denylist file.
func newDenylistWithFile(
	ctx context.Context, filename string, opts ...Option,
) (*Denylist, chan *Denylist) {
	options := &fileOptions{
		pollingInterval: defaultPollingInterval,
		timeSource:      timeutil.DefaultTimeSource{},
	}
	for _, opt := range opts {
		opt(options)
	}
	ret, err := readDenyList(ctx, options, filename)
	if err != nil {
		// don't return just yet; sqlproxy should be able to carry on without a proper denylist
		// and we still have a chance to recover.
		// TODO(ye): add monitoring for failed updates; we don't want silent failures
		log.Errorf(ctx, "error when reading from file %s: %v", filename, err)

		// Fail open by returning an empty deny list.
		ret = &Denylist{
			entries: make(map[DenyEntity]*DenyEntry),
		}
	}

	return ret, watchForUpdate(ctx, options, filename)
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

var strToTypeMap = map[string]Type{
	"ip":      IPAddrType,
	"cluster": ClusterType,
}

var typeToStrMap = map[Type]string{
	IPAddrType:  "ip",
	ClusterType: "cluster",
}

// UnmarshalYAML implements yaml.Unmarshaler interface for type.
func (typ *Type) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var raw string
	err := unmarshal(&raw)
	if err != nil {
		return err
	}

	normalized := strings.ToLower(raw)
	t, ok := strToTypeMap[normalized]
	if !ok {
		*typ = UnknownType
	} else {
		*typ = t
	}

	return nil
}

// MarshalYAML implements yaml.Marshaler interface for type.
func (typ Type) MarshalYAML() (interface{}, error) {
	return typ.String(), nil
}

// String implements Stringer interface for type.
func (typ Type) String() string {
	s, ok := typeToStrMap[typ]
	if !ok {
		return "UNKNOWN"
	}
	return s
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

func readDenyList(ctx context.Context, options *fileOptions, filename string) (*Denylist, error) {
	handle, err := os.Open(filename)
	if err != nil {
		log.Errorf(ctx, "open file %s: %v", filename, err)
		return nil, err
	}
	defer handle.Close()

	dlf, err := Deserialize(handle)
	if err != nil {
		stat, _ := handle.Stat()
		if stat != nil {
			log.Errorf(ctx, "error updating denylist from file %s modified at %s: %v",
				filename, stat.ModTime(), err)
		} else {
			log.Errorf(ctx, "error updating denylist from file %s: %v",
				filename, err)
		}
		return nil, err
	}
	dl := &Denylist{
		entries:    make(map[DenyEntity]*DenyEntry),
		timeSource: options.timeSource,
	}
	for _, entry := range dlf.Denylist {
		dl.entries[entry.Entity] = entry
	}
	return dl, nil
}

// WatchForUpdates periodically reloads the denylist file. The daemon is
// canceled on ctx cancellation.
func watchForUpdate(ctx context.Context, options *fileOptions, filename string) chan *Denylist {
	result := make(chan *Denylist)
	go func() {
		// TODO(ye): use notification via SIGHUP instead.
		// TODO(ye): use inotify or similar mechanism for watching file updates
		// instead of polling.
		t := timeutil.NewTimer()
		defer t.Stop()
		for {
			t.Reset(options.pollingInterval)
			select {
			case <-ctx.Done():
				close(result)
				log.Errorf(ctx, "WatchList daemon stopped: %v", ctx.Err())
				return
			case <-t.C:
				t.Read = true
				list, err := readDenyList(ctx, options, filename)
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
