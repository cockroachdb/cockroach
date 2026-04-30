// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package inmemstorage provides a minimal, purely in-memory implementation of
// cloud.ExternalStorage intended for use in unit tests.
//
// The implementation is backed by a map of name to *bytes.Buffer guarded by a
// mutex. It intentionally drags in none of the production wiring that
// nodelocal:// or userfile:// implementations require: there are no cluster
// settings hooks, no metrics, no IO accounting, no real filesystem or KV
// access. It is constructed directly via New rather than via the URI factory,
// because it is not appropriate as a user-visible storage provider.
//
// Use this when a test needs to round-trip bytes through the
// cloud.ExternalStorage interface and does not care about anything else (the
// canonical example is exercising code that writes and reads files via the
// interface). For tests that need to exercise filesystem or KV semantics --
// including any code path that depends on settings, metrics or accounting --
// use a real implementation instead.
package inmemstorage

import (
	"bytes"
	"context"
	"io"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Option configures a Storage at construction time. Use the With* functions to
// build one.
type Option func(*options)

type options struct {
	settings *cluster.Settings
	initial  map[string][]byte
}

// WithSettings provides the *cluster.Settings that the storage's Settings()
// method will return. If unset, MakeTestingClusterSettings is used.
func WithSettings(st *cluster.Settings) Option {
	return func(o *options) { o.settings = st }
}

// WithInitialContents pre-populates the storage with the provided entries.
// The byte slices are copied; the caller retains ownership.
func WithInitialContents(contents map[string][]byte) Option {
	return func(o *options) { o.initial = contents }
}

// New returns a new in-memory cloud.ExternalStorage. See the package doc for
// the intended use cases and limitations.
func New(opts ...Option) cloud.ExternalStorage {
	o := options{}
	for _, fn := range opts {
		fn(&o)
	}
	if o.settings == nil {
		o.settings = cluster.MakeTestingClusterSettings()
	}
	s := &Storage{settings: o.settings, files: make(map[string]*bytes.Buffer, len(o.initial))}
	for name, data := range o.initial {
		// Copy the input so that subsequent mutations by the caller don't bleed
		// into the storage.
		buf := bytes.NewBuffer(append([]byte(nil), data...))
		s.files[name] = buf
	}
	return s
}

// Storage is an in-memory cloud.ExternalStorage. Construct it via New; the
// zero value is not usable.
type Storage struct {
	settings *cluster.Settings

	mu syncutil.Mutex
	// files holds the current contents of the storage, keyed by name. Each
	// entry is a snapshot of the bytes installed by the most recent successful
	// Writer.Close call (or by WithInitialContents). The buffer pointer for a
	// given key is replaced atomically on overwrite so that an outstanding
	// reader continues to see the bytes it observed at ReadFile time.
	files map[string]*bytes.Buffer
}

var _ cloud.ExternalStorage = (*Storage)(nil)

// Conf returns a sentinel cloudpb.ExternalStorage. Callers should not depend
// on its contents: this implementation is not registered with the URI factory
// and the returned value cannot be used to reconstruct it.
func (s *Storage) Conf() cloudpb.ExternalStorage {
	return cloudpb.ExternalStorage{Provider: cloudpb.ExternalStorageProvider_Unknown}
}

// ExternalIOConf returns a zero-value config; this implementation has no IO
// configuration of its own.
func (s *Storage) ExternalIOConf() base.ExternalIODirConfig {
	return base.ExternalIODirConfig{}
}

// RequiresExternalIOAccounting always returns false: in-memory test storage
// is not subject to external IO resource accounting.
func (s *Storage) RequiresExternalIOAccounting() bool { return false }

// Settings returns the cluster settings provided at construction time, or a
// freshly minted testing settings instance if none were provided.
func (s *Storage) Settings() *cluster.Settings { return s.settings }

// ReadFile returns a reader over the named file's bytes, honoring opts.Offset
// and opts.LengthHint. If the file does not exist, cloud.ErrFileDoesNotExist
// (wrapped) is returned.
func (s *Storage) ReadFile(
	_ context.Context, basename string, opts cloud.ReadOptions,
) (ioctx.ReadCloserCtx, int64, error) {
	s.mu.Lock()
	buf, ok := s.files[basename]
	s.mu.Unlock()
	if !ok {
		return nil, 0, cloud.WrapErrFileDoesNotExist(
			errors.Newf("%q", basename), "inmemstorage")
	}

	// Snapshot the bytes so that a subsequent Writer.Close that replaces the
	// map entry (or a Delete) doesn't change what this reader sees.
	data := buf.Bytes()
	size := int64(len(data))

	if opts.Offset > size {
		return nil, 0, errors.Errorf(
			"inmemstorage: offset %d past end of file %q (size %d)", opts.Offset, basename, size)
	}
	data = data[opts.Offset:]
	if opts.LengthHint > 0 && opts.LengthHint < int64(len(data)) {
		// LengthHint is a hint, not a hard limit, but trimming up-front matches
		// the spirit of the hint and helps tests assert on it.
		data = data[:opts.LengthHint]
	}

	r := ioctx.ReadCloserAdapter(io.NopCloser(bytes.NewReader(data)))
	if opts.NoFileSize {
		return r, 0, nil
	}
	return r, size, nil
}

// Writer returns an io.WriteCloser that buffers writes in memory. On Close,
// the buffer is installed as the new contents of basename, atomically
// replacing any prior value. Subsequent Close calls return an error.
func (s *Storage) Writer(_ context.Context, basename string) (io.WriteCloser, error) {
	return &writer{storage: s, name: basename, buf: &bytes.Buffer{}}, nil
}

// matchingNames returns the set of stored names that begin with prefix. The
// returned slice is owned by the caller; the lock is released before
// returning.
func (s *Storage) matchingNames(prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.files))
	for name := range s.files {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	return names
}

// List enumerates entries whose names start with prefix, applying
// opts.Delimiter grouping and opts.AfterKey filtering as documented on
// cloud.ListOptions. Results are emitted in lexicographic order. Iteration
// stops without error if fn returns cloud.ErrListingDone; any other error
// returned by fn is propagated.
func (s *Storage) List(
	_ context.Context, prefix string, opts cloud.ListOptions, fn cloud.ListingFn,
) error {
	names := s.matchingNames(prefix)
	sort.Strings(names)

	// AfterKey is a full key relative to the storage's base prefix. This
	// implementation has no base prefix, so we compare against the full name
	// directly. See cloud.ListOptions for the precise semantics.
	afterKey := opts.AfterKey

	var prevPrefix string
	for _, name := range names {
		rel := strings.TrimPrefix(name, prefix)
		if opts.Delimiter != "" {
			if i := strings.Index(rel, opts.Delimiter); i >= 0 {
				rel = rel[:i+len(opts.Delimiter)]
			}
			if rel == prevPrefix {
				continue
			}
			prevPrefix = rel
		}

		// AfterKey applies after delimiter grouping, and is compared against the
		// full key (prefix + relative).
		if prefix+rel <= afterKey {
			continue
		}

		if err := fn(rel); err != nil {
			if errors.Is(err, cloud.ErrListingDone) {
				return nil
			}
			return err
		}
	}
	return nil
}

// Delete removes the named entry. It is a no-op (returns nil) if the entry
// does not exist, matching the cloud.ExternalStorage contract.
func (s *Storage) Delete(_ context.Context, basename string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.files, basename)
	return nil
}

// Size returns the number of bytes currently stored under basename, or
// cloud.ErrFileDoesNotExist if there is no such entry.
func (s *Storage) Size(_ context.Context, basename string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	buf, ok := s.files[basename]
	if !ok {
		return 0, cloud.WrapErrFileDoesNotExist(
			errors.Newf("%q", basename), "inmemstorage")
	}
	return int64(buf.Len()), nil
}

// Close is a no-op.
func (s *Storage) Close() error { return nil }

// writer is the io.WriteCloser returned by Storage.Writer. It buffers writes
// in memory and installs the accumulated bytes into the parent Storage on
// Close. The writer is single-use: subsequent Close calls return an error.
type writer struct {
	storage *Storage
	name    string
	buf     *bytes.Buffer
	closed  bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("inmemstorage: write to closed writer")
	}
	return w.buf.Write(p)
}

func (w *writer) Close() error {
	if w.closed {
		return errors.New("inmemstorage: writer already closed")
	}
	w.closed = true
	w.storage.mu.Lock()
	defer w.storage.mu.Unlock()
	// Install a fresh buffer so that any in-flight reader holding the prior
	// buffer pointer is unaffected by subsequent overwrites.
	w.storage.files[w.name] = w.buf
	return nil
}
