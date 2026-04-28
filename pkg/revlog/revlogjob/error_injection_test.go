// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob_test

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/inmemstorage"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogjob"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// errStorage wraps a cloud.ExternalStorage and lets the test inject
// failures on specific path prefixes. Used to verify the writer
// surfaces — rather than swallows — errors from external storage.
type errStorage struct {
	inner cloud.ExternalStorage

	mu          syncutil.Mutex
	failOnWrite map[string]error
	writeCalls  atomic.Int64
}

func newErrStorage(inner cloud.ExternalStorage) *errStorage {
	return &errStorage{inner: inner, failOnWrite: map[string]error{}}
}

func (s *errStorage) failNextWriteUnder(prefix string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failOnWrite[prefix] = err
}

func (s *errStorage) Conf() cloudpb.ExternalStorage            { return s.inner.Conf() }
func (s *errStorage) ExternalIOConf() base.ExternalIODirConfig { return s.inner.ExternalIOConf() }
func (s *errStorage) Settings() *cluster.Settings              { return s.inner.Settings() }
func (s *errStorage) RequiresExternalIOAccounting() bool {
	return s.inner.RequiresExternalIOAccounting()
}
func (s *errStorage) Close() error                             { return s.inner.Close() }
func (s *errStorage) Delete(c context.Context, b string) error { return s.inner.Delete(c, b) }
func (s *errStorage) Size(c context.Context, b string) (int64, error) {
	return s.inner.Size(c, b)
}
func (s *errStorage) ReadFile(
	c context.Context, b string, opts cloud.ReadOptions,
) (ioctx.ReadCloserCtx, int64, error) {
	return s.inner.ReadFile(c, b, opts)
}
func (s *errStorage) List(
	c context.Context, p string, o cloud.ListOptions, fn cloud.ListingFn,
) error {
	return s.inner.List(c, p, o, fn)
}

func (s *errStorage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	s.writeCalls.Add(1)
	s.mu.Lock()
	for prefix, err := range s.failOnWrite {
		if strings.HasPrefix(basename, prefix) {
			delete(s.failOnWrite, prefix)
			s.mu.Unlock()
			return nil, err
		}
	}
	s.mu.Unlock()
	return s.inner.Writer(ctx, basename)
}

// TestProducerSurfacesStorageErrors verifies that when ExternalStorage
// fails to open a writer for a data file, the producer's
// OnCheckpoint returns a wrapped error rather than swallowing the
// failure or silently dropping the events.
//
// Without this, a transient storage failure would look to the
// orchestration layer like a successful tick close — and the
// associated events would never re-flush because the producer would
// have already moved past them.
func TestProducerSurfacesStorageErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	inner := inmemstorage.New()
	t.Cleanup(func() { _ = inner.Close() })
	es := newErrStorage(inner)
	es.failNextWriteUnder("log/data/", errors.New("injected disk full"))

	d, err := revlogjob.NewDriver(es, []roachpb.Span{allSpan}, ts(100),
		testTickWidth, &seqFileIDs{}, revlogjob.ResumeState{})
	require.NoError(t, err)

	d.OnValue(ctx, roachpb.Key("a"), ts(105), []byte("v"), nil)
	err = d.OnCheckpoint(ctx, allSpan, ts(115))
	require.Error(t, err, "OnCheckpoint should surface the storage error")
	require.ErrorContains(t, err, "injected disk full")
}
