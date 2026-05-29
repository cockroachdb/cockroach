// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inmemstorage_test

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/inmemstorage"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// writeFile is a small helper that writes the provided bytes to the given
// storage via the ExternalStorage interface.
func writeFile(
	t *testing.T, ctx context.Context, s cloud.ExternalStorage, name string, data []byte,
) {
	t.Helper()
	require.NoError(t, cloud.WriteFile(ctx, s, name, bytes.NewReader(data)))
}

// readAll reads the full contents of the named file via ReadFile and returns
// the raw bytes plus the reported size.
func readAll(
	t *testing.T, ctx context.Context, s cloud.ExternalStorage, name string, opts cloud.ReadOptions,
) ([]byte, int64) {
	t.Helper()
	r, sz, err := s.ReadFile(ctx, name, opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close(ctx)) }()
	data, err := ioctx.ReadAll(ctx, r)
	require.NoError(t, err)
	return data, sz
}

func TestRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := inmemstorage.New()
	defer func() { require.NoError(t, s.Close()) }()

	writeFile(t, ctx, s, "hello", []byte("world"))
	data, sz := readAll(t, ctx, s, "hello", cloud.ReadOptions{})
	require.Equal(t, []byte("world"), data)
	require.Equal(t, int64(5), sz)

	got, err := s.Size(ctx, "hello")
	require.NoError(t, err)
	require.Equal(t, int64(5), got)
}

func TestReadMissing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := inmemstorage.New()
	defer func() { require.NoError(t, s.Close()) }()

	_, _, err := s.ReadFile(ctx, "nope", cloud.ReadOptions{})
	require.True(t, errors.Is(err, cloud.ErrFileDoesNotExist), "got %v", err)

	_, err = s.Size(ctx, "nope")
	require.True(t, errors.Is(err, cloud.ErrFileDoesNotExist), "got %v", err)
}

func TestDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := inmemstorage.New()
	defer func() { require.NoError(t, s.Close()) }()

	writeFile(t, ctx, s, "a", []byte("contents"))
	require.NoError(t, s.Delete(ctx, "a"))

	_, _, err := s.ReadFile(ctx, "a", cloud.ReadOptions{})
	require.True(t, errors.Is(err, cloud.ErrFileDoesNotExist))

	// Deleting a non-existent entry is a no-op.
	require.NoError(t, s.Delete(ctx, "never-existed"))
}

func TestReadOffsetAndLengthHint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := inmemstorage.New()
	defer func() { require.NoError(t, s.Close()) }()

	writeFile(t, ctx, s, "f", []byte("0123456789"))

	tests := []struct {
		name          string
		opts          cloud.ReadOptions
		expectedBytes []byte
		expectedSize  int64
	}{
		{name: "no offset", opts: cloud.ReadOptions{}, expectedBytes: []byte("0123456789"), expectedSize: 10},
		{name: "offset only", opts: cloud.ReadOptions{Offset: 3}, expectedBytes: []byte("3456789"), expectedSize: 10},
		{name: "length hint only", opts: cloud.ReadOptions{LengthHint: 4}, expectedBytes: []byte("0123"), expectedSize: 10},
		{name: "offset and hint", opts: cloud.ReadOptions{Offset: 2, LengthHint: 3}, expectedBytes: []byte("234"), expectedSize: 10},
		{name: "offset at end", opts: cloud.ReadOptions{Offset: 10}, expectedBytes: []byte{}, expectedSize: 10},
		{name: "no file size", opts: cloud.ReadOptions{NoFileSize: true}, expectedBytes: []byte("0123456789"), expectedSize: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, sz := readAll(t, ctx, s, "f", tc.opts)
			require.Equal(t, tc.expectedBytes, data)
			require.Equal(t, tc.expectedSize, sz)
		})
	}
}

func TestReadOffsetPastEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := inmemstorage.New()
	defer func() { require.NoError(t, s.Close()) }()

	writeFile(t, ctx, s, "f", []byte("abc"))
	_, _, err := s.ReadFile(ctx, "f", cloud.ReadOptions{Offset: 100})
	require.Error(t, err)
}

func TestListBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := inmemstorage.New()
	defer func() { require.NoError(t, s.Close()) }()

	for _, name := range []string{"a/1", "a/2", "a/3", "b/1", "b/2", "c/1"} {
		writeFile(t, ctx, s, name, []byte(name))
	}

	got := list(t, ctx, s, "a/", cloud.ListOptions{})
	require.Equal(t, []string{"1", "2", "3"}, got)

	got = list(t, ctx, s, "b/", cloud.ListOptions{})
	require.Equal(t, []string{"1", "2"}, got)

	got = list(t, ctx, s, "", cloud.ListOptions{})
	require.Equal(t, []string{"a/1", "a/2", "a/3", "b/1", "b/2", "c/1"}, got)
}

func TestListWithDelimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := inmemstorage.New()
	defer func() { require.NoError(t, s.Close()) }()

	for _, name := range []string{"a/1", "a/2", "b/1", "b/2/x", "c"} {
		writeFile(t, ctx, s, name, nil)
	}

	got := list(t, ctx, s, "", cloud.ListOptions{Delimiter: "/"})
	require.Equal(t, []string{"a/", "b/", "c"}, got)
}

func TestListWithAfterKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := inmemstorage.New()
	defer func() { require.NoError(t, s.Close()) }()

	for _, name := range []string{"d/a", "d/b", "d/c", "d/d"} {
		writeFile(t, ctx, s, name, nil)
	}

	// AfterKey is the full key. Strictly greater than "d/b" yields "c", "d".
	got := list(t, ctx, s, "d/", cloud.ListOptions{AfterKey: "d/b"})
	require.Equal(t, []string{"c", "d"}, got)
}

func TestListEarlyExit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := inmemstorage.New()
	defer func() { require.NoError(t, s.Close()) }()

	for _, name := range []string{"x/1", "x/2", "x/3"} {
		writeFile(t, ctx, s, name, nil)
	}

	var visited []string
	err := s.List(ctx, "x/", cloud.ListOptions{}, func(name string) error {
		visited = append(visited, name)
		if name == "2" {
			return cloud.ErrListingDone
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2"}, visited)
}

func TestListPropagatesError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := inmemstorage.New()
	defer func() { require.NoError(t, s.Close()) }()

	writeFile(t, ctx, s, "x/1", nil)
	sentinel := errors.New("boom")
	err := s.List(ctx, "x/", cloud.ListOptions{}, func(string) error { return sentinel })
	require.True(t, errors.Is(err, sentinel))
}

func TestWriterReplacesContents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := inmemstorage.New()
	defer func() { require.NoError(t, s.Close()) }()

	writeFile(t, ctx, s, "k", []byte("first"))
	writeFile(t, ctx, s, "k", []byte("second"))

	data, _ := readAll(t, ctx, s, "k", cloud.ReadOptions{})
	require.Equal(t, []byte("second"), data)
}

// TestReaderUnaffectedByConcurrentOverwrite verifies the documented
// invariant that a reader returned by ReadFile observes a stable snapshot of
// the file's contents even if the file is overwritten or deleted before the
// reader is drained.
func TestReaderUnaffectedByConcurrentOverwrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := inmemstorage.New()
	defer func() { require.NoError(t, s.Close()) }()

	writeFile(t, ctx, s, "k", []byte("original"))

	r, _, err := s.ReadFile(ctx, "k", cloud.ReadOptions{})
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close(ctx)) }()

	// Replace and then delete before reading.
	writeFile(t, ctx, s, "k", []byte("replaced"))
	require.NoError(t, s.Delete(ctx, "k"))

	data, err := ioctx.ReadAll(ctx, r)
	require.NoError(t, err)
	require.Equal(t, []byte("original"), data)
}

func TestWriterCloseIdempotence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := inmemstorage.New()
	defer func() { require.NoError(t, s.Close()) }()

	w, err := s.Writer(ctx, "k")
	require.NoError(t, err)
	_, err = w.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	// The contract: Close is single-shot. A second Close is an error so callers
	// notice double-close bugs.
	require.Error(t, w.Close())
	// Subsequent writes also fail.
	_, err = w.Write([]byte("more"))
	require.Error(t, err)
}

// TestConcurrencySmoke spawns many goroutines doing reads, writes, deletes and
// lists. Run with -race to catch data races; the assertions are intentionally
// light because the goroutines race each other freely.
func TestConcurrencySmoke(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := inmemstorage.New()
	defer func() { require.NoError(t, s.Close()) }()

	const workers = 16
	const opsPerWorker = 100

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				name := fmt.Sprintf("w%d/k%d", worker, i%4)
				switch i % 4 {
				case 0:
					_ = cloud.WriteFile(ctx, s, name, bytes.NewReader([]byte(name)))
				case 1:
					if r, _, err := s.ReadFile(ctx, name, cloud.ReadOptions{}); err == nil {
						_, _ = ioctx.ReadAll(ctx, r)
						_ = r.Close(ctx)
					}
				case 2:
					_ = s.Delete(ctx, name)
				case 3:
					_ = s.List(ctx, fmt.Sprintf("w%d/", worker), cloud.ListOptions{},
						func(string) error { return nil })
				}
			}
		}(w)
	}
	wg.Wait()
}

// list is a small helper that drains s.List into a sorted slice for
// comparison.
func list(
	t *testing.T, ctx context.Context, s cloud.ExternalStorage, prefix string, opts cloud.ListOptions,
) []string {
	t.Helper()
	var out []string
	require.NoError(t, s.List(ctx, prefix, opts, func(name string) error {
		out = append(out, name)
		return nil
	}))
	sort.Strings(out)
	return out
}
