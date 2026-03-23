// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"context"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRejectedFilename(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tests := []struct {
		name     string
		fname    string
		rejected string
	}{
		{
			name:     "http",
			fname:    "http://127.0.0.1",
			rejected: "http://127.0.0.1/.rejected",
		},
		{
			name:     "nodelocal",
			fname:    "nodelocal://1/file.csv",
			rejected: "nodelocal://1/file.csv.rejected",
		},
	}
	for _, tc := range tests {
		rej, err := rejectedFilename(tc.fname)
		if err != nil {
			t.Fatal(err)
		}
		if tc.rejected != rej {
			t.Errorf("expected:\n%v\ngot:\n%v\n", tc.rejected, rej)
		}
	}
}

// nilDataProducer produces infinite stream of nulls.
// It implements importRowProducer.
type nilDataProducer struct{}

func (p *nilDataProducer) Scan() bool {
	return true
}

func (p *nilDataProducer) Err() error {
	return nil
}

func (p *nilDataProducer) Skip() error {
	return nil
}

func (p *nilDataProducer) Row() (interface{}, error) {
	return nil, nil
}

func (p *nilDataProducer) Progress() float32 {
	return 0.0
}

var _ importRowProducer = &nilDataProducer{}

// errorReturningConsumer always returns an error.
// It implements importRowConsumer.
type errorReturningConsumer struct {
	err error
}

func (d *errorReturningConsumer) FillDatums(
	_ context.Context, _ interface{}, _ int64, c *row.DatumRowConverter,
) error {
	return d.err
}

var _ importRowConsumer = &errorReturningConsumer{}

// nilDataConsumer consumes and emits infinite stream of null.
// it implements importRowConsumer.
type nilDataConsumer struct{}

func (n *nilDataConsumer) FillDatums(
	_ context.Context, _ interface{}, _ int64, c *row.DatumRowConverter,
) error {
	c.Datums[0] = tree.DNull
	return nil
}

var _ importRowConsumer = &nilDataConsumer{}

func TestParallelImportProducerHandlesConsumerErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Dummy descriptor for import
	descr := descpb.TableDescriptor{
		Name: "test",
		Columns: []descpb.ColumnDescriptor{
			{Name: "column", ID: 1, Type: types.Int, Nullable: true},
		},
	}

	// Flush datum converter frequently
	defer row.TestingSetDatumRowConverterBatchSize(1)()

	// Create KV channel and arrange for it to be drained
	kvCh := make(chan row.KVBatch)
	defer close(kvCh)
	go func() {
		for range kvCh {
		}
	}()

	// Prepare import context, which flushes to kvCh frequently.
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	importCtx := &parallelImportContext{
		semaCtx:    &semaCtx,
		numWorkers: 1,
		batchSize:  2,
		evalCtx:    testEvalCtx,
		tableDesc:  tabledesc.NewBuilder(&descr).BuildImmutableTable(),
		kvCh:       kvCh,
	}

	consumer := &errorReturningConsumer{errors.New("consumer aborted")}

	require.Equal(t, consumer.err,
		runParallelImport(context.Background(), importCtx,
			&importFileContext{}, &nilDataProducer{}, consumer))
}

func TestParallelImportProducerHandlesCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Dummy descriptor for import
	descr := descpb.TableDescriptor{
		Name: "test",
		Columns: []descpb.ColumnDescriptor{
			{Name: "column", ID: 1, Type: types.Int, Nullable: true},
		},
	}

	// Flush datum converter frequently
	defer row.TestingSetDatumRowConverterBatchSize(1)()

	// Create KV channel and arrange for it to be drained
	kvCh := make(chan row.KVBatch)
	defer close(kvCh)
	go func() {
		for range kvCh {
		}
	}()

	// Prepare import context, which flushes to kvCh frequently.
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	importCtx := &parallelImportContext{
		numWorkers: 1,
		batchSize:  2,
		semaCtx:    &semaCtx,
		evalCtx:    testEvalCtx,
		tableDesc:  tabledesc.NewBuilder(&descr).BuildImmutableTable(),
		kvCh:       kvCh,
	}

	// Run a hundred imports, which will timeout shortly after they start.
	require.NoError(t, ctxgroup.GroupWorkers(context.Background(), 100,
		func(_ context.Context, _ int) error {
			timeout := time.Millisecond * time.Duration(250+rand.Intn(250))
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer func(f func()) {
				f()
			}(cancel)
			require.Equal(t, context.DeadlineExceeded,
				runParallelImport(ctx, importCtx,
					&importFileContext{}, &nilDataProducer{}, &nilDataConsumer{}))
			return nil
		}))
}

// mockSeekableReader is a mock that implements ReadCloserCtx, ReaderAtCtx, and SeekerCtx
// for testing makeFileReader with Parquet format
type mockSeekableReader struct {
	data   []byte
	pos    int64
	closed bool
}

func newMockSeekableReader(data string) *mockSeekableReader {
	return &mockSeekableReader{
		data: []byte(data),
	}
}

func (m *mockSeekableReader) Read(ctx context.Context, p []byte) (int, error) {
	if m.pos >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[m.pos:])
	m.pos += int64(n)
	return n, nil
}

func (m *mockSeekableReader) Close(ctx context.Context) error {
	m.closed = true
	return nil
}

func (m *mockSeekableReader) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *mockSeekableReader) Seek(ctx context.Context, offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = m.pos + offset
	case io.SeekEnd:
		newPos = int64(len(m.data)) + offset
	default:
		return 0, errors.Newf("invalid whence: %d", whence)
	}

	if newPos < 0 {
		return 0, errors.New("negative position")
	}

	m.pos = newPos
	return newPos, nil
}

// mockNonSeekableReader implements ReadCloserCtx but NOT SeekerCtx
type mockNonSeekableReader struct {
	data   []byte
	pos    int64
	closed bool
}

func newMockNonSeekableReader(data string) *mockNonSeekableReader {
	return &mockNonSeekableReader{
		data: []byte(data),
	}
}

func (m *mockNonSeekableReader) Read(ctx context.Context, p []byte) (int, error) {
	if m.pos >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[m.pos:])
	m.pos += int64(n)
	return n, nil
}

func (m *mockNonSeekableReader) Close(ctx context.Context) error {
	m.closed = true
	return nil
}

// mockExternalStorage is a minimal mock implementation of cloud.ExternalStorage
// for testing file reader creation.
type mockExternalStorage struct {
	data []byte
}

func newMockExternalStorage(data string) *mockExternalStorage {
	return &mockExternalStorage{
		data: []byte(data),
	}
}

func (m *mockExternalStorage) ReadFile(
	ctx context.Context, basename string, opts cloud.ReadOptions,
) (ioctx.ReadCloserCtx, int64, error) {
	// Create a reader starting at the specified offset
	start := opts.Offset
	if start >= int64(len(m.data)) {
		return nil, 0, io.EOF
	}

	// If LengthHint is set, limit the data to that length
	end := int64(len(m.data))
	if opts.LengthHint > 0 && start+opts.LengthHint < end {
		end = start + opts.LengthHint
	}

	return newMockSeekableReader(string(m.data[start:end])), int64(len(m.data)), nil
}

func (m *mockExternalStorage) Close() error                  { return nil }
func (m *mockExternalStorage) Conf() cloudpb.ExternalStorage { return cloudpb.ExternalStorage{} }
func (m *mockExternalStorage) ExternalIOConf() base.ExternalIODirConfig {
	return base.ExternalIODirConfig{}
}
func (m *mockExternalStorage) RequiresExternalIOAccounting() bool { return false }
func (m *mockExternalStorage) Settings() *cluster.Settings        { return nil }
func (m *mockExternalStorage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	return nil, errors.New("not implemented")
}
func (m *mockExternalStorage) List(
	ctx context.Context, prefix string, opts cloud.ListOptions, fn cloud.ListingFn,
) error {
	return errors.New("not implemented")
}
func (m *mockExternalStorage) Delete(ctx context.Context, basename string) error {
	return errors.New("not implemented")
}
func (m *mockExternalStorage) Size(ctx context.Context, basename string) (int64, error) {
	return int64(len(m.data)), nil
}

func TestMakeFileReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testData := "test data for file reader"

	t.Run("parquet-with-seekable-reader", func(t *testing.T) {
		mockStorage := newMockExternalStorage(testData)
		mockReader := newMockSeekableReader(testData)
		format := roachpb.IOFileFormat{
			Format: roachpb.IOFileFormat_Parquet,
		}

		reader, closer, err := makeFileReader(ctx, format, mockReader, "test.parquet", int64(len(testData)), mockStorage)
		require.NoError(t, err)
		require.NotNil(t, reader)
		require.NotNil(t, closer)

		// Verify fileReader has all required interfaces
		require.NotNil(t, reader.Reader)
		require.NotNil(t, reader.ReaderAt)
		require.NotNil(t, reader.Seeker)
		require.Equal(t, int64(len(testData)), reader.total)

		// Clean up
		err = closer.Close()
		require.NoError(t, err)
	})

	t.Run("parquet-with-non-seekable-reader", func(t *testing.T) {
		// Even with non-seekable readers from storage, Parquet should work
		// because we wrap them with RandomAccessReader
		mockStorage := newMockExternalStorage(testData)
		mockReader := newMockNonSeekableReader(testData)
		format := roachpb.IOFileFormat{
			Format: roachpb.IOFileFormat_Parquet,
		}

		reader, closer, err := makeFileReader(ctx, format, mockReader, "test.parquet", int64(len(testData)), mockStorage)
		require.NoError(t, err)
		require.NotNil(t, reader)
		require.NotNil(t, closer)

		// Verify fileReader has all required interfaces (wrapper provides them)
		require.NotNil(t, reader.Reader)
		require.NotNil(t, reader.ReaderAt)
		require.NotNil(t, reader.Seeker)
		require.Equal(t, int64(len(testData)), reader.total)

		// Clean up
		err = closer.Close()
		require.NoError(t, err)
	})

	t.Run("csv-format", func(t *testing.T) {
		// CSV doesn't need seeking, so use a non-seekable reader factory
		mockStorage := newMockExternalStorage(testData)
		mockReader := newMockNonSeekableReader(testData)
		format := roachpb.IOFileFormat{
			Format:      roachpb.IOFileFormat_CSV,
			Compression: roachpb.IOFileFormat_None,
		}

		reader, closer, err := makeFileReader(ctx, format, mockReader, "test.csv", int64(len(testData)), mockStorage)
		require.NoError(t, err)
		require.NotNil(t, reader)
		require.NotNil(t, closer)

		// Verify fileReader for CSV
		require.NotNil(t, reader.Reader)
		require.Nil(t, reader.ReaderAt) // CSV doesn't need ReaderAt
		require.Nil(t, reader.Seeker)   // CSV doesn't need Seeker
		require.Equal(t, int64(len(testData)), reader.total)

		// Clean up
		err = closer.Close()
		require.NoError(t, err)
	})

	t.Run("avro-format", func(t *testing.T) {
		// Avro doesn't need seeking, so use a non-seekable reader factory
		mockStorage := newMockExternalStorage(testData)
		mockReader := newMockNonSeekableReader(testData)
		format := roachpb.IOFileFormat{
			Format:      roachpb.IOFileFormat_Avro,
			Compression: roachpb.IOFileFormat_None,
		}

		reader, closer, err := makeFileReader(ctx, format, mockReader, "test.avro", int64(len(testData)), mockStorage)
		require.NoError(t, err)
		require.NotNil(t, reader)
		require.NotNil(t, closer)

		// Verify fileReader for Avro
		require.NotNil(t, reader.Reader)
		require.Nil(t, reader.ReaderAt)
		require.Nil(t, reader.Seeker)
		require.Equal(t, int64(len(testData)), reader.total)

		// Clean up
		err = closer.Close()
		require.NoError(t, err)
	})
}
