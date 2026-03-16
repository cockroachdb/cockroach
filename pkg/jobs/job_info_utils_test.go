// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestReadWriteChunkedFileToJobInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	ctx := context.Background()
	params := base.TestServerArgs{}
	params.Knobs.JobsTestingKnobs = NewTestingKnobsWithShortIntervals()
	s := serverutils.StartServerOnly(t, params)
	defer s.Stopper().Stop(ctx)

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "empty file",
			data: []byte{},
		},
		{
			name: "file less than 1MiB",
			data: make([]byte, 1<<20-1), // 1 MiB - 1 byte
		},
		{
			name: "file equal to 1MiB",
			data: make([]byte, 1<<20), // 1 MiB
		},
		{
			name: "file greater than 1MiB",
			data: make([]byte, 1<<20+1), // 1 MiB + 1 byte
		},
		{
			name: "file much greater than 1MiB",
			data: make([]byte, 10<<20), // 10 MiB
		},
	}

	db := s.InternalDB().(isql.DB)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.data) > 0 {
				randutil.ReadTestdataBytes(rng, tt.data)
			}
			require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				err := WriteChunkedFileToJobInfo(ctx, tt.name, tt.data, txn, jobspb.JobID(123))
				if err != nil {
					return err
				}
				got, err := ReadChunkedFileToJobInfo(ctx, tt.name, txn, jobspb.JobID(123))
				if err != nil {
					return err
				}
				require.Equal(t, tt.data, got)
				return nil
			}))
		})
	}
}

func TestDeleteChunkedFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params := base.TestServerArgs{}
	params.Knobs.JobsTestingKnobs = NewTestingKnobsWithShortIntervals()
	s := serverutils.StartServerOnly(t, params)
	defer s.Stopper().Stop(ctx)

	db := s.InternalDB().(isql.DB)
	jobID := jobspb.JobID(456)

	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		data := []byte("test data for deletion")
		if err := WriteChunkedFileToJobInfo(ctx, "delete-test", data, txn, jobID); err != nil {
			return err
		}
		if err := DeleteChunkedFile(ctx, "delete-test", txn, jobID); err != nil {
			return err
		}
		got, err := ReadChunkedFileToJobInfo(ctx, "delete-test", txn, jobID)
		if err != nil {
			return err
		}
		require.Empty(t, got)
		return nil
	}))
}

func TestReadWriteChunkedProtos(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	ctx := context.Background()
	params := base.TestServerArgs{}
	params.Knobs.JobsTestingKnobs = NewTestingKnobsWithShortIntervals()
	s := serverutils.StartServerOnly(t, params)
	defer s.Stopper().Stop(ctx)

	db := s.InternalDB().(isql.DB)
	jobID := jobspb.JobID(789)

	t.Run("write and read multiple protos", func(t *testing.T) {
		msgs := []jobspb.Payload{
			{Description: "first"},
			{Description: "second"},
			{Description: "third"},
		}
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return WriteChunkedProtos(ctx, "test-protos", msgs, txn, jobID)
		}))

		var got []jobspb.Payload
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			found, err := ReadChunkedProtos(ctx, "test-protos", txn, jobID, &got)
			require.NoError(t, err)
			require.True(t, found)
			return nil
		}))
		require.Len(t, got, 3)
		require.Equal(t, "first", got[0].Description)
		require.Equal(t, "second", got[1].Description)
		require.Equal(t, "third", got[2].Description)
	})

	t.Run("empty slice", func(t *testing.T) {
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return WriteChunkedProtos(
				ctx, "test-empty", []jobspb.Payload{}, txn, jobID,
			)
		}))

		var got []jobspb.Payload
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			found, err := ReadChunkedProtos(ctx, "test-empty", txn, jobID, &got)
			require.NoError(t, err)
			require.False(t, found)
			return nil
		}))
		require.Empty(t, got)
	})

	t.Run("read nonexistent", func(t *testing.T) {
		var got []jobspb.Payload
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			found, err := ReadChunkedProtos(ctx, "nonexistent", txn, jobID, &got)
			require.NoError(t, err)
			require.False(t, found)
			return nil
		}))
		require.Empty(t, got)
	})

	// Test that protos spanning multiple chunks are written and read correctly,
	// and that overwriting with a different number of chunks cleans up properly.
	t.Run("multi-chunk round trip and overwrite", func(t *testing.T) {
		// Each proto is ~100KB when marshaled, so 15 protos is ~1.5 MiB which
		// spans at least 2 chunks.
		makeProtos := func(n int) []jobspb.Payload {
			msgs := make([]jobspb.Payload, n)
			for i := range msgs {
				desc := make([]byte, 100*1024)
				randutil.ReadTestdataBytes(rng, desc)
				msgs[i] = jobspb.Payload{Description: string(desc)}
			}
			return msgs
		}

		filename := "test-multi-chunk-protos"

		// Write 15 protos (~1.5 MiB) which should span multiple chunks.
		msgs := makeProtos(15)
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return WriteChunkedProtos(ctx, filename, msgs, txn, jobID)
		}))
		var got []jobspb.Payload
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			found, err := ReadChunkedProtos(ctx, filename, txn, jobID, &got)
			require.NoError(t, err)
			require.True(t, found)
			return nil
		}))
		require.Len(t, got, 15)
		for i, g := range got {
			require.Equal(t, msgs[i].Description, g.Description)
		}

		// Overwrite with fewer protos (5, ~500KB, single chunk).
		msgs2 := makeProtos(5)
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return WriteChunkedProtos(ctx, filename, msgs2, txn, jobID)
		}))
		got = nil
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			found, err := ReadChunkedProtos(ctx, filename, txn, jobID, &got)
			require.NoError(t, err)
			require.True(t, found)
			return nil
		}))
		require.Len(t, got, 5)
		for i, g := range got {
			require.Equal(t, msgs2[i].Description, g.Description)
		}

		// Overwrite with more protos again (30, ~3 MiB, multiple chunks).
		msgs3 := makeProtos(30)
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return WriteChunkedProtos(ctx, filename, msgs3, txn, jobID)
		}))
		got = nil
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			found, err := ReadChunkedProtos(ctx, filename, txn, jobID, &got)
			require.NoError(t, err)
			require.True(t, found)
			return nil
		}))
		require.Len(t, got, 30)
		for i, g := range got {
			require.Equal(t, msgs3[i].Description, g.Description)
		}
	})
}

func TestOverwriteChunkingWithVariableLengths(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	ctx := context.Background()
	params := base.TestServerArgs{}
	params.Knobs.JobsTestingKnobs = NewTestingKnobsWithShortIntervals()
	s := serverutils.StartServerOnly(t, params)
	defer s.Stopper().Stop(ctx)

	tests := []struct {
		name       string
		numChunks  int
		data       []byte
		moreChunks []byte
		lessChunks []byte
	}{
		{
			name:      "zero chunks",
			data:      []byte{},
			numChunks: 0,
		},
		{
			name:      "one chunk",
			numChunks: 1,
		},
		{
			name:      "two chunks",
			numChunks: 2,
		},
		{
			name:      "five chunks",
			numChunks: 5,
		},
	}

	db := s.InternalDB().(isql.DB)
	generateData := func(numChunks int) []byte {
		data := make([]byte, (1<<20)*numChunks)
		if len(data) > 0 {
			randutil.ReadTestdataBytes(rng, data)
		}
		return data
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.data = generateData(tt.numChunks)
			// Write the first file, this will generate a certain number of chunks.
			require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				return WriteChunkedFileToJobInfo(ctx, tt.name, tt.data, txn, jobspb.JobID(123))
			}))

			// Overwrite the file with fewer chunks, this should delete the extra
			// chunks before writing the new ones.
			t.Run("overwrite with fewer chunks", func(t *testing.T) {
				lessChunks := tt.numChunks - 1
				if lessChunks >= 0 {
					tt.data = generateData(lessChunks)
					var got []byte
					require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
						err := WriteChunkedFileToJobInfo(ctx, tt.name, tt.data, txn, jobspb.JobID(123))
						if err != nil {
							return err
						}
						got, err = ReadChunkedFileToJobInfo(ctx, tt.name, txn, jobspb.JobID(123))
						return err
					}))
					require.Equal(t, tt.data, got)
				}
			})

			// Overwrite the file with more chunks, this should delete the extra
			// chunks before writing the new ones.
			t.Run("overwrite with more chunks", func(t *testing.T) {
				moreChunks := tt.numChunks + 1
				tt.data = generateData(moreChunks)
				var got []byte
				require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					err := WriteChunkedFileToJobInfo(ctx, tt.name, tt.data, txn, jobspb.JobID(123))
					if err != nil {
						return err
					}
					got, err = ReadChunkedFileToJobInfo(ctx, tt.name, txn, jobspb.JobID(123))
					return err
				}))
				require.Equal(t, tt.data, got)
			})
		})
	}
}
