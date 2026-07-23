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
