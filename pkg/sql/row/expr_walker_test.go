// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package row

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func createAndIncrementSeqDescriptor(
	ctx context.Context,
	t *testing.T,
	id int,
	codec keys.SQLCodec,
	incrementBy int64,
	seqOpts descpb.TableDescriptor_SequenceOpts,
	db *kv.DB,
) catalog.TableDescriptor {
	desc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:           descpb.ID(id),
		SequenceOpts: &seqOpts,
	}).BuildImmutableTable()
	seqValueKey := codec.SequenceKey(uint32(desc.GetID()))
	_, err := kv.IncrementValRetryable(
		ctx, db, seqValueKey, incrementBy)
	require.NoError(t, err)
	return desc
}

func createMockImportJob(
	ctx context.Context,
	t *testing.T,
	registry *jobs.Registry,
	seqIDToAllocatedChunks map[int32]*jobspb.SequenceDetails_SequenceChunks,
	resumePos int64,
) *jobs.Job {
	seqDetails := []*jobspb.SequenceDetails{{SeqIdToChunks: seqIDToAllocatedChunks}}
	mockImportRecord := jobs.Record{
		Details: jobspb.ImportDetails{},
		Progress: jobspb.ImportProgress{
			SequenceDetails: seqDetails,
			ResumePos:       []int64{resumePos},
		},
	}
	mockImportJob, err := registry.CreateJobWithTxn(ctx, mockImportRecord, registry.MakeJobID(), nil)
	require.NoError(t, err)
	return mockImportJob
}

// TestJobBackedSeqChunkProvider this is a unit test of the sequence chunk
// provider which is used to populate default expressions during an import.
func TestJobBackedSeqChunkProvider(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	evalCtx := &tree.EvalContext{
		Context:          ctx,
		DB:               db,
		Codec:            keys.TODOSQLCodec,
		InternalExecutor: s.InternalExecutor().(sqlutil.InternalExecutor),
	}

	registry := s.JobRegistry().(*jobs.Registry)
	testCases := []struct {
		name string
		// chunks written to progress before we RequestChunk.
		allocatedChunks map[int32]*jobspb.SequenceDetails_SequenceChunks
		// value by which the sequence is incremented before we RequestChunk.
		incrementBy int64
		resumePos   int64
		// row being processed during the import.
		rowID              int64
		instancesPerRow    int64
		seqIDToOpts        map[int]descpb.TableDescriptor_SequenceOpts
		seqIDToExpectedVal map[int]int64
		seqIDToNumChunks   map[int]int
	}{
		{
			// No previously allocated chunks, this is the first row being imported.
			name:            "first-chunk",
			allocatedChunks: nil,
			rowID:           0,
			instancesPerRow: 1,
			seqIDToOpts: map[int]descpb.TableDescriptor_SequenceOpts{55: {
				Increment: 1,
				MinValue:  1,
				MaxValue:  100,
				Start:     1,
			}},
			seqIDToExpectedVal: map[int]int64{55: 1},
			seqIDToNumChunks:   map[int]int{55: 1},
		},
		{
			// Import row in already allocated first chunk. Should not allocate a new
			// chunk.
			name: "row-in-first-chunk",
			allocatedChunks: map[int32]*jobspb.SequenceDetails_SequenceChunks{56: {Chunks: []*jobspb.SequenceValChunk{{
				ChunkStartVal:     1,
				ChunkStartRow:     0,
				ChunkSize:         10,
				NextChunkStartRow: 10,
			}}}},
			rowID:           8,
			instancesPerRow: 1,
			seqIDToOpts: map[int]descpb.TableDescriptor_SequenceOpts{56: {
				Increment: 1,
				MinValue:  1,
				MaxValue:  100,
				Start:     1,
			}},
			seqIDToExpectedVal: map[int]int64{56: 9},
			seqIDToNumChunks:   map[int]int{56: 1},
		},
		{
			// Import row is greater than the max row covered by the allocated chunk.
			// We expect to see another chunk getting allocated.
			name: "need-new-chunk",
			allocatedChunks: map[int32]*jobspb.SequenceDetails_SequenceChunks{57: {Chunks: []*jobspb.SequenceValChunk{{
				ChunkStartVal:     1,
				ChunkStartRow:     0,
				ChunkSize:         10,
				NextChunkStartRow: 10,
			}}}},
			incrementBy: 10,
			// rowID is equal to NextChunkStartRow
			rowID:           10,
			instancesPerRow: 1,
			seqIDToOpts: map[int]descpb.TableDescriptor_SequenceOpts{57: {
				Increment: 1,
				MinValue:  1,
				MaxValue:  100,
				Start:     1,
			}},
			seqIDToExpectedVal: map[int]int64{57: 11},
			seqIDToNumChunks:   map[int]int{57: 2},
		},
		{
			// Same test case as before, but the resume position means that the first
			// chunk should get cleaned up as it covers already processed rows.
			name: "cleanup-old-chunks",
			allocatedChunks: map[int32]*jobspb.SequenceDetails_SequenceChunks{58: {Chunks: []*jobspb.SequenceValChunk{{
				ChunkStartVal:     1,
				ChunkStartRow:     0,
				ChunkSize:         10,
				NextChunkStartRow: 10,
			}}}},
			incrementBy:     10,
			resumePos:       10,
			rowID:           10,
			instancesPerRow: 1,
			seqIDToOpts: map[int]descpb.TableDescriptor_SequenceOpts{58: {
				Increment: 1,
				MinValue:  1,
				MaxValue:  100,
				Start:     1,
			}},
			seqIDToExpectedVal: map[int]int64{58: 11},
			seqIDToNumChunks:   map[int]int{58: 1},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			job := createMockImportJob(ctx, t, registry, test.allocatedChunks, test.resumePos)
			j := &SeqChunkProvider{Registry: registry, JobID: job.ID()}
			annot := &CellInfoAnnotation{
				sourceID: 0,
				rowID:    test.rowID,
			}

			for id, val := range test.seqIDToExpectedVal {
				seqDesc := createAndIncrementSeqDescriptor(ctx, t, id, keys.TODOSQLCodec,
					test.incrementBy, test.seqIDToOpts[id], db)
				seqMetadata := &SequenceMetadata{
					id:              descpb.ID(id),
					seqDesc:         seqDesc,
					instancesPerRow: test.instancesPerRow,
					curChunk:        nil,
					curVal:          0,
				}
				require.NoError(t, j.RequestChunk(evalCtx, annot, seqMetadata))
				getJobProgressQuery := `SELECT progress FROM system.jobs J WHERE J.id = $1`

				var progressBytes []byte
				require.NoError(t, sqlDB.QueryRow(getJobProgressQuery, job.ID()).Scan(&progressBytes))
				var progress jobspb.Progress
				require.NoError(t, protoutil.Unmarshal(progressBytes, &progress))
				chunks := progress.GetImport().SequenceDetails[0].SeqIdToChunks[int32(id)].Chunks

				// Ensure that the sequence value for the row is what we expect.
				require.Equal(t, val, seqMetadata.curVal)
				// Ensure we have as many chunks written to the job progress as we
				// expect.
				require.Equal(t, test.seqIDToNumChunks[id], len(chunks))
			}
		})
	}
}
