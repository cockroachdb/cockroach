// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// TestIndexBackfillMergeCheckpointSurvivesRetries verifies that the completed
// spans checkpointed by the declarative schema changer's index merge survive
// across more than one job retry.
//
// Regression test for the bug where MergeIndexes rebuilds its completed-span
// accumulator from scratch on each invocation and the first progress update
// replaces the checkpoint wholesale, so the durable checkpoint only ever
// contains the current invocation's chunks. After a second retry, spans merged
// by the first invocation are re-included in the spans to do and re-merged.
//
// The merge must be interrupted twice to expose this:
//
//   - invocation 1 merges chunks A, B; checkpoint = {A, B}; retriable failure.
//   - invocation 2 resumes past A, B, merges chunk C; its first checkpoint
//     flush overwrites the checkpoint to {C}, losing {A, B}; failure.
//   - invocation 3 computes todo = fullSpan - {C}, re-scanning A and B.
func TestIndexBackfillMergeCheckpointSurvivesRetries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "schema change with injected retries is slow")

	var writesFn func() error
	populateTempIndexWithWrites := runBeforeFirstBackfillChunk(func() error { return writesFn() })

	// readMergeCheckpoint returns the merge completed spans currently persisted
	// in the schema change job's payload. It is assigned after the server
	// starts.
	var readMergeCheckpoint func() []roachpb.Span

	waitForCheckpoint := func(cond func(spans []roachpb.Span) bool) []roachpb.Span {
		for range 1200 {
			if spans := readMergeCheckpoint(); cond(spans) {
				return spans
			}
			time.Sleep(50 * time.Millisecond)
		}
		return nil
	}

	retriableErr := func() error {
		return kvpb.NewError(&kvpb.AmbiguousResultError{}).GoError()
	}

	// Fail the merge right before scanning these (1-based, cumulative across
	// all invocations) chunks. The first failure interrupts invocation 1 after
	// two chunks have been merged and checkpointed. The second failure
	// interrupts invocation 2 after it has merged several more chunks and
	// flushed its own checkpoint.
	const failAtChunk1 = 3
	const failAtChunk2 = 8

	mergeChunk := 0
	var firstCheckpoint, secondCheckpoint []roachpb.Span
	checkStartingKey := func(_ roachpb.Key) error {
		mergeChunk++
		switch mergeChunk {
		case failAtChunk1:
			// Wait for invocation 1's completed chunks to be durably
			// checkpointed before interrupting it.
			firstCheckpoint = waitForCheckpoint(func(spans []roachpb.Span) bool {
				return len(spans) > 0
			})
			if firstCheckpoint == nil {
				t.Errorf("timed out waiting for the first merge checkpoint to be written")
			}
			return retriableErr()
		case failAtChunk2:
			// Wait for invocation 2's progress to be durably checkpointed
			// before interrupting it.
			prev := fmt.Sprint(firstCheckpoint)
			secondCheckpoint = waitForCheckpoint(func(spans []roachpb.Span) bool {
				return len(spans) > 0 && fmt.Sprint(spans) != prev
			})
			if secondCheckpoint == nil {
				t.Errorf("timed out waiting for the second merge checkpoint to be written")
			} else {
				// The checkpoint must still contain every span completed
				// before the first failure.
				var g roachpb.SpanGroup
				g.Add(secondCheckpoint...)
				for _, sp := range firstCheckpoint {
					if !g.Encloses(sp) {
						t.Errorf(
							"merge checkpoint lost span %s completed before the first retry:\n before: %v\n after:  %v",
							sp, firstCheckpoint, secondCheckpoint,
						)
					}
				}
			}
			return retriableErr()
		}
		return nil
	}

	const maxValue = 2000
	params := base.TestServerArgs{
		Knobs: indexBackfillMergeRetryTestingKnobs(populateTempIndexWithWrites, checkStartingKey),
	}

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	codec := s.ApplicationLayer().Codec()

	readMergeCheckpoint = func() []roachpb.Span {
		var payloadBytes []byte
		if err := sqlDB.QueryRow(
			`SELECT payload FROM crdb_internal.system_jobs WHERE job_type = 'NEW SCHEMA CHANGE' ORDER BY id DESC LIMIT 1`,
		).Scan(&payloadBytes); err != nil {
			return nil
		}
		var payload jobspb.Payload
		if err := protoutil.Unmarshal(payloadBytes, &payload); err != nil {
			return nil
		}
		nsc := payload.GetNewSchemaChange()
		if nsc == nil {
			return nil
		}
		var spans []roachpb.Span
		for i := range nsc.MergeProgress {
			for j := range nsc.MergeProgress[i].MergePairs {
				spans = append(spans, nsc.MergeProgress[i].MergePairs[j].CompletedSpans...)
			}
		}
		return spans
	}

	if _, err := sqlDB.Exec(`
SET create_table_with_schema_locked=false;
SET use_declarative_schema_changer='on';
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(fmt.Sprintf(
		`SET CLUSTER SETTING bulkio.index_backfill.batch_size = %d;`, maxValue/5,
	)); err != nil {
		t.Fatal(err)
	}
	// Keep merge chunks small so that each invocation processes several chunks,
	// and checkpoint frequently so that completed chunks are durable before the
	// injected failures.
	if _, err := sqlDB.Exec(
		`SET CLUSTER SETTING bulkio.index_backfill.merge_batch_size = 100;`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(
		`SET CLUSTER SETTING bulkio.index_backfill.checkpoint_interval = '2ms';`,
	); err != nil {
		t.Fatal(err)
	}

	writesFn = doubleUpdateWrites(sqlDB, maxValue)

	// Bulk insert.
	if err := sqltestutils.BulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	addIndexSchemaChange(t, sqlDB, kvDB, codec, maxValue, 2, func() {
		if _, err := sqlDB.Exec("SHOW JOBS WHEN COMPLETE (SELECT job_id FROM [SHOW JOBS])"); err != nil {
			t.Fatal(err)
		}
	})
	require.Greater(t, mergeChunk, failAtChunk2, "expected the merge to be interrupted twice")
}
