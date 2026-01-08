// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestingWriteResumeSpan writes a checkpoint for the backfill work on origSpan.
// origSpan is the span of keys that were assigned to be backfilled,
// resume is the left over work from origSpan.
func TestingWriteResumeSpan(
	ctx context.Context,
	txn isql.Txn,
	codec keys.SQLCodec,
	col *descs.Collection,
	id descpb.ID,
	mutationID descpb.MutationID,
	filter backfill.MutationFilter,
	finished roachpb.Spans,
	jobsRegistry *jobs.Registry,
) error {
	ctx, traceSpan := tracing.ChildSpan(ctx, "checkpoint")
	defer traceSpan.Finish()

	resumeSpans, manifests, job, mutationIdx, err := rowexec.GetResumeSpansAndSSTManifests(
		ctx, jobsRegistry, txn, codec, col, id, mutationID, filter,
	)
	if err != nil {
		return err
	}

	resumeSpans = roachpb.SubtractSpans(resumeSpans, finished)
	return rowexec.SetResumeSpansAndSSTManifestsInJob(ctx, &codec, resumeSpans, manifests, mutationIdx, txn, job)
}

func TestWriteResumeSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			// Disable all schema change execution.
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				SchemaChangeJobNoOp: func() bool {
					return true
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	sqlRunner.Exec(t, `SET create_table_with_schema_locked=false`)
	sqlRunner.Exec(t, `SET use_declarative_schema_changer='off'`)
	sqlRunner.Exec(t, `CREATE DATABASE t;`)
	sqlRunner.Exec(t, `CREATE TABLE t.test (k INT PRIMARY KEY, v INT);`)
	sqlRunner.Exec(t, `CREATE UNIQUE INDEX vidx ON t.test (v);`)

	// makeKey creates a key with the tenant prefix to work correctly
	// when running with an external test tenant.
	prefix := s.Codec().TenantPrefix()
	prefix = prefix[:len(prefix):len(prefix)]
	makeKey := func(str string) roachpb.Key {
		key := make(roachpb.Key, 0, len(prefix)+len(str))
		key = append(key, prefix...)
		key = append(key, str...)
		return key
	}

	resumeSpans := []roachpb.Span{
		{Key: makeKey("a"), EndKey: makeKey("b")},
		{Key: makeKey("c"), EndKey: makeKey("d")},
		{Key: makeKey("e"), EndKey: makeKey("f")},
		{Key: makeKey("g"), EndKey: makeKey("h")},
		{Key: makeKey("i"), EndKey: makeKey("j")},
		{Key: makeKey("k"), EndKey: makeKey("l")},
		{Key: makeKey("m"), EndKey: makeKey("n")},
		{Key: makeKey("o"), EndKey: makeKey("p")},
		{Key: makeKey("q"), EndKey: makeKey("r")},
	}

	registry := s.JobRegistry().(*jobs.Registry)
	tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(
		kvDB, s.Codec(), "t", "test")

	if err := kvDB.Put(
		ctx,
		catalogkeys.MakeDescMetadataKey(s.Codec(), tableDesc.ID),
		tableDesc.DescriptorProto(),
	); err != nil {
		t.Fatal(err)
	}

	mutationID := tableDesc.AllMutations()[0].MutationID()
	var jobID jobspb.JobID

	if len(tableDesc.MutationJobs) > 0 {
		for _, job := range tableDesc.MutationJobs {
			if job.MutationID == mutationID {
				jobID = job.JobID
				break
			}
		}
	}

	details := jobspb.SchemaChangeDetails{ResumeSpanList: []jobspb.ResumeSpanList{
		{ResumeSpans: resumeSpans}}}

	job, err := registry.LoadJob(ctx, jobID)
	if err != nil {
		t.Fatal(errors.Wrapf(err, "can't find job %d", jobID))
	}

	require.NoError(t, job.NoTxn().Update(ctx, func(
		_ isql.Txn, _ jobs.JobMetadata, ju *jobs.JobUpdater,
	) error {
		ju.UpdateState(jobs.StateRunning)
		return nil
	}))

	err = job.NoTxn().SetDetails(ctx, details)
	if err != nil {
		t.Fatal(err)
	}

	testData := []struct {
		orig   roachpb.Span
		resume roachpb.Span
	}{
		// Work performed in the middle of a span.
		{orig: roachpb.Span{Key: makeKey("a1"), EndKey: makeKey("a3")},
			resume: roachpb.Span{Key: makeKey("a2"), EndKey: makeKey("a3")}},
		// Work completed in the middle of a span.
		{orig: roachpb.Span{Key: makeKey("c1"), EndKey: makeKey("c2")},
			resume: roachpb.Span{}},
		// Work performed in the right of a span.
		{orig: roachpb.Span{Key: makeKey("e1"), EndKey: makeKey("f")},
			resume: roachpb.Span{Key: makeKey("e2"), EndKey: makeKey("f")}},
		// Work completed in the right of a span.
		{orig: roachpb.Span{Key: makeKey("g1"), EndKey: makeKey("h")},
			resume: roachpb.Span{}},
		// Work performed in the left of a span.
		{orig: roachpb.Span{Key: makeKey("i"), EndKey: makeKey("i2")},
			resume: roachpb.Span{Key: makeKey("i1"), EndKey: makeKey("i2")}},
		// Work completed in the left of a span.
		{orig: roachpb.Span{Key: makeKey("k"), EndKey: makeKey("k2")},
			resume: roachpb.Span{}},
		// Work performed on a span.
		{orig: roachpb.Span{Key: makeKey("m"), EndKey: makeKey("n")},
			resume: roachpb.Span{Key: makeKey("m1"), EndKey: makeKey("n")}},
		// Work completed on a span.
		{orig: roachpb.Span{Key: makeKey("o"), EndKey: makeKey("p")},
			resume: roachpb.Span{}},
	}
	for _, test := range testData {
		finished := test.orig
		if test.resume.Key != nil {
			finished.EndKey = test.resume.Key
		}
		if err := sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
			return TestingWriteResumeSpan(
				ctx,
				txn,
				s.Codec(),
				col,
				tableDesc.ID,
				mutationID,
				backfill.IndexMutationFilter,
				roachpb.Spans{finished},
				registry,
			)
		}); err != nil {
			t.Error(err)
		}
	}

	expected := []roachpb.Span{
		// Work performed in the middle of a span.
		{Key: makeKey("a"), EndKey: makeKey("a1")},
		{Key: makeKey("a2"), EndKey: makeKey("b")},
		// Work completed in the middle of a span.
		{Key: makeKey("c"), EndKey: makeKey("c1")},
		{Key: makeKey("c2"), EndKey: makeKey("d")},
		// Work performed in the right of a span.
		{Key: makeKey("e"), EndKey: makeKey("e1")},
		{Key: makeKey("e2"), EndKey: makeKey("f")},
		// Work completed in the right of a span.
		{Key: makeKey("g"), EndKey: makeKey("g1")},
		// Work performed in the left of a span.
		{Key: makeKey("i1"), EndKey: makeKey("j")},
		// Work completed in the left of a span.
		{Key: makeKey("k2"), EndKey: makeKey("l")},
		// Work performed on a span.
		{Key: makeKey("m1"), EndKey: makeKey("n")},
		// Work completed on a span; ["o", "p"] complete.
		{Key: makeKey("q"), EndKey: makeKey("r")},
	}

	var got []roachpb.Span
	if err := sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) (err error) {
		got, _, _, err = rowexec.GetResumeSpans(
			ctx, registry, txn, s.Codec(), col, tableDesc.ID, mutationID, backfill.IndexMutationFilter)
		return err
	}); err != nil {
		t.Error(err)
	}
	if len(expected) != len(got) {
		t.Fatalf("expected = %+v\n got = %+v", expected, got)
	}
	for i, e := range expected {
		if !e.EqualValue(got[i]) {
			t.Fatalf("expected = %+v, got = %+v", e, got[i])
		}
	}
}

func TestResumeSpanSSTManifestRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				SchemaChangeJobNoOp: func() bool {
					// Disable schema change execution. This way all mutations never leave the descriptor.
					return true
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	sqlRunner.Exec(t, `SET create_table_with_schema_locked=false`)
	sqlRunner.Exec(t, `SET use_declarative_schema_changer='off'`)
	sqlRunner.Exec(t, `CREATE DATABASE t;`)
	sqlRunner.Exec(t, `CREATE TABLE t.test (k INT PRIMARY KEY, v INT);`)
	sqlRunner.Exec(t, `CREATE UNIQUE INDEX vidx ON t.test (v);`)

	registry := s.JobRegistry().(*jobs.Registry)
	tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(
		kvDB, s.Codec(), "t", "test")

	// Pick up the index we created above. Note: there is a schema change knob
	// that no-ops the schema change leaving the mutation in place.
	require.Equal(t, 2, len(tableDesc.AllMutations()))
	mutationID := tableDesc.AllMutations()[0].MutationID()

	// The job is marked as completed since we made it a no-op. But since we want
	// to update the job progress, lets change it back to running.
	require.Equal(t, 1, len(tableDesc.MutationJobs))
	jobID := tableDesc.MutationJobs[0].JobID
	require.NotZero(t, jobID)
	sqlRunner.Exec(t, fmt.Sprintf("UPDATE system.jobs SET status = 'running' WHERE id = %d", jobID))

	var expected jobspb.IndexBackfillSSTManifest
	ts := s.Clock().Now()
	expected.URI = "nodelocal://1/index-backfill/test"
	prefix := s.Codec().TenantPrefix()
	prefix = prefix[:len(prefix):len(prefix)]
	makeKey := func(s string) roachpb.Key {
		key := make(roachpb.Key, 0, len(prefix)+len(s))
		key = append(key, prefix...)
		key = append(key, s...)
		return key
	}
	span := roachpb.Span{
		Key:    makeKey("sst-start"),
		EndKey: makeKey("sst-end"),
	}
	expected.Span = &span
	expected.FileSize = 12345
	expected.RowSample = makeKey("sample")
	expected.WriteTimestamp = &ts

	if err := sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		spans, _, job, mutationIdx, err := rowexec.GetResumeSpansAndSSTManifests(
			ctx, registry, txn, s.Codec(), col, tableDesc.ID, mutationID, backfill.IndexMutationFilter,
		)
		if err != nil {
			return err
		}
		codec := s.Codec()
		return rowexec.SetResumeSpansAndSSTManifestsInJob(
			ctx, &codec, spans, []jobspb.IndexBackfillSSTManifest{expected}, mutationIdx, txn, job,
		)
	}); err != nil {
		t.Fatal(err)
	}

	if err := sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		_, manifests, _, _, err := rowexec.GetResumeSpansAndSSTManifests(
			ctx, registry, txn, s.Codec(), col, tableDesc.ID, mutationID, backfill.IndexMutationFilter,
		)
		if err != nil {
			return err
		}
		require.Equal(t, []jobspb.IndexBackfillSSTManifest{expected}, manifests)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
