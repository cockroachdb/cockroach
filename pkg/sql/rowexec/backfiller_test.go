// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec_test

import (
	"context"
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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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

	resumeSpans, job, mutationIdx, err := rowexec.GetResumeSpans(
		ctx, jobsRegistry, txn, codec, col, id, mutationID, filter,
	)
	if err != nil {
		return err
	}

	resumeSpans = roachpb.SubtractSpans(resumeSpans, finished)
	return rowexec.SetResumeSpansInJob(ctx, resumeSpans, mutationIdx, txn, job)
}

func TestWriteResumeSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	server, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			// Disable all schema change execution.
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				SchemaChangeJobNoOp: func() bool {
					return true
				},
			},
		},
	})
	defer server.Stopper().Stop(ctx)

	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	sqlRunner.Exec(t, `SET use_declarative_schema_changer='off'`)
	sqlRunner.Exec(t, `CREATE DATABASE t;`)
	sqlRunner.Exec(t, `CREATE TABLE t.test (k INT PRIMARY KEY, v INT);`)
	sqlRunner.Exec(t, `CREATE UNIQUE INDEX vidx ON t.test (v);`)

	resumeSpans := []roachpb.Span{
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
		{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
		{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")},
		{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")},
		{Key: roachpb.Key("i"), EndKey: roachpb.Key("j")},
		{Key: roachpb.Key("k"), EndKey: roachpb.Key("l")},
		{Key: roachpb.Key("m"), EndKey: roachpb.Key("n")},
		{Key: roachpb.Key("o"), EndKey: roachpb.Key("p")},
		{Key: roachpb.Key("q"), EndKey: roachpb.Key("r")},
	}

	registry := server.JobRegistry().(*jobs.Registry)
	tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(
		kvDB, keys.SystemSQLCodec, "t", "test")

	if err := kvDB.Put(
		ctx,
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.ID),
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
		ju.UpdateStatus(jobs.StatusRunning)
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
		{orig: roachpb.Span{Key: roachpb.Key("a1"), EndKey: roachpb.Key("a3")},
			resume: roachpb.Span{Key: roachpb.Key("a2"), EndKey: roachpb.Key("a3")}},
		// Work completed in the middle of a span.
		{orig: roachpb.Span{Key: roachpb.Key("c1"), EndKey: roachpb.Key("c2")},
			resume: roachpb.Span{}},
		// Work performed in the right of a span.
		{orig: roachpb.Span{Key: roachpb.Key("e1"), EndKey: roachpb.Key("f")},
			resume: roachpb.Span{Key: roachpb.Key("e2"), EndKey: roachpb.Key("f")}},
		// Work completed in the right of a span.
		{orig: roachpb.Span{Key: roachpb.Key("g1"), EndKey: roachpb.Key("h")},
			resume: roachpb.Span{}},
		// Work performed in the left of a span.
		{orig: roachpb.Span{Key: roachpb.Key("i"), EndKey: roachpb.Key("i2")},
			resume: roachpb.Span{Key: roachpb.Key("i1"), EndKey: roachpb.Key("i2")}},
		// Work completed in the left of a span.
		{orig: roachpb.Span{Key: roachpb.Key("k"), EndKey: roachpb.Key("k2")},
			resume: roachpb.Span{}},
		// Work performed on a span.
		{orig: roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("n")},
			resume: roachpb.Span{Key: roachpb.Key("m1"), EndKey: roachpb.Key("n")}},
		// Work completed on a span.
		{orig: roachpb.Span{Key: roachpb.Key("o"), EndKey: roachpb.Key("p")},
			resume: roachpb.Span{}},
	}
	for _, test := range testData {
		finished := test.orig
		if test.resume.Key != nil {
			finished.EndKey = test.resume.Key
		}
		if err := sql.TestingDescsTxn(ctx, server, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
			return TestingWriteResumeSpan(
				ctx,
				txn,
				keys.SystemSQLCodec,
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
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("a1")},
		{Key: roachpb.Key("a2"), EndKey: roachpb.Key("b")},
		// Work completed in the middle of a span.
		{Key: roachpb.Key("c"), EndKey: roachpb.Key("c1")},
		{Key: roachpb.Key("c2"), EndKey: roachpb.Key("d")},
		// Work performed in the right of a span.
		{Key: roachpb.Key("e"), EndKey: roachpb.Key("e1")},
		{Key: roachpb.Key("e2"), EndKey: roachpb.Key("f")},
		// Work completed in the right of a span.
		{Key: roachpb.Key("g"), EndKey: roachpb.Key("g1")},
		// Work performed in the left of a span.
		{Key: roachpb.Key("i1"), EndKey: roachpb.Key("j")},
		// Work completed in the left of a span.
		{Key: roachpb.Key("k2"), EndKey: roachpb.Key("l")},
		// Work performed on a span.
		{Key: roachpb.Key("m1"), EndKey: roachpb.Key("n")},
		// Work completed on a span; ["o", "p"] complete.
		{Key: roachpb.Key("q"), EndKey: roachpb.Key("r")},
	}

	var got []roachpb.Span
	if err := sql.TestingDescsTxn(ctx, server, func(ctx context.Context, txn isql.Txn, col *descs.Collection) (err error) {
		got, _, _, err = rowexec.GetResumeSpans(
			ctx, registry, txn, keys.SystemSQLCodec, col, tableDesc.ID, mutationID, backfill.IndexMutationFilter)
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
