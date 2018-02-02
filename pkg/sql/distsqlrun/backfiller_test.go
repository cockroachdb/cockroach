// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun_test

import (
	"context"
	"testing"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestWriteResumeSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()

	server, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			// Disable all schema change execution.
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				SyncFilter: func(tscc sql.TestingSchemaChangerCollection) {
					tscc.ClearSchemaChangers()
				},
				AsyncExecNotification: func() error {
					return errors.New("async schema changer disabled")
				},
			},
			// Disable backfill migrations, we still need the jobs table migration.
			SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
				DisableBackfillMigrations: true,
			},
		},
	})
	defer server.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
	CREATE DATABASE t;
	CREATE TABLE t.public.test (k INT PRIMARY KEY, v INT);
	CREATE UNIQUE INDEX vidx ON t.public.test (v);
	`); err != nil {
		t.Fatal(err)
	}

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
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	if err := kvDB.Put(
		ctx,
		sqlbase.MakeDescMetadataKey(tableDesc.ID),
		sqlbase.WrapDescriptor(tableDesc),
	); err != nil {
		t.Fatal(err)
	}

	mutationID := tableDesc.Mutations[0].MutationID
	var jobID int64

	if len(tableDesc.MutationJobs) > 0 {
		for _, job := range tableDesc.MutationJobs {
			if job.MutationID == mutationID {
				jobID = job.JobID
				break
			}
		}
	}

	details := jobs.SchemaChangeDetails{ResumeSpanList: []jobs.ResumeSpanList{
		{ResumeSpans: resumeSpans}}}

	job, err := registry.LoadJob(ctx, jobID)

	if err != nil {
		t.Fatal(errors.Wrapf(err, "can't find job %d", jobID))
	}

	err = job.SetDetails(ctx, details)
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
		if err := distsqlrun.WriteResumeSpan(
			ctx, kvDB, tableDesc.ID, test.orig, test.resume, 0, registry,
		); err != nil {
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

	got, err := distsqlrun.GetResumeSpansFromJob(ctx, registry, nil, jobID, 0)
	if err != nil {
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
