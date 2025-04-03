// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"archive/zip"
	"context"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// A special job Resumer that records a structured span recording during
// execution.
var _ jobs.Resumer = &traceSpanResumer{}
var _ jobs.TraceableJob = &traceSpanResumer{}

func (r *traceSpanResumer) ForceRealSpan() bool {
	return true
}

func (r *traceSpanResumer) DumpTraceAfterRun() bool {
	return true
}

type traceSpanResumer struct {
	ctx               context.Context
	recordedSpanCh    chan struct{}
	completeResumerCh chan struct{}
}

func (r *traceSpanResumer) Resume(ctx context.Context, _ interface{}) error {
	_, span := tracing.ChildSpan(ctx, "trace test")
	defer span.Finish()
	// Picked a random proto message that was simple to match output against.
	span.RecordStructured(&serverpb.TableStatsRequest{Database: "foo", Table: "bar"})
	r.recordedSpanCh <- struct{}{}
	<-r.completeResumerCh
	return nil
}

func (r *traceSpanResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}, _ error) error {
	return errors.New("unimplemented")
}

// CollectProfile implements the jobs.Resumer interface.
func (r *traceSpanResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func TestDebugJobTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	ctx := context.Background()
	argsFn := func(args *base.TestServerArgs) {
		args.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	}

	c := newCLITestWithArgs(TestCLIParams{T: t}, argsFn)
	defer c.Cleanup()
	c.omitArgs = true

	registry := c.Server.JobRegistry().(*jobs.Registry)
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	completeResumerCh := make(chan struct{})
	recordedSpanCh := make(chan struct{})
	defer close(completeResumerCh)
	defer close(recordedSpanCh)

	defer jobs.TestingRegisterConstructor(
		jobspb.TypeBackup,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &traceSpanResumer{
				ctx:               jobCtx,
				completeResumerCh: completeResumerCh,
				recordedSpanCh:    recordedSpanCh,
			}
		},
		jobs.UsesTenantCostControl,
	)()

	// Create a "backup job" but we have overridden the resumer constructor above
	// to inject our traceSpanResumer.
	var job *jobs.StartableJob
	id := registry.MakeJobID()
	require.NoError(t, c.Server.InternalDB().(isql.DB).Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) (err error) {
		err = registry.CreateStartableJobWithTxn(ctx, &job, id, txn, jobs.Record{
			Username: username.RootUserName(),
			Details:  jobspb.BackupDetails{},
			Progress: jobspb.BackupProgress{},
		})
		return err
	}))

	require.NoError(t, job.Start(ctx))

	// Wait for the job to record information in the trace span.
	<-recordedSpanCh

	args := []string{strconv.Itoa(int(id))}
	pgURL, cleanup := pgurlutils.PGUrl(t, c.Server.AdvSQLAddr(),
		"TestDebugJobTrace", url.User(username.RootUser))
	defer cleanup()

	_, err := c.RunWithCaptureArgs([]string{`debug`, `job-trace`, args[0], fmt.Sprintf(`--url=%s`, pgURL.String()), `--format=csv`})
	require.NoError(t, err)
	checkBundle(t, id, "node1-trace.txt", "node1-jaeger.json")
}

func checkBundle(t *testing.T, jobID jobspb.JobID, expectedFiles ...string) {
	t.Helper()

	filename := fmt.Sprintf("%d-%s", jobID, jobTraceZipSuffix)
	defer func() {
		_ = os.Remove(filename)
	}()
	r, err := zip.OpenReader(filename)
	require.NoError(t, err)

	// Make sure the bundle contains the expected list of files.
	var files []string
	for _, f := range r.File {
		if f.UncompressedSize64 == 0 {
			t.Fatalf("file %s is empty", f.Name)
		}
		files = append(files, f.Name)
	}

	var expList []string
	for _, s := range expectedFiles {
		expList = append(expList, strings.Split(s, " ")...)
	}
	sort.Strings(files)
	sort.Strings(expList)
	if fmt.Sprint(files) != fmt.Sprint(expList) {
		t.Errorf("unexpected list of files:\n  %v\nexpected:\n  %v", files, expList)
	}
}
