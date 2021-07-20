// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

// A special jobs.Resumer that, instead of finishing
// the job successfully, forces the job to be paused.
var _ jobs.Resumer = &traceSpanResumer{}
var _ jobs.TraceableJob = &traceSpanResumer{}

func (r *traceSpanResumer) ForceRealSpan() {}

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

func (r *traceSpanResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	return errors.New("unimplemented")
}

// Header from the output of `cockroach debug job-trace`.
var jobTraceHeader = []string{
	"span_id", "goroutine_id", "operation", "start_time", "duration", "payload_type", "payload_jsonb",
}

func TestDebugJobTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	defer jobs.TestingSetAdoptAndCancelIntervals(100*time.Millisecond, 100*time.Millisecond)()

	c := NewCLITest(TestCLIParams{T: t})
	defer c.Cleanup()
	c.omitArgs = true

	registry := c.TestServer.JobRegistry().(*jobs.Registry)
	jobCtx, _ := context.WithCancel(ctx)
	completeResumerCh := make(chan struct{})
	recordedSpanCh := make(chan struct{})
	defer close(completeResumerCh)
	defer close(recordedSpanCh)

	jobs.RegisterConstructor(
		jobspb.TypeBackup,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &traceSpanResumer{
				ctx:               jobCtx,
				completeResumerCh: completeResumerCh,
				recordedSpanCh:    recordedSpanCh,
			}
		},
	)

	// Create a "backup job" but we have overridden the resumer constructor above
	// to inject our traceSpanResumer.
	var job *jobs.StartableJob
	id := registry.MakeJobID()
	require.NoError(t, c.TestServer.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		err = registry.CreateStartableJobWithTxn(ctx, &job, id, txn, jobs.Record{
			Username: security.RootUserName(),
			Details:  jobspb.BackupDetails{},
			Progress: jobspb.BackupProgress{},
		})
		return err
	}))

	require.NoError(t, job.Start(ctx))

	// Wait for the job to record information in the trace span.
	<-recordedSpanCh

	tempDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	targetFilePath := filepath.Join(tempDir, "tracetest")
	args := []string{strconv.Itoa(int(id)), targetFilePath}
	pgURL, _ := sqlutils.PGUrl(t, c.TestServer.ServingSQLAddr(),
		"TestDebugJobTrace", url.User(security.RootUser))

	_, err := c.RunWithCaptureArgs([]string{`debug`, `job-trace`, args[0], args[1], fmt.Sprintf(`--url=%s`, pgURL.String()), `--format=csv`})
	require.NoError(t, err)
	actual, err := ioutil.ReadFile(targetFilePath)
	if err != nil {
		t.Errorf("Failed to read actual result from %v: %v", targetFilePath, err)
	}

	if err := matchCSVHeader(string(actual), jobTraceHeader); err != nil {
		t.Fatal(err)
	}

	operationName := fmt.Sprintf("BACKUP-%d", id)
	exp := [][]string{
		// This is the span recording we injected above.
		{`\d+`, `\d+`, operationName, ".*", ".*", "server.serverpb.TableStatsRequest", "{\"@type\": \"type.googleapis.com/cockroach.server.serverpb.TableStatsRequest\", \"database\": \"foo\", \"table\": \"bar\"}"},
	}
	if err := MatchCSV(string(actual), exp); err != nil {
		t.Fatal(err)
	}
}

func matchCSVHeader(csvStr string, expectedHeader []string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Errorf("csv input:\n%v\nexpected:\n%s\nerrors:%s",
				csvStr, pretty.Sprint(expectedHeader), err)
		}
	}()

	csvStr = ElideInsecureDeprecationNotice(csvStr)
	reader := csv.NewReader(strings.NewReader(csvStr))
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	if len(records) < 1 {
		return errors.Errorf("csv is empty")
	}

	// Only match the first record, i.e. the expectedHeader.
	headerRow := records[0]
	if lr, lm := len(headerRow), len(expectedHeader); lr != lm {
		return errors.Errorf("csv header has %d columns, but expected %d", lr, lm)
	}
	for j := range expectedHeader {
		exp, act := expectedHeader[j], headerRow[j]
		if exp != act {
			err = errors.Errorf("found %q which does not match %q", act, exp)
		}
	}
	return err
}
