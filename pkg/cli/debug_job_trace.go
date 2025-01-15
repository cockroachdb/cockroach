// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	tracezipper "github.com/cockroachdb/cockroach/pkg/util/tracing/zipper"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugJobTraceFromClusterCmd = &cobra.Command{
	Use:   "job-trace <job_id> --url=<cluster connection string>",
	Short: "get the trace payloads for the executing job",
	Args:  cobra.MinimumNArgs(1),
	RunE:  clierrorplus.MaybeDecorateError(runDebugJobTrace),
}

const jobTraceZipSuffix = "job-trace.zip"

func runDebugJobTrace(_ *cobra.Command, args []string) (resErr error) {
	jobID, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return err
	}
	ctx := context.Background()
	sqlConn, err := makeSQLClient(ctx, "cockroach debug job-trace", useSystemDb)
	if err != nil {
		return errors.Wrap(err, "could not establish connection to cluster")
	}
	defer func() { resErr = errors.CombineErrors(resErr, sqlConn.Close()) }()

	return constructJobTraceZipBundle(ctx, sqlConn, jobID)
}

func getJobTraceID(sqlConn clisqlclient.Conn, jobID int64) (int64, error) {
	var traceID int64
	// We normally avoid programatic access to the job's message log, but this is
	// a debug command so we can allow it here.
	for attempt, query := range []string{
		`SELECT message::int FROM system.job_message WHERE job_id=$1 AND kind = 'trace-id' ORDER BY written DESC LIMIT 1`,
		`SELECT trace_id FROM crdb_internal.jobs WHERE job_id=$1`,
	} {
		rows, err := sqlConn.Query(context.Background(), query, jobID)
		if err != nil {
			// Cluster might not be on 25.1 yet; just fallback to old query.
			if attempt == 0 {
				continue
			}
			return traceID, err
		}
		vals := make([]driver.Value, 1)
		for {
			var err error
			if err = rows.Next(vals); err == io.EOF {
				break
			}
			if err != nil {
				return traceID, err
			}
		}
		if err := rows.Close(); err != nil {
			return traceID, err
		}
		if vals[0] == nil {
			continue
		}
		var ok bool
		traceID, ok = vals[0].(int64)
		if !ok {
			return traceID, errors.New("failed to parse traceID")
		}
		return traceID, nil
	}
	return traceID, errors.Newf("no job entry found for %d", jobID)
}

func constructJobTraceZipBundle(ctx context.Context, sqlConn clisqlclient.Conn, jobID int64) error {
	// Check if a timeout has been set for this command.
	if cliCtx.cmdTimeout != 0 {
		if err := sqlConn.Exec(context.Background(),
			`SET statement_timeout = $1`, cliCtx.cmdTimeout.String()); err != nil {
			return err
		}
	}

	traceID, err := getJobTraceID(sqlConn, jobID)
	if err != nil {
		return err
	}

	zipper := tracezipper.MakeSQLConnInflightTraceZipper(sqlConn.GetDriverConn())
	zipBytes, err := zipper.Zip(ctx, traceID)
	if err != nil {
		return err
	}

	var f *os.File
	filename := fmt.Sprintf("%d-%s", jobID, jobTraceZipSuffix)
	if f, err = os.Create(filename); err != nil {
		return err
	}
	_, err = f.Write(zipBytes)
	if err != nil {
		return err
	}
	defer f.Close()
	return nil
}
