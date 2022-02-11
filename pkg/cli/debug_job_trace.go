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

	sqlConn, err := makeSQLClient("cockroach debug job-trace", useSystemDb)
	if err != nil {
		return errors.Wrap(err, "could not establish connection to cluster")
	}
	defer func() { resErr = errors.CombineErrors(resErr, sqlConn.Close()) }()

	return constructJobTraceZipBundle(context.Background(), sqlConn, jobID)
}

func getJobTraceID(sqlConn clisqlclient.Conn, jobID int64) (int64, error) {
	var traceID int64
	rows, err := sqlConn.Query(context.Background(),
		`SELECT trace_id FROM crdb_internal.jobs WHERE job_id=$1`, jobID)
	if err != nil {
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
		return traceID, errors.Newf("no job entry found for %d", jobID)
	}
	var ok bool
	traceID, ok = vals[0].(int64)
	if !ok {
		return traceID, errors.New("failed to parse traceID")
	}
	return traceID, nil
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

	zipper := tracezipper.MakeSQLConnInflightTraceZipper(sqlConn.GetDriverConn().(driver.QueryerContext))
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
