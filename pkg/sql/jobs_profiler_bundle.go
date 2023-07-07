// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler/profilerconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/klauspost/compress/gzip"
)

const bundleChunkSize = 1024 * 1024 // 1 MiB

// RequestExecutionDetails implements the JobProfiler interface.
func (p *planner) RequestExecutionDetails(ctx context.Context, jobID jobspb.JobID) error {
	execCfg := p.ExecCfg()
	if !execCfg.Settings.Version.IsActive(ctx, clusterversion.V23_1) {
		return errors.Newf("execution details can only be requested on a cluster with version >= %s",
			clusterversion.V23_1.String())
	}

	e := MakeJobProfilerExecutionDetailsBuilder(execCfg.InternalDB, jobID)
	// TODO(adityamaru): When we start collecting more information we can consider
	// parallelize the collection of the various pieces.
	e.addDistSQLDiagram(ctx)

	return nil
}

// ExecutionDetailsBuilder can be used to read and write execution details corresponding
// to a job.
type ExecutionDetailsBuilder struct {
	db    isql.DB
	jobID jobspb.JobID
}

func compressChunk(chunkBuf []byte) ([]byte, error) {
	gzipBuf := bytes.NewBuffer([]byte{})
	gz := gzip.NewWriter(gzipBuf)
	if _, err := gz.Write(chunkBuf); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return gzipBuf.Bytes(), nil
}

// WriteExecutionDetail will break up data into chunks of a fixed size, and
// gzip compress them before writing them to the job_info table.
func (e *ExecutionDetailsBuilder) WriteExecutionDetail(
	ctx context.Context, filename string, data []byte,
) error {
	return e.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		jobInfo := jobs.InfoStorageForJob(txn, e.jobID)

		for len(data) > 0 {
			chunkSize := bundleChunkSize
			chunk := data
			if len(chunk) > chunkSize {
				chunk = chunk[:chunkSize]
			}
			data = data[len(chunk):]
			var err error
			chunk, err = compressChunk(chunk)
			if err != nil {
				return errors.Wrapf(err, "failed to compress chunk for file %s", filename)
			}

			// On listing we want the info_key of each chunk to sort after the
			// previous chunk of the same file so that the chunks can be reassembled
			// on download. For this reason we use a monotonically increasing
			// timestamp as the suffix.
			err = jobInfo.Write(ctx, profilerconstants.MakeProfilerBundleChunkKey(filename,
				timeutil.Now().UnixNano()), chunk)
			if err != nil {
				return errors.Wrapf(err, "failed to write chunk for file %s", filename)
			}
		}
		return nil
	})
}

// ReadExecutionDetail will stitch together all the chunks corresponding to the
// filename and return the uncompressed data of the file.
func (e *ExecutionDetailsBuilder) ReadExecutionDetail(
	ctx context.Context, filename string,
) ([]byte, error) {
	// TODO(adityamaru): If filename=all add logic to zip up all the files corresponding
	// to the job's execution details and return the zipped bundle instead.

	buf := bytes.NewBuffer([]byte{})
	if err := e.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Reset the buf inside the txn closure to guard against txn retries.
		buf.Reset()
		jobInfo := jobs.InfoStorageForJob(txn, e.jobID)

		// Iterate over all the chunks of the requested file and return the unzipped
		// chunks of data.
		if err := jobInfo.Iterate(ctx, profilerconstants.MakeProfilerExecutionDetailsChunkKeyPrefix(filename),
			func(infoKey string, value []byte) error {
				log.Infof(ctx, "infoKey %s", infoKey)
				r, err := gzip.NewReader(bytes.NewBuffer(value))
				if err != nil {
					return err
				}
				decompressed, err := io.ReadAll(r)
				if err != nil {
					return err
				}
				buf.Write(decompressed)
				return nil
			}); err != nil {
			return errors.Wrapf(err, "failed to iterate over chunks for job %d", jobID)
		}

		return nil
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MakeJobProfilerExecutionDetailsBuilder returns an instance of an ExecutionDetailsBuilder.
func MakeJobProfilerExecutionDetailsBuilder(
	db isql.DB, jobID jobspb.JobID,
) ExecutionDetailsBuilder {
	e := ExecutionDetailsBuilder{
		db: db, jobID: jobID,
	}
	return e
}

// addDistSQLDiagram generates and persists a `distsql.<timestamp>.html` file.
func (e *ExecutionDetailsBuilder) addDistSQLDiagram(ctx context.Context) {
	query := `SELECT plan_diagram FROM [SHOW JOB $1 WITH EXECUTION DETAILS]`
	row, err := e.db.Executor().QueryRowEx(ctx, "profiler-bundler-add-diagram", nil, /* txn */
		sessiondata.NoSessionDataOverride, query, e.jobID)
	if err != nil {
		log.Errorf(ctx, "failed to write DistSQL diagram for job %d: %+v", e.jobID, err.Error())
		return
	}
	if row[0] != tree.DNull {
		dspDiagramURL := string(tree.MustBeDString(row[0]))
		filename := fmt.Sprintf("distsql.%s.html", timeutil.Now().Format("20060102_150405"))
		if err := e.WriteExecutionDetail(ctx, filename,
			[]byte(fmt.Sprintf(`<meta http-equiv="Refresh" content="0; url=%s">`, dspDiagramURL))); err != nil {
			log.Errorf(ctx, "failed to write DistSQL diagram for job %d: %+v", e.jobID, err.Error())
		}
	}
}
