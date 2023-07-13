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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler/profilerconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/klauspost/compress/gzip"
)

const bundleChunkSize = 1 << 20 // 1 MiB
const finalChunkSuffix = "#_final"

// RequestExecutionDetails implements the JobProfiler interface.
func (p *planner) RequestExecutionDetails(ctx context.Context, jobID jobspb.JobID) error {
	execCfg := p.ExecCfg()
	if !execCfg.Settings.Version.IsActive(ctx, clusterversion.V23_1) {
		return errors.Newf("execution details can only be requested on a cluster with version >= %s",
			clusterversion.V23_1.String())
	}

	e := MakeJobProfilerExecutionDetailsBuilder(execCfg.SQLStatusServer, execCfg.InternalDB, jobID)
	// TODO(adityamaru): When we start collecting more information we can consider
	// parallelize the collection of the various pieces.
	e.addDistSQLDiagram(ctx)
	e.addLabelledGoroutines(ctx)

	return nil
}

// ExecutionDetailsBuilder can be used to read and write execution details corresponding
// to a job.
type ExecutionDetailsBuilder struct {
	srv   serverpb.SQLStatusServer
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
		// Take a copy of the data to operate on inside the txn closure.
		chunkData := data[:]
		jobInfo := jobs.InfoStorageForJob(txn, e.jobID)

		var chunkCounter int
		var chunkName string
		for len(chunkData) > 0 {
			chunkSize := bundleChunkSize
			chunk := chunkData
			if len(chunk) > chunkSize {
				chunkName = fmt.Sprintf("%s#%04d", filename, chunkCounter)
				chunk = chunk[:chunkSize]
			} else {
				// This is the last chunk we will write, assign it a sentinel file name.
				chunkName = filename + finalChunkSuffix
			}
			chunkData = chunkData[len(chunk):]
			var err error
			chunk, err = compressChunk(chunk)
			if err != nil {
				return errors.Wrapf(err, "failed to compress chunk for file %s", filename)
			}

			// On listing we want the info_key of each chunk to sort after the
			// previous chunk of the same file so that the chunks can be reassembled
			// on download. For this reason we use a monotonically increasing
			// chunk counter as the suffix.
			err = jobInfo.Write(ctx, profilerconstants.MakeProfilerExecutionDetailsChunkKey(chunkName), chunk)
			if err != nil {
				return errors.Wrapf(err, "failed to write chunk for file %s", filename)
			}
			chunkCounter++
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
		var lastInfoKey string
		if err := jobInfo.Iterate(ctx, profilerconstants.MakeProfilerExecutionDetailsChunkKeyPrefix(filename),
			func(infoKey string, value []byte) error {
				lastInfoKey = infoKey
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

		if lastInfoKey != "" && !strings.Contains(lastInfoKey, finalChunkSuffix) {
			return errors.Newf("failed to read all chunks for file %s, last info key read was %s", filename, lastInfoKey)
		}

		return nil
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ListExecutionDetailFiles lists all the files that have been generated as part
// of a job's execution details.
func (e *ExecutionDetailsBuilder) ListExecutionDetailFiles(ctx context.Context) ([]string, error) {
	var res []string
	if err := e.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		jobInfo := jobs.InfoStorageForJob(txn, e.jobID)

		// Iterate over all the files that have been stored as part of the job's
		// execution details.
		files := make([]string, 0)
		if err := jobInfo.Iterate(ctx, profilerconstants.ExecutionDetailsChunkKeyPrefix,
			func(infoKey string, value []byte) error {
				// Look for the final chunk of each file to find the unique file name.
				if strings.HasSuffix(infoKey, finalChunkSuffix) {
					files = append(files, strings.TrimSuffix(infoKey, finalChunkSuffix))
				}
				return nil
			}); err != nil {
			return errors.Wrapf(err, "failed to iterate over execution detail files for job %d", jobID)
		}
		res = files
		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

// MakeJobProfilerExecutionDetailsBuilder returns an instance of an ExecutionDetailsBuilder.
func MakeJobProfilerExecutionDetailsBuilder(
	srv serverpb.SQLStatusServer, db isql.DB, jobID jobspb.JobID,
) ExecutionDetailsBuilder {
	e := ExecutionDetailsBuilder{
		srv: srv, db: db, jobID: jobID,
	}
	return e
}

// addLabelledGoroutines collects and persists goroutines from all nodes in the
// cluster that have a pprof label tying it to the job whose execution details
// are being collected.
func (e *ExecutionDetailsBuilder) addLabelledGoroutines(ctx context.Context) {
	profileRequest := serverpb.ProfileRequest{
		NodeId:      "all",
		Type:        serverpb.ProfileRequest_GOROUTINE,
		Labels:      true,
		LabelFilter: fmt.Sprintf("%d", e.jobID),
	}
	resp, err := e.srv.Profile(ctx, &profileRequest)
	if err != nil {
		log.Errorf(ctx, "failed to collect goroutines for job %d: %+v", e.jobID, err.Error())
		return
	}
	filename := fmt.Sprintf("goroutines.%s.txt", timeutil.Now().Format("20060102_150405.00"))
	if err := e.WriteExecutionDetail(ctx, filename, resp.Data); err != nil {
		log.Errorf(ctx, "failed to write goroutine for job %d: %+v", e.jobID, err.Error())
	}
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
		filename := fmt.Sprintf("distsql.%s.html", timeutil.Now().Format("20060102_150405.00"))
		if err := e.WriteExecutionDetail(ctx, filename,
			[]byte(fmt.Sprintf(`<meta http-equiv="Refresh" content="0; url=%s">`, dspDiagramURL))); err != nil {
			log.Errorf(ctx, "failed to write DistSQL diagram for job %d: %+v", e.jobID, err.Error())
		}
	}
}
