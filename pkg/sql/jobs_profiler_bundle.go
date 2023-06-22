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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler/profilerconstants"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler/profilerpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/memzipper"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

var bundleChunkSize = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"jobs.profiler.bundle_chunk_size",
	"chunk size for jobs profiler diagnostic bundles",
	1024*1024,
	func(val int64) error {
		if val < 16 {
			return errors.Errorf("chunk size must be at least 16 bytes")
		}
		return nil
	},
)

// profilerBundle contains diagnostics information collected for a job.
type profilerBundle struct {
	// Zip file binary data.
	zip []byte

	st *cluster.Settings
	db isql.DB

	jobID jobspb.JobID
}

// GenerateBundle implements the JobProfiler interface.
func (p *planner) GenerateBundle(ctx context.Context, jobID jobspb.JobID) error {
	execCfg := p.ExecCfg()
	bundle, err := buildProfilerBundle(ctx, execCfg.InternalDB, execCfg.Settings, jobID)
	if err != nil {
		return err
	}
	return bundle.insert(ctx)
}

// buildProfilerBundle collects metadata related to the execution of the job. It
// generates a bundle for storage in system.job_info.
func buildProfilerBundle(
	ctx context.Context, db isql.DB, st *cluster.Settings, jobID jobspb.JobID,
) (profilerBundle, error) {
	b := makeProfilerBundleBuilder(db, jobID)

	// TODO(adityamaru): add traces, aggregated stats, per-component progress to
	// the bundle.
	b.addDistSQLDiagram(ctx)

	buf, err := b.finalize()
	if err != nil {
		return profilerBundle{}, err
	}
	return profilerBundle{zip: buf.Bytes(), st: st, db: db, jobID: jobID}, nil
}

// insert breaks the profiler bundle into chunks and writes these chunks to the
// `system.job_info` table. Once all the chunks have been written, this method
// write a metadata row describing the bundle.
func (b *profilerBundle) insert(ctx context.Context) error {
	// Generate a unique ID for the profiler bundle.
	id := uuid.MakeV4()
	return b.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		jobInfo := jobs.InfoStorageForJob(txn, b.jobID)

		var numChunks int
		for len(b.zip) > 0 {
			chunkSize := int(bundleChunkSize.Get(&b.st.SV))
			chunk := b.zip
			if len(chunk) > chunkSize {
				chunk = chunk[:chunkSize]
			}
			b.zip = b.zip[len(chunk):]

			err := jobInfo.Write(ctx, profilerconstants.MakeProfilerBundleChunkKey(
				id.String(), timeutil.Now().UnixNano()), chunk)
			if err != nil {
				return errors.Wrap(err, "failed to write profiler bundle chunk")
			}
			numChunks++
		}

		// Now that we have inserted all the bundle chunks, write a row describing
		// the bundle we've collected.
		md := profilerpb.ProfilerBundleMetadata{
			BundleID:    id,
			NumChunks:   int32(numChunks),
			CollectedAt: timeutil.Now(),
		}
		mdBytes, err := protoutil.Marshal(&md)
		if err != nil {
			return err
		}
		if err := jobInfo.Write(ctx, profilerconstants.MakeProfilerBundleMetadataKey(id.String()), mdBytes); err != nil {
			return errors.Wrap(err, "failed to write profiler bundle metadata")
		}
		return nil
	})
}

type profilerBundleBuilder struct {
	db isql.DB

	jobID jobspb.JobID

	z memzipper.Zipper
}

func makeProfilerBundleBuilder(db isql.DB, jobID jobspb.JobID) profilerBundleBuilder {
	b := profilerBundleBuilder{
		db: db, jobID: jobID,
	}
	b.z.Init()
	return b
}

func (b *profilerBundleBuilder) addDistSQLDiagram(ctx context.Context) {
	query := `SELECT plan_diagram FROM [SHOW JOB $1 WITH EXECUTION DETAILS]`
	row, err := b.db.Executor().QueryRowEx(ctx, "profiler-bundler-add-diagram", nil, /* txn */
		sessiondata.NoSessionDataOverride, query, b.jobID)
	if err != nil {
		b.z.AddFile("distsql.error", err.Error())
	}
	if row[0] != tree.DNull {
		dspDiagramURL := string(tree.MustBeDString(row[0]))
		b.z.AddFile("distsql.html",
			fmt.Sprintf(`<meta http-equiv="Refresh" content="0; url=%s">`, dspDiagramURL))
	}
}

// finalize generates the zipped bundle and returns it as a buffer.
func (b *profilerBundleBuilder) finalize() (*bytes.Buffer, error) {
	return b.z.Finalize()
}
