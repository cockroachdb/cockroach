// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobsprofiler

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
	"github.com/cockroachdb/cockroach/pkg/sql"
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

	// Stores any error in the collection, building, or insertion of the bundle.
	collectionErr error

	jobID jobspb.JobID
}

// buildProfilerBundle collects metadata related to the execution of the job. It
// generates a bundle for storage in system.job_info.
func buildProfilerBundle(
	ctx context.Context,
	db isql.DB,
	ie *sql.InternalExecutor,
	st *cluster.Settings,
	jobID jobspb.JobID,
) profilerBundle {
	b := makeProfilerBundleBuilder(db, ie, jobID)

	b.addDistSQLDiagram(ctx)

	buf, err := b.finalize()
	if err != nil {
		return profilerBundle{collectionErr: err}
	}
	return profilerBundle{zip: buf.Bytes(), st: st, db: db, jobID: jobID}
}

func (b *profilerBundle) insert(
	ctx context.Context,
) error {
	// Generate a unique ID for the profiler bundle.
	id := uuid.MakeV4()
	return b.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		jobInfo := jobs.InfoStorageForJob(txn, b.jobID)

		chunkID := 0
		for len(b.zip) > 0 {
			chunkSize := int(bundleChunkSize.Get(&b.st.SV))
			chunk := b.zip
			if len(chunk) > chunkSize {
				chunk = chunk[:chunkSize]
			}
			b.zip = b.zip[len(chunk):]

			err := jobInfo.Write(ctx, profilerconstants.MakeProfilerBundleChunkKey(id.String(), chunkID), chunk)
			if err != nil {
				return errors.Wrapf(err, "failed to write profiler bundle chunk %d", chunkID)
			}
			chunkID++
		}

		// Now that we have inserted all the bundle chunks, write a row describing
		// the bundle we've collected.
		md := profilerpb.ProfilerBundleMetadata{
			BundleID:    id,
			NumChunks:   int32(chunkID),
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
	ie *sql.InternalExecutor

	jobID jobspb.JobID

	z memzipper.Zipper
}

func makeProfilerBundleBuilder(
	db isql.DB,
	ie *sql.InternalExecutor,
	jobID jobspb.JobID,
) profilerBundleBuilder {
	b := profilerBundleBuilder{
		db: db, ie: ie, jobID: jobID,
	}
	b.z.Init()
	return b
}

func (b *profilerBundleBuilder) addDistSQLDiagram(ctx context.Context) {
	query := `SELECT plan_diagram FROM [SHOW JOB $1 WITH EXECUTION DETAILS]`
	row, err := b.ie.QueryRowEx(ctx, "profiler-bundler-add-diagram", nil, /* txn */
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
