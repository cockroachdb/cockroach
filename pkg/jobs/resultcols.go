// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// BulkJobExecutionResultHeader is the header for various bulk job commands
// stmt results. IMPORT uses ImportJobExecutionResultHeader instead.
var BulkJobExecutionResultHeader = colinfo.ResultColumns{
	{Name: "job_id", Typ: types.Int},
	{Name: "status", Typ: types.String},
	{Name: "fraction_completed", Typ: types.Float},
	{Name: "rows", Typ: types.Int},
	{Name: "index_entries", Typ: types.Int},
	{Name: "bytes", Typ: types.Int},
}

// ImportJobExecutionResultHeader is the header for IMPORT stmt results. It
// extends BulkJobExecutionResultHeader with an inspect_job_id column that
// reports the ID of the background INSPECT job triggered for row count
// validation, if any.
var ImportJobExecutionResultHeader = colinfo.ResultColumns{
	{Name: "job_id", Typ: types.Int},
	{Name: "status", Typ: types.String},
	{Name: "fraction_completed", Typ: types.Float},
	{Name: "rows", Typ: types.Int},
	{Name: "index_entries", Typ: types.Int},
	{Name: "bytes", Typ: types.Int},
	{Name: "inspect_job_id", Typ: types.Int},
}

var BackupRestoreJobResultHeader = colinfo.ResultColumns{
	{Name: "job_id", Typ: types.Int},
	{Name: "status", Typ: types.String},
	{Name: "fraction_completed", Typ: types.Float},
	{Name: "rows", Typ: types.Int},
}

// OnlineRestoreJobExecutionResultHeader is the header for an online restore
// job, which provides a header different from the usual bulk job execution
var OnlineRestoreJobExecutionResultHeader = colinfo.ResultColumns{
	{Name: "job_id", Typ: types.Int},
	{Name: "tables", Typ: types.Int},
	{Name: "approx_rows", Typ: types.Int},
	{Name: "approx_bytes", Typ: types.Int},
	{Name: "background_download_job_id", Typ: types.Int},
}

// InspectJobExecutionResultHeader is the header for INSPECT job results.
var InspectJobExecutionResultHeader = colinfo.ResultColumns{
	{Name: "job_id", Typ: types.Int},
	{Name: "status", Typ: types.String},
}

// DetachedJobExecutionResultHeader is the header for various job commands when
// job executes in detached mode (i.e. the caller doesn't wait for job completion).
var DetachedJobExecutionResultHeader = colinfo.ResultColumns{
	{Name: "job_id", Typ: types.Int},
}
