// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package utilccl

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// BulkJobExecutionResultHeader is the header for various job commands
// (BACKUP, RESTORE, IMPORT, etc) stmt results.
var BulkJobExecutionResultHeader = colinfo.ResultColumns{
	{Name: "job_id", Typ: types.Int},
	{Name: "status", Typ: types.String},
	{Name: "fraction_completed", Typ: types.Float},
	{Name: "rows", Typ: types.Int},
	{Name: "index_entries", Typ: types.Int},
	{Name: "bytes", Typ: types.Int},
}

// DetachedJobExecutionResultHeader is a the header for various job commands when
// job executes in detached mode (i.e. the caller doesn't wait for job completion).
var DetachedJobExecutionResultHeader = colinfo.ResultColumns{
	{Name: "job_id", Typ: types.Int},
}
