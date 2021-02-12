// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package streaming

import (
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// CompleteIngestionHook is the hook run by the
// crdb_internal.complete_stream_ingestion_job builtin.
// It is used to signal to a running stream ingestion job to stop ingesting data
// and eventually move to a consistent state as of the latest resolved
// timestamp.
var CompleteIngestionHook func(*tree.EvalContext, *kv.Txn, int, hlc.Timestamp) error
