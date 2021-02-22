// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowPayloadsForTrace(
	n *tree.ShowPayloadsForTrace,
) (tree.Statement, error) {
	// TODO(angelapwen): Do we want telemetry?

	sqlStmt := fmt.Sprintf(
		`WITH spans AS (
        SELECT span_id
        FROM crdb_internal.node_inflight_trace_spans
        WHERE trace_id=%v
      ) SELECT *
        FROM spans, LATERAL crdb_internal.payloads_for_span(spans.span_id)
    `, n.TraceID)

	return parse(sqlStmt)
}
