// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// delegateShowRanges implements the SHOW REGIONS statement.
func (d *delegator) delegateShowRegions(n *tree.ShowRegions) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Regions)
	return nil, unimplemented.New("show regions", "implementation pending")
}
