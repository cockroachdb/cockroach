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
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// kvScanNode used for planning distributed execution
// of raw KV scans.
type kvScanNode struct {
	optColumnsSlot
	span roachpb.Span
}

// startExec is part of the planNode interface.
func (e *kvScanNode) startExec(params runParams) error {
	panic("kvScanNode cannot be run in local mode")
}

// Next is part of the planNode interface.
func (e *kvScanNode) Next(params runParams) (bool, error) {
	panic("kvScanNode cannot be run in local mode")
}

// Values is part of the planNode interface.
func (e *kvScanNode) Values() tree.Datums {
	panic("kvScanNode cannot be run in local mode")
}

// Close is part of the planNode interface.
func (e *kvScanNode) Close(ctx context.Context) {
}
