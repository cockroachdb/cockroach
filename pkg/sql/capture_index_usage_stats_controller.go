// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// CaptureIndexUsageStatsController implements the SQL Stats subsystem control plane. This exposes
// administrative interfaces that can be consumed by other parts of the database
// (e.g. status server, builtins) to control the behavior of the SQL Stats
// subsystem.
type CaptureIndexUsageStatsController struct {
	db *kv.DB
	ie sqlutil.InternalExecutor
	st *cluster.Settings
}

// NewCaptureIndexUsageStatsController returns a new instance of CaptureIndexUsageStatsController.
func NewCaptureIndexUsageStatsController(
	db *kv.DB,
	ie sqlutil.InternalExecutor,
	st *cluster.Settings,
) *CaptureIndexUsageStatsController {
	return &CaptureIndexUsageStatsController{
		db: db,
		ie: ie,
		st: st,
	}
}

// CreateCaptureIndexUsageStatsSchedule implements the tree.CaptureIndexUsageStatsController
// interface.
func (s CaptureIndexUsageStatsController) CreateCaptureIndexUsageStatsSchedule(ctx context.Context) error {
	return s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := CreateCaptureIndexUsageStatsScheduleJob(ctx, s.ie, txn, s.st)
		return err
	})
}
