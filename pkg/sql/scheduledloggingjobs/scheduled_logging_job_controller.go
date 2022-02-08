// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scheduledloggingjobs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// Controller is an interface that starts all the scheduled jobs that are
// available to its interface.
type Controller interface {
	// Start creates/starts all the scheduled jobs this controller can
	// create.
	Start(ctx context.Context)
	// The unexported function(s) designate the jobs that this controller is
	// expected to create/start, but are only intended to be called by the
	// Controller.
	createCaptureIndexUsageStatsScheduledJob(ctx context.Context) error
}

// LoggingJobsController exposes administrative interfaces that can be consumed
// by other parts of the database (e.g. status server, builtins) to interact
// with the scheduled logging job system.
type LoggingJobsController struct {
	db *kv.DB
	ie sqlutil.InternalExecutor
	cs *cluster.Settings
}

// NewLoggingJobController returns a new instance of LoggingJobController.
func NewLoggingJobController(
	db *kv.DB, ie sqlutil.InternalExecutor, cs *cluster.Settings,
) *LoggingJobsController {
	return &LoggingJobsController{
		db: db,
		ie: ie,
		cs: cs,
	}
}

// Start creates all scheduled logging jobs. Start implements the
// sql.Controller interface.
func (c *LoggingJobsController) Start(ctx context.Context) {
	err := c.createCaptureIndexUsageStatsScheduledJob(ctx)
	if err != nil {
		panic(err)
	}
}

// createCaptureIndexUsageStatsScheduledJob creates all scheduled logging jobs.
// createCaptureIndexUsageStatsScheduledJob implements the sql.Controller interface.
func (c *LoggingJobsController) createCaptureIndexUsageStatsScheduledJob(
	ctx context.Context,
) error {
	err := c.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		err := CreateCaptureIndexUsageStatsScheduledJob(ctx, c.ie, txn, c.cs)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}
