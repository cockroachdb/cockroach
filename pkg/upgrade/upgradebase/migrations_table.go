// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package upgradebase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// !!! comment
func MarkMigrationCompleted(
	ctx context.Context, ie sqlutil.InternalExecutor, v roachpb.Version,
) error {
	_, err := ie.ExecEx(
		ctx,
		"migration-job-mark-job-succeeded",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`
INSERT
  INTO system.migrations
        (
            major,
            minor,
            patch,
            internal,
            completed_at
        )
VALUES ($1, $2, $3, $4, $5)`,
		v.Major,
		v.Minor,
		v.Patch,
		v.Internal,
		timeutil.Now())
	return err
}
