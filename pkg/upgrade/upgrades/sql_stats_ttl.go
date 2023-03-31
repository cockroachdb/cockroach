// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

func sqlStatsTTLChange(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.SystemDeps,
) error {
	tables := []string{
		"system.statement_statistics",
		"system.transaction_statistics",
		"system.statement_activity",
		"system.transaction_activity",
	}

	// These migrations are skipped for backup tests because these tables are not part
	// of the backup.
	shouldConfigureTTL := true
	if knobs := d.SQLStatsKnobs; knobs != nil {
		shouldConfigureTTL = !knobs.SkipZoneConfigBootstrap
	}

	if shouldConfigureTTL {
		for _, table := range tables {
			if _, err := d.DB.Executor().ExecEx(
				ctx,
				"set-SQLStatsTables-TTL",
				nil,
				sessiondata.InternalExecutorOverride{User: username.RootUserName()},
				fmt.Sprintf("ALTER TABLE %s CONFIGURE ZONE USING gc.ttlseconds = $1", table),
				3600, /* one hour */
			); err != nil {
				return err
			}
		}
	}
	return nil
}
