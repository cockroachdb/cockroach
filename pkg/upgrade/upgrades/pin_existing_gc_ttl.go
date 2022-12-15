// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// pinExistingGCTTL explicitly sets the default GC TTL to 25h if it
// wasn't already by the user. We're doing this as part of lowering the
// default from 25h to 4h, without having that change implicitly for
// existing clusters (newly instantiated clusters will start with the 4h
// default).
func pinExistingGCTTL(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return d.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		row, err := d.InternalExecutor.QueryRowEx(
			ctx, "get-default-zone-config", txn,
			sessiondata.NodeUserSessionDataOverride,
			`
	SELECT
		(crdb_internal.pb_to_json('cockroach.config.zonepb.ZoneConfig', raw_config_protobuf)->'gc'->'ttlSeconds')::INT
	FROM crdb_internal.zones
	WHERE target = 'RANGE default'
	LIMIT 1
`)
		if err != nil {
			return err
		}

		defaultGCTTLSeconds := tree.MustBeDInt(row[0])
		if defaultGCTTLSeconds != 25*60*60 {
			return nil // nothing to do
		}

		if _, err := d.InternalExecutor.ExecEx(
			ctx, "set-gc-ttl-range-default", txn,
			sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf("ALTER RANGE DEFAULT CONFIGURE ZONE USING gc.ttlseconds = %d", 25*60*60),
		); err != nil {
			return err
		}
		return nil
	})
}
