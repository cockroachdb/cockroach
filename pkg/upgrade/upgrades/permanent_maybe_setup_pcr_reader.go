// Copyright 2024 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/replication"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func maybeSetupPCRStandbyReader(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if d.ReaderTenant == nil {
		return nil
	}
	id, ts, err := d.ReaderTenant.ReadFromTenantInfo(ctx)
	if err != nil {
		return err
	}
	if !id.IsSet() {
		return nil
	}

	log.Infof(ctx, "setting up read-only catalog as of %s reading from tenant %s", ts, id)
	if err := replication.SetupOrAdvanceStandbyReaderCatalog(ctx, id, ts, d.DB, d.Settings); err != nil {
		return err
	}
	return nil
}
