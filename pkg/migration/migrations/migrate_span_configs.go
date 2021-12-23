// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

func ensureSpanConfigReconciliation(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	if !d.Codec.ForSystemTenant() {
		return nil
	}

	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		if !d.SpanConfig.Reconciler.Checkpoint().IsEmpty() {
			return nil
		}

		log.Warningf(ctx, "waiting for span config reconciliation...")
		continue
	}

	return errors.Newf("unable to reconcile span configs")
}

func ensureSpanConfigSubscription(
	ctx context.Context, _ clusterversion.ClusterVersion, deps migration.SystemDeps, _ *jobs.Job,
) error {
	return deps.Cluster.UntilClusterStable(ctx, func() error {
		return deps.Cluster.ForEveryNode(ctx, "ensure-span-config-subscription",
			func(ctx context.Context, client serverpb.MigrationClient) error {
				req := &serverpb.WaitForSpanConfigSubscriptionRequest{}
				_, err := client.WaitForSpanConfigSubscription(ctx, req)
				return err
			})
	})
}
