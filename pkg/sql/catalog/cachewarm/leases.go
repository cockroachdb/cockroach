// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cachewarm

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Leases will eagerly acquire leases on entries it finds in the namespace
// table.
//
// TODO(ajwerner): Make this pre-warming bounded in terms of total number
// of descriptors and maximum concurrency. Ideally, we want everything
// that's in the system database first, then we want the databases, then we
// want everything else. That will allow users to connect to databases and
// find what they're looking for rapidly. For now, this is something of a
// hack where we just run with very high parallelism to fetch everything in
// the common case. As we make leases lower-latency to fetch, we can pull
// the concurrency down to something more reasonable like 16 or 32.
func Leases(
	ctx context.Context,
	stopper *stop.Stopper,
	settings *cluster.Settings,
	db *kv.DB,
	lm *lease.Manager,
	cf descs.TxnManager,
) {
	if !PreFetchLeasesEnabled.Get(&settings.SV) {
		return
	}
	_ = stopper.RunAsyncTask(ctx, "prewarm-leases", func(ctx context.Context) {
		warmLeases(ctx, stopper, db, lm, cf)
	})
}

var PreFetchLeasesEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.catalog.prefetch.leases.enabled",
	"if enabled, will lead to leases being prefetched on server startup",
	false,
)

func warmLeases(
	ctx context.Context, stopper *stop.Stopper, db *kv.DB, lm *lease.Manager, cf descs.TxnManager,
) {
	var entries nstree.Catalog
	if err := cf.DescsTxn(ctx, db, func(
		ctx context.Context, txn *kv.Txn, collection *descs.Collection,
	) (err error) {
		entries, err = collection.Direct().ScanAllNamespaceEntries(ctx, txn)
		return err
	}); err != nil {
		if ctx.Err() == nil {
			log.Warningf(ctx, "failed to read catalog")
		}
		return
	}

	log.Infof(ctx, "pre-fetching descriptor leases")
	_ = entries.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
		return stopper.RunAsyncTask(ctx, "pre-fetch descriptor", func(ctx context.Context) {
			desc, _ := lm.Acquire(ctx, db.Clock().Now(), e.GetID())
			if desc != nil {
				desc.Release(ctx)
			}
		})
	})
}
