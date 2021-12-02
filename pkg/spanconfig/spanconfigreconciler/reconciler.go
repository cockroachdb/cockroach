// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigreconciler

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// Reconciler is a concrete implementation of the spanconfig.Reconciler
// interface.
type Reconciler struct {
	sqlWatcher    spanconfig.SQLWatcher
	sqlTranslator spanconfig.SQLTranslator
	kvAccessor    spanconfig.KVAccessor

	execCfg *sql.ExecutorConfig
	codec   keys.SQLCodec
	tenID   roachpb.TenantID
	knobs   *spanconfig.TestingKnobs
}

var _ spanconfig.Reconciler = &Reconciler{}

// New constructs a new Reconciler.
func New(
	sqlWatcher spanconfig.SQLWatcher,
	sqlTranslator spanconfig.SQLTranslator,
	kvAccessor spanconfig.KVAccessor,
	execCfg *sql.ExecutorConfig,
	codec keys.SQLCodec,
	tenID roachpb.TenantID,
	knobs *spanconfig.TestingKnobs,
) *Reconciler {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	return &Reconciler{
		sqlWatcher:    sqlWatcher,
		sqlTranslator: sqlTranslator,
		kvAccessor:    kvAccessor,

		execCfg: execCfg,
		codec:   codec,
		tenID:   tenID,
		knobs:   knobs,
	}
}

// Reconcile is part of the spanconfig.Reconciler interface; it's responsible
// for reconciling a tenant's zone configs with the cluster's span configs (KV
// construct). It does so incrementally and continuously, internally leveraging
// SQL{Watcher,Translator}, KVAccessor, and Store to make progress.
//
// Let's walk through what it does. At a high-level, we maintain an in-memory
// data structure that's up-to-date with the contents of the KV (at least the
// subset of spans we have access to, i.e. the keyspace carved out for our
// tenant ID). We watch for changes to SQL state (descriptors, zone configs),
// translate the SQL updates to the flattened span+config form, "diff" the
// updates against our data structure to see if there are any changes we need to
// inform KV of. If so, we do, and ensure that our data structure is kept
// up-to-date. We continue watching for future updates and repeat as necessary.
//
// There's only single instance of the Reconciler running for a given tenant at
// a given point it time (mutual exclusion/leasing is provided by the jobs
// subsystem). We needn't worry about contending writers, or the KV state being
// changed from underneath us. What we do have to worry about, however, is
// suspended tenants' not being reconciling while suspended. It's possible for
// a suspended tenant's SQL state to be GC-ed away at older MVCC timestamps;
// when watching for changes, we could fail to observe tables/indexes/partitions
// getting deleted. Left as is, this would result in us never issuing a
// corresponding deletion requests for the dropped span configs -- we'd be
// leaving orphaned span configs lying around (taking up storage space and
// creating pointless empty ranges). A "full reconciliation pass" is our attempt
// to find all these extraneous entries in KV and to delete them.
//
// We can use our span config data structure here too, one that's pre-populated
// with the contents of KV. We translate the entire SQL state into constituent
// spans and configs, diff against our data structure to generate KV updates
// that we then apply. We follow this with clearing out all these spans in our
// data structure, leaving behind all extraneous entries to be found in KV --
// entries we can then simply issue deletes for.
//
// TODO(irfansharif): The descriptions above presume holding the entire set of
// span configs in memory, but we could break away from that by adding
// pagination + retrieval limit to the GetSpanConfigEntriesFor API. We'd then
// paginate through chunks of the keyspace at a time, do a "full reconciliation
// pass" over just that chunk, and continue.
//
// TODO(irfansharif): We probably want some degree of batching when issuing RPCs
// to KV -- right now we sent forth the entire set of updates since our last
// checkpoint. For changes to, say, RANGE DEFAULT, the RPC request proto is
// proportional to the number of schema objects.
func (r *Reconciler) Reconcile(
	ctx context.Context, checkpoint hlc.Timestamp, callback func(checkpoint hlc.Timestamp) error,
) error {
	// TODO(irfansharif): Check system.{zones,descriptors} for last GC timestamp
	// and avoid the full reconciliation pass if the checkpointed provided is
	// visible to the rangefeed. Right now we're doing a full reconciliation
	// pass every time the reconciliation job kicks us off.
	_ = checkpoint

	var tenantSpan roachpb.Span
	if r.codec.ForSystemTenant() {
		// The system tenant governs all system keys (meta, liveness, timeseries
		// ranges, etc.) and system tenant tables.
		//
		// TODO(irfansharif): Should we include the scratch range here? Some
		// tests make use of it; we may want to declare configs over it and have
		// it considered all the same.
		tenantSpan = roachpb.Span{
			Key:    keys.EverythingSpan.Key,
			EndKey: keys.TableDataMax,
		}
	} else {
		// Secondary tenants govern everything prefixed by their tenant ID.
		tenPrefix := keys.MakeTenantPrefix(r.tenID)
		tenantSpan = roachpb.Span{
			Key:    tenPrefix,
			EndKey: tenPrefix.PrefixEnd(),
		}
	}

	var initialStore, incrementalStore *spanconfigstore.Store
	initialStore = spanconfigstore.New(roachpb.SpanConfig{})
	{
		// Fully populate the initialStore with KVAccessor contents.
		entries, err := r.kvAccessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{
			tenantSpan,
		})
		if err != nil {
			return err
		}

		for _, entry := range entries {
			initialStore.Apply(ctx, false /* dryrun */, spanconfig.Update{
				Span:   entry.Span,
				Config: entry.Config,
			})
		}
	}

	var fullTranslationTS hlc.Timestamp
	{ // Translate the entire SQL state and ensure KVAccessor reflects the latest.
		entries, timestamp, err := spanconfig.FullTranslate(ctx, r.sqlTranslator)
		if err != nil {
			return err
		}
		fullTranslationTS = timestamp

		updates := make([]spanconfig.Update, len(entries))
		for i, entry := range entries {
			updates[i] = spanconfig.Update{
				Span:   entry.Span,
				Config: entry.Config,
			}
		}

		toDelete, toUpsert := initialStore.Apply(ctx, false /* dryrun */, updates...)
		if len(toDelete) != 0 || len(toUpsert) != 0 {
			if err := r.kvAccessor.UpdateSpanConfigEntries(ctx, toDelete, toUpsert); err != nil {
				return err
			}
		}

		// Make a copy of the current view of the world (i.e. KVAccessor
		// contents) for incremental maintenance below.
		incrementalStore = initialStore.Copy(ctx)

		// Delete all updated spans in the initialStore to filter out extraneous
		// entries in the KVAccessor.
		for _, update := range updates {
			initialStore.Apply(ctx, false /* dryrun */, spanconfig.Update{
				Span: update.Span,
			})
		}

		// Delete the extraneous entries.
		var extraSpans []roachpb.Span
		if err := initialStore.ForEachOverlapping(ctx, keys.EverythingSpan,
			func(entry roachpb.SpanConfigEntry) error {
				extraSpans = append(extraSpans, entry.Span)
				incrementalStore.Apply(ctx, false /* dryrun */, spanconfig.Update{
					Span: entry.Span,
				})

				return nil
			},
		); err != nil {
			return err
		}
		if len(extraSpans) != 0 {
			if err := r.kvAccessor.UpdateSpanConfigEntries(ctx, extraSpans, nil); err != nil {
				return err
			}
		}
	}

	if err := callback(fullTranslationTS); err != nil {
		return err
	}

	// TODO(irfansharif): Hook into #73086, which can help bubble up retryable
	// errors from the watcher in the (very) unlikely case that it's unable to
	// generate incremental updates from the given timestamp (things could've
	// been GC-ed from underneath us). If such an error occurs, we'd want to
	// translate again an use an even higher timestamp to watch for incremental
	// updates.

	// Watch for incremental updates, applying KV as things change.
	return r.sqlWatcher.WatchForSQLUpdates(ctx, fullTranslationTS,
		func(ctx context.Context, updates []spanconfig.DescriptorUpdate, checkpoint hlc.Timestamp) error {
			if len(updates) == 0 {
				return callback(checkpoint) // nothing to do; propagate the checkpoint
			}

			var allIDs, missingIDs descpb.IDs
			if err := sql.DescsTxn(ctx, r.execCfg,
				func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
					for _, update := range updates {
						allIDs = append(allIDs, update.ID)

						if update.DescriptorType == catalog.Table {
							desc, err := descsCol.GetImmutableDescriptorByID(ctx, txn, update.ID, tree.CommonLookupFlags{
								Required:       true, // we want to find out error out for missing descriptors
								IncludeDropped: true,
								IncludeOffline: true,
								AvoidLeased:    true, // we want a consistent read
							})

							considerAsMissing := false
							if errors.Is(err, catalog.ErrDescriptorNotFound) {
								considerAsMissing = true
							} else if err != nil {
								return err
							} else if r.knobs.ExcludeDroppedDescriptorsFromLookup && desc.Dropped() {
								considerAsMissing = true
							}

							if considerAsMissing {
								missingIDs = append(missingIDs, update.ID) // accumulate the set of missing table IDs
							}
						}
					}

					return nil
				},
			); err != nil {
				return err
			}

			entries, _, err := r.sqlTranslator.Translate(ctx, allIDs)
			if err != nil {
				return err
			}

			spanConfigUpdates := make([]spanconfig.Update, 0, len(missingIDs)+len(entries))
			for _, missingID := range missingIDs {
				// For the missing table IDs, add corresponding spans to the
				// delete list.
				tableSpan := roachpb.Span{
					Key:    r.codec.TablePrefix(uint32(missingID)),
					EndKey: r.codec.TablePrefix(uint32(missingID)).PrefixEnd(),
				}
				spanConfigUpdates = append(spanConfigUpdates, spanconfig.Update{
					Span: tableSpan,
				})
			}
			for _, entry := range entries {
				// Accumulate the span config updates for SQL state that has
				// changed.
				spanConfigUpdates = append(spanConfigUpdates, spanconfig.Update{
					Span:   entry.Span,
					Config: entry.Config,
				})
			}
			toDelete, toUpsert := incrementalStore.Apply(ctx, false /* dryrun */, spanConfigUpdates...)
			if len(toDelete) == 0 && len(toUpsert) == 0 {
				return callback(checkpoint) // noop; nothing to do
			}

			if err := r.kvAccessor.UpdateSpanConfigEntries(ctx, toDelete, toUpsert); err != nil {
				return err
			}

			return callback(checkpoint)
		},
	)
}
