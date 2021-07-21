// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Accessor mediates access to the cluster's span configs applicable for a given
// tenant.
//
// TODO(zcfgs-pod): Should the manager be the only non-test type implementing
// this interface, for clarity?
type Accessor interface {
	// GetSpanConfigEntriesFor retrieves the span configurations over the
	// requested span.
	GetSpanConfigEntriesFor(ctx context.Context, span roachpb.Span) ([]roachpb.SpanConfigEntry, error)

	// UpdateSpanConfigEntries updates the span configurations over the given
	// spans.
	UpdateSpanConfigEntries(ctx context.Context, update []roachpb.SpanConfigEntry, delete []roachpb.Span) error
}

// ReconciliationDependencies captures what's needed by the span config
// reconciliation process. The reconciliation process reconciles a tenant's span
// configs with the cluster's.
type ReconciliationDependencies interface {
	// Accessor mediates access to the subset of the cluster's span configs
	// applicable to a given tenant.
	Accessor

	// TODO(zcfgs-pod): We'll also needs access to a tenant's system.descriptor,
	// or ideally a Watcher emitting relevant Updates from a tenant's
	// system.{descriptor,zones}. That suggests we'll want two Watcher
	// implementations:
	// - Something per-store, watching over system.span_configurations.
	// - Something watching over a tenant's system.{descriptor,zones}, emitting
	//   updates for the reconciliation process.
}

// Watcher emits observed updates to span configs.
type Watcher interface {
	Watch(ctx context.Context, stopper *stop.Stopper) (<-chan Update, error)
}

// Update captures what span has seen a config change. It's the unit of what a
// Watcher emits.
type Update struct {
	// Entry captures the keyspan and the corresponding config that has been
	// updated. If deleted is true, the config over that span has been deleted
	// (those keys no longer exist). If false, the embedded config is what the
	// keyspan was updated to.
	Entry Entry

	// Deleted is true if the span config entry has been deleted.
	Deleted bool
}

// Config is an interface to be shared by {Zone,Span} configs.
type Config interface {
	Equal(o interface{}) bool

	// Config is a marker method.
	Config()
	GenerateSpanConfig() (roachpb.SpanConfig, error)
}

// Reconciler is responsible for reconciling zone configs to span configs.
type Reconciler interface {
	// GenerateSpanConfigurations generates span configurations for the given
	// set of IDs. Right now it expects the given IDs to be table IDs.
	// TODO(arul): Account for non-table IDs as well.
	GenerateSpanConfigurations(ctx context.Context, txn *kv.Txn, ids descpb.IDs) ([]Entry, error)

	// FullReconcile generates a span configuration for all tables.
	FullReconcile(ctx context.Context) ([]Entry, error)
}
