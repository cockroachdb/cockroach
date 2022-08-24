// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sctestdeps

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/descmetadata"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Option configures the TestState.
type Option interface {
	apply(*TestState)
}

type optionFunc func(*TestState)

func (o optionFunc) apply(state *TestState) { o(state) }

var _ Option = (optionFunc)(nil)

// WithNamespace sets the TestState namespace to the provided value.
func WithNamespace(c nstree.Catalog) Option {
	return optionFunc(func(state *TestState) {
		_ = c.ForEachNamespaceEntry(func(e catalog.NameEntry) error {
			state.committed.UpsertNamespaceEntry(e, e.GetID())
			state.uncommitted.UpsertNamespaceEntry(e, e.GetID())
			return nil
		})
	})
}

// WithDescriptors sets the TestState descriptors to the provided value.
// This function also scrubs any volatile timestamps from the descriptor.
func WithDescriptors(c nstree.Catalog) Option {
	modifTime := hlc.Timestamp{WallTime: defaultOverriddenCreatedAt.UnixNano()}
	return optionFunc(func(state *TestState) {
		_ = c.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
			pb := desc.DescriptorProto()
			if table, isTable := desc.(catalog.TableDescriptor); isTable {
				mut := table.NewBuilder().BuildExistingMutable().(*tabledesc.Mutable)
				for _, idx := range mut.AllIndexes() {
					if !idx.CreatedAt().IsZero() {
						idx.IndexDesc().CreatedAtNanos = defaultOverriddenCreatedAt.UnixNano()
					}
				}
				mut.ModificationTime = modifTime
				pb = mut.DescriptorProto()
			}
			b := descbuilder.NewBuilderWithMVCCTimestamp(pb, modifTime)
			state.committed.UpsertDescriptorEntry(b.BuildImmutable())
			state.uncommitted.UpsertDescriptorEntry(b.BuildExistingMutable())
			return nil
		})
	})
}

// WithSessionData sets the TestState sessiondata to the provided value.
func WithSessionData(sessionData sessiondata.SessionData) Option {
	return optionFunc(func(state *TestState) {
		state.sessionData = sessionData
	})
}

// WithZoneConfigs sets the TestStates zone config map to the provided value.
func WithZoneConfigs(zoneConfigs map[catid.DescID]*zonepb.ZoneConfig) Option {
	return optionFunc(func(state *TestState) {
		state.zoneConfigs = zoneConfigs
	})
}

// WithTestingKnobs sets the TestState testing knobs to the provided value.
func WithTestingKnobs(testingKnobs *scexec.TestingKnobs) Option {
	return optionFunc(func(state *TestState) {
		state.testingKnobs = testingKnobs
	})
}

// WithStatements sets the TestState statement to the provided value.
func WithStatements(statements ...string) Option {
	return optionFunc(func(state *TestState) {
		state.statements = statements
	})
}

// WithCurrentDatabase sets the TestState current database to the provided value.
func WithCurrentDatabase(db string) Option {
	return optionFunc(func(state *TestState) {
		state.currentDatabase = db
	})
}

// WithBackfillerTracker injects a BackfillerTracker to be provided by the
// TestState. If this option is not provided, the default tracker will
// resolve any descriptor referenced and return an empty backfill progress.
// All writes in the default tracker are ignored.
func WithBackfillerTracker(backfillTracker scexec.BackfillerTracker) Option {
	return optionFunc(func(state *TestState) {
		state.backfillTracker = backfillTracker
	})
}

// WithBackfiller injects a Backfiller to be provided by the TestState.
// The default backfiller logs the backfill event into the test state.
func WithBackfiller(backfiller scexec.Backfiller) Option {
	return optionFunc(func(state *TestState) {
		state.backfiller = backfiller
	})
}

// WithComments injects sets comment cache of TestState to the provided value.
func WithComments(comments map[descmetadata.CommentKey]string) Option {
	return optionFunc(func(state *TestState) {
		state.comments = comments
	})
}

// WithMerger injects a Merger to be provided by the TestState.
// The default merger logs the merge event into the test state.
func WithMerger(merger scexec.Merger) Option {
	return optionFunc(func(state *TestState) {
		state.merger = merger
	})
}

var (
	// defaultOverriddenCreatedAt is used to populate the CreatedAt timestamp for
	// all descriptors injected into the catalog. We inject this to make the
	// tests deterministic.
	defaultOverriddenCreatedAt = time.Date(2022, time.January, 1, 0, 0, 0, 0, time.UTC)

	// defaultCreatedAt is used to populated the CreatedAt timestamp for all newly
	// created indexes.
	defaultCreatedAt = defaultOverriddenCreatedAt.Add(time.Hour)
)

var defaultOptions = []Option{
	optionFunc(func(state *TestState) {
		state.backfillTracker = &testBackfillerTracker{deps: state}
		state.backfiller = &testBackfiller{s: state}
		state.merger = &testBackfiller{s: state}
		state.indexSpanSplitter = &indexSpanSplitter{}
		state.approximateTimestamp = defaultCreatedAt
		state.zoneConfigs = make(map[catid.DescID]*zonepb.ZoneConfig)
	}),
}
