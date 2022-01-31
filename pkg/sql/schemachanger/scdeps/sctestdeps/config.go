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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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
			state.catalog.UpsertNamespaceEntry(e, e.GetID())
			return nil
		})
	})
}

// WithDescriptors sets the TestState descriptors to the provided value.
// This function also scrubs any volatile timestamps from the descriptor.
func WithDescriptors(c nstree.Catalog) Option {
	return optionFunc(func(state *TestState) {
		_ = c.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
			if table, isTable := desc.(catalog.TableDescriptor); isTable {
				mut := table.NewBuilder().BuildExistingMutable().(*tabledesc.Mutable)
				for _, idx := range mut.AllIndexes() {
					if !idx.CreatedAt().IsZero() {
						idx.IndexDesc().CreatedAtNanos = defaultOverriddenCreatedAt.UnixNano()
					}
				}
				desc = mut.ImmutableCopy()
			}
			state.catalog.UpsertDescriptorEntry(desc)
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

// WithTestingKnobs sets the TestState testing knobs to the provided value.
func WithTestingKnobs(testingKnobs *scrun.TestingKnobs) Option {
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

// WithBackfillTracker injects a BackfillTracker to be provided by the
// TestState. If this option is not provided, the default tracker will
// resolve any descriptor referenced and return an empty backfill progress.
// All writes in the default tracker are ignored.
func WithBackfillTracker(backfillTracker scexec.BackfillTracker) Option {
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
		state.backfillTracker = &testBackfillTracker{deps: state}
		state.backfiller = &testBackfiller{s: state}
		state.indexSpanSplitter = &indexSpanSplitter{}
		state.approximateTimestamp = defaultCreatedAt
	}),
}
