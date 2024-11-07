// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sctestdeps

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
		_ = c.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
			state.committed.UpsertNamespaceEntry(e, e.GetID(), e.GetMVCCTimestamp())
			return nil
		})
	})
}

// WithDescriptors sets the TestState descriptors to the provided value.
// This function also scrubs any volatile timestamps from the descriptor.
func WithDescriptors(c nstree.Catalog) Option {
	modifTime := hlc.Timestamp{WallTime: defaultOverriddenCreatedAt.UnixNano()}
	return optionFunc(func(state *TestState) {
		_ = c.ForEachDescriptor(func(desc catalog.Descriptor) error {
			mut := desc.NewBuilder().BuildCreatedMutable()
			switch m := mut.(type) {
			case *tabledesc.Mutable:
				for _, idx := range m.AllIndexes() {
					if !idx.CreatedAt().IsZero() {
						idx.IndexDesc().CreatedAtNanos = modifTime.WallTime
					}
				}
				m.CreateAsOfTime = modifTime
			}
			mut.ResetModificationTime()
			desc = mut.ImmutableCopy()
			state.committed.UpsertDescriptor(desc)
			return nil
		})
	})
}

// WithSystemDatabaseDescriptor adds the system database descriptor to the
// catalog.
//
// TODO(jeffswenson): delete this once `DROP DATABASE` works with a
// multi-region system database. (See PR #109844).
func WithSystemDatabaseDescriptor() Option {
	return optionFunc(func(state *TestState) {
		state.committed.UpsertDescriptor(systemschema.MakeSystemDatabaseDesc())
	})
}

// WithSessionData sets the TestState sessiondata to the provided value.
func WithSessionData(sessionData sessiondata.SessionData) Option {
	return optionFunc(func(state *TestState) {
		state.sessionData = sessionData
	})
}

// WithZoneConfigs sets the TestStates zone config map to the provided value.
func WithZoneConfigs(zoneConfigs map[catid.DescID]catalog.ZoneConfig) Option {
	return optionFunc(func(state *TestState) {
		for id, zc := range zoneConfigs {
			state.committed.UpsertZoneConfig(id, zc.ZoneConfigProto(), zc.GetRawBytesInStorage())
		}
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
func WithComments(comments map[catalogkeys.CommentKey]string) Option {
	return optionFunc(func(state *TestState) {
		for key, cmt := range comments {
			if err := state.committed.UpsertComment(key, cmt); err != nil {
				panic(err)
			}
		}
	})
}

// WithMerger injects a Merger to be provided by the TestState.
// The default merger logs the merge event into the test state.
func WithMerger(merger scexec.Merger) Option {
	return optionFunc(func(state *TestState) {
		state.merger = merger
	})
}

func WithIDGenerator(s serverutils.ApplicationLayerInterface) Option {
	return optionFunc(func(state *TestState) {
		state.evalCtx.DescIDGenerator = descidgen.NewGenerator(s.ClusterSettings(), s.Codec(), s.DB())
	})
}

func WithReferenceProviderFactory(f scbuild.ReferenceProviderFactory) Option {
	return optionFunc(func(state *TestState) {
		state.refProviderFactory = f
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

		semaCtx := tree.MakeSemaContext(state)
		semaCtx.SearchPath = &state.SessionData().SearchPath
		state.semaCtx = &semaCtx

		evalCtx := &eval.Context{
			SessionDataStack:   sessiondata.NewStack(state.SessionData()),
			Settings:           state.ClusterSettings(),
			ClientNoticeSender: state.ClientNoticeSender(),
		}
		state.evalCtx = evalCtx
	}),
}
