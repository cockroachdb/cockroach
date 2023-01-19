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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/scviz"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/redact"
)

// TestState is a backing struct used to implement all schema changer
// dependencies, like scbuild.Dependencies or scexec.Dependencies, for the
// purpose of facilitating end-to-end testing of the declarative schema changer.
type TestState struct {

	// committed and uncommitted mock the catalog as it is persisted in the KV
	// layer:
	// - committed represents the catalog as it is visible outside the schema
	//   change transactions;
	// - uncommitted is the catalog such as it is in the schema change statement
	//   transaction before it commits.
	// If we're in a transaction (via WithTxn) and no schema changes have taken
	// place yet, uncommitted is the same as committed, however executing a schema
	// change statement will probably alter the contents of uncommitted and these
	// will not be reflected in committed until the transaction commits, i.e. the
	// WithTxn method returns.
	committed, uncommitted nstree.MutableCatalog

	comments                map[catalogkeys.CommentKey]string
	zoneConfigs             map[catid.DescID]catalog.ZoneConfig
	currentDatabase         string
	phase                   scop.Phase
	sessionData             sessiondata.SessionData
	statements              []string
	testingKnobs            *scexec.TestingKnobs
	jobs                    []jobs.Record
	createdJobsInCurrentTxn []jobspb.JobID
	jobCounter              int
	txnCounter              int
	sideEffectLogBuffer     strings.Builder

	// The below portions fo the Dependencies are stored as interfaces because
	// we permit users of this package to override the default implementations.
	// This approach allows the TestState object to be flexibly used in various
	// different testing contexts, providing a sane default implementation of
	// dependencies with optional overrides.
	backfiller        scexec.Backfiller
	merger            scexec.Merger
	indexSpanSplitter scexec.IndexSpanSplitter
	backfillTracker   scexec.BackfillerTracker

	// approximateTimestamp is used to populate approximate timestamps in
	// descriptors.
	approximateTimestamp time.Time
}

// NewTestDependencies returns a TestState populated with the provided options.
func NewTestDependencies(options ...Option) *TestState {
	var s TestState
	for _, o := range defaultOptions {
		o.apply(&s)
	}
	for _, o := range options {
		o.apply(&s)
	}
	return &s
}

// LogSideEffectf writes an entry to the side effect log, to keep track of any
// state changes which would have occurred in real dependencies.
func (s *TestState) LogSideEffectf(fmtstr string, args ...interface{}) {
	s.sideEffectLogBuffer.WriteString(fmt.Sprintf(fmtstr, args...))
	s.sideEffectLogBuffer.WriteRune('\n')
}

// SideEffectLog returns the contents of the side effect log.
// See LogSideEffectf for details.
func (s *TestState) SideEffectLog() string {
	return s.sideEffectLogBuffer.String()
}

// WithTxn simulates the execution of a transaction.
func (s *TestState) WithTxn(fn func(s *TestState)) {
	s.txnCounter++
	defer func() {
		u := s.uncommitted
		s.committed, s.uncommitted = nstree.MutableCatalog{}, nstree.MutableCatalog{}
		_ = u.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
			s.committed.UpsertNamespaceEntry(e, e.GetID(), e.GetMVCCTimestamp())
			s.uncommitted.UpsertNamespaceEntry(e, e.GetID(), e.GetMVCCTimestamp())
			return nil
		})
		_ = u.ForEachDescriptor(func(d catalog.Descriptor) error {
			mut := d.NewBuilder().BuildCreatedMutable()
			mut.ResetModificationTime()
			d = mut.ImmutableCopy()
			s.committed.UpsertDescriptor(d)
			s.uncommitted.UpsertDescriptor(d)
			return nil
		})
		_ = u.ForEachComment(func(key catalogkeys.CommentKey, cmt string) error {
			s.committed.UpsertComment(key, cmt)
			s.uncommitted.UpsertComment(key, cmt)
			return nil
		})
		_ = u.ForEachZoneConfig(func(id catid.DescID, zc catalog.ZoneConfig) error {
			zc = zc.Clone()
			s.committed.UpsertZoneConfig(id, zc.ZoneConfigProto(), zc.GetRawBytesInStorage())
			zc = zc.Clone()
			s.uncommitted.UpsertZoneConfig(id, zc.ZoneConfigProto(), zc.GetRawBytesInStorage())
			return nil
		})
		s.LogSideEffectf("commit transaction #%d", s.txnCounter)
		if len(s.createdJobsInCurrentTxn) > 0 {
			s.LogSideEffectf("notified job registry to adopt jobs: %v", s.createdJobsInCurrentTxn)
		}
		s.createdJobsInCurrentTxn = nil
	}()
	s.LogSideEffectf("begin transaction #%d", s.txnCounter)
	fn(s)
}

func (s *TestState) mvccTimestamp() hlc.Timestamp {
	return hlc.Timestamp{WallTime: defaultOverriddenCreatedAt.UnixNano() + int64(s.txnCounter)}
}

// IncrementPhase sets the state to the next phase.
func (s *TestState) IncrementPhase() {
	s.phase++
}

// JobRecord returns the job record in the fake job registry for the given job
// ID, if it exists, nil otherwise.
func (s *TestState) JobRecord(jobID jobspb.JobID) *jobs.Record {
	idx := int(jobID) - 1
	if idx < 0 || idx >= len(s.jobs) {
		return nil
	}
	return &s.jobs[idx]
}

// FormatAstAsRedactableString implements scbuild.AstFormatter
func (s *TestState) FormatAstAsRedactableString(
	statement tree.Statement, ann *tree.Annotations,
) redact.RedactableString {
	// Return the SQL back non-redacted and not fully resolved for the purposes
	// of testing.
	f := tree.NewFmtCtx(
		tree.FmtAlwaysQualifyTableNames|tree.FmtMarkRedactionNode,
		tree.FmtAnnotations(ann))
	f.FormatNode(statement)
	formattedRedactableStatementString := f.CloseAndGetString()
	return redact.RedactableString(formattedRedactableStatementString)
}

// AstFormatter dummy formatter for AST nodes.
func (s *TestState) AstFormatter() scbuild.AstFormatter {
	return s
}

// CheckFeature implements scbuild.SchemaFeatureCheck
func (s *TestState) CheckFeature(ctx context.Context, featureName tree.SchemaFeatureName) error {
	s.LogSideEffectf("checking for feature: %s", featureName)
	return nil
}

// CanPerformDropOwnedBy implements scbuild.SchemaFeatureCheck.
func (s *TestState) CanPerformDropOwnedBy(
	ctx context.Context, role username.SQLUsername,
) (bool, error) {
	return true, nil
}

// FeatureChecker implements scbuild.Dependencies
func (s *TestState) FeatureChecker() scbuild.FeatureChecker {
	return s
}

// Get implements DescriptorCommentCache interface.
func (s *TestState) get(
	objID catid.DescID, subID uint32, commentType catalogkeys.CommentType,
) (comment string, ok bool) {
	commentKey := catalogkeys.MakeCommentKey(uint32(objID), subID, commentType)
	comment, ok = s.comments[commentKey]
	return comment, ok
}

// GetDatabaseComment implements the scdecomp.CommentGetter interface.
func (s *TestState) GetDatabaseComment(dbID catid.DescID) (comment string, ok bool) {
	return s.get(dbID, 0, catalogkeys.DatabaseCommentType)
}

// GetSchemaComment implements the scdecomp.CommentGetter interface.
func (s *TestState) GetSchemaComment(schemaID catid.DescID) (comment string, ok bool) {
	return s.get(schemaID, 0, catalogkeys.SchemaCommentType)
}

// GetTableComment implements the scdecomp.CommentGetter interface.
func (s *TestState) GetTableComment(tableID catid.DescID) (comment string, ok bool) {
	return s.get(tableID, 0, catalogkeys.TableCommentType)
}

// GetColumnComment implements the scdecomp.CommentGetter interface.
func (s *TestState) GetColumnComment(
	tableID catid.DescID, pgAttrNum catid.PGAttributeNum,
) (comment string, ok bool) {
	return s.get(tableID, uint32(pgAttrNum), catalogkeys.ColumnCommentType)
}

// GetIndexComment implements the scdecomp.CommentGetter interface.
func (s *TestState) GetIndexComment(
	tableID catid.DescID, indexID catid.IndexID,
) (comment string, ok bool) {
	return s.get(tableID, uint32(indexID), catalogkeys.IndexCommentType)
}

// GetConstraintComment implements the scdecomp.CommentGetter interface.
func (s *TestState) GetConstraintComment(
	tableID catid.DescID, constraintID catid.ConstraintID,
) (comment string, ok bool) {
	return s.get(tableID, uint32(constraintID), catalogkeys.ConstraintCommentType)
}

// DescriptorCommentGetter implements scbuild.Dependencies interface.
func (s *TestState) DescriptorCommentGetter() scbuild.CommentGetter {
	return s
}

// ClientNoticeSender implements scbuild.Dependencies.
func (s *TestState) ClientNoticeSender() eval.ClientNoticeSender {
	return &faketreeeval.DummyClientNoticeSender{}
}

func (s *TestState) descriptorDiff(desc catalog.Descriptor) string {
	var old protoutil.Message
	if d, _ := s.mustReadImmutableDescriptor(desc.GetID()); d != nil {
		mut := d.NewBuilder().BuildCreatedMutable()
		mut.ResetModificationTime()
		old = mut.DescriptorProto()
	}
	return sctestutils.ProtoDiff(old, desc.DescriptorProto(), sctestutils.DiffArgs{
		Indent:       "  ",
		CompactLevel: 3,
	}, func(i interface{}) {
		scviz.RewriteEmbeddedIntoParent(i)
		if m, ok := i.(map[string]interface{}); ok {
			ds, exists := m["declarativeSchemaChangerState"].(map[string]interface{})
			if !exists {
				return
			}
			for _, k := range []string{
				"currentStatuses", "targetRanks", "targets",
			} {
				if _, kExists := ds[k]; kExists {
					ds[k] = "<redacted>"
				}
			}
		}
	})
}
