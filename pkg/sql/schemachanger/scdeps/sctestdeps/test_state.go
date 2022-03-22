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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/redact"
)

// TestState is a backing struct used to implement all schema changer
// dependencies, like scbuild.Dependencies or scexec.Dependencies, for the
// purpose of facilitating end-to-end testing of the declarative schema changer.
type TestState struct {
	catalog, synthetic  nstree.MutableCatalog
	currentDatabase     string
	phase               scop.Phase
	sessionData         sessiondata.SessionData
	statements          []string
	testingKnobs        *scrun.TestingKnobs
	jobs                []jobs.Record
	jobCounter          int
	txnCounter          int
	sideEffectLogBuffer strings.Builder

	// The below portions fo the Dependencies are stored as interfaces because
	// we permit users of this package to override the default implementations.
	// This approach allows the TestState object to be flexibly used in various
	// different testing contexts, providing a sane default implementation of
	// dependencies with optional overrides.
	backfiller        scexec.Backfiller
	indexSpanSplitter scexec.IndexSpanSplitter
	backfillTracker   scexec.BackfillTracker

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
	defer s.synthetic.Clear()
	defer s.LogSideEffectf("commit transaction #%d", s.txnCounter)
	s.LogSideEffectf("begin transaction #%d", s.txnCounter)
	fn(s)
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

// FeatureChecker implements scbuild.Dependencies
func (s *TestState) FeatureChecker() scbuild.FeatureChecker {
	return s
}
