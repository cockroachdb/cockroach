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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// TestState is a backing struct used to implement all schema changer
// dependencies, like scbuild.Dependencies or scexec.Dependencies, for the
// purpose of facilitating end-to-end testing of the declarative schema changer.
type TestState struct {
	descriptors, syntheticDescriptors nstree.Map
	namespace                         map[descpb.NameInfo]descpb.ID
	currentDatabase                   string
	phase                             scop.Phase
	sessionData                       sessiondata.SessionData
	statements                        []string
	testingKnobs                      *scrun.TestingKnobs
	jobs                              []jobs.Record
	jobCounter                        int
	txnCounter                        int
	sideEffectLogBuffer               strings.Builder
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
	defer s.syntheticDescriptors.Clear()
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
