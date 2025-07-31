// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package validate

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const invalidSchemaChangerStatePrefix = "invalid schema changer state"

func validateSchemaChangerState(d catalog.Descriptor, vea catalog.ValidationErrorAccumulator) {
	scs := d.GetDeclarativeSchemaChangerState()
	if scs == nil {
		return
	}
	report := func(err error) {
		vea.Report(errors.WrapWithDepth(
			1, err, invalidSchemaChangerStatePrefix,
		))
	}

	// TODO(ajwerner): Add job validation to ensure that the job exists and is
	// in a non-terminal state. This would probably not be part of ValidateSelf.
	if scs.JobID == 0 {
		report(errors.New("empty job ID"))
	}

	// Validate that the targets correspond to this descriptor.
	for i, t := range scs.Targets {
		if got := screl.GetDescID(t.Element()); got != d.GetID() {
			report(errors.Errorf("target %d corresponds to descriptor %d != %d",
				i, got, d.GetID()))
		}
		switch t.TargetStatus {
		case scpb.Status_PUBLIC, scpb.Status_ABSENT, scpb.Status_TRANSIENT_ABSENT:
			// These are valid target status values.
		default:
			report(errors.Errorf("target %d is targeting an invalid status %s",
				i, t.TargetStatus))
		}
	}

	// Validate that the various parallel fields are sound.
	{
		var haveProblem bool
		if nt, ntr := len(scs.Targets), len(scs.TargetRanks); nt != ntr {
			haveProblem = true
			report(errors.Errorf("number mismatch between Targets and TargetRanks: %d != %d",
				nt, ntr))
		}
		if nt, ns := len(scs.Targets), len(scs.CurrentStatuses); nt != ns {
			haveProblem = true
			report(errors.Errorf("number mismatch between Targets and CurentStatuses: %d != %d",
				nt, ns))
		}
		// If there's a mismatch, the validation below will not be sane.
		if haveProblem {
			return
		}
	}

	// Validate that the target ranks are unique.
	ranksToTarget := map[uint32]*scpb.Target{}
	{
		var duplicates intsets.Fast
		for i, r := range scs.TargetRanks {
			if _, exists := ranksToTarget[r]; exists {
				duplicates.Add(int(r))
			} else {
				ranksToTarget[r] = &scs.Targets[i]
			}
		}
		if duplicates.Len() > 0 {
			report(errors.Errorf("TargetRanks contains duplicate entries %v",
				redact.SafeString(fmt.Sprint(duplicates.Ordered()))))
		}
	}

	// Validate that the statements are sorted.
	if !slices.IsSortedFunc(scs.RelevantStatements, func(a, b scpb.DescriptorState_Statement) int {
		return cmp.Compare(a.StatementRank, b.StatementRank)
	}) {
		report(errors.New("RelevantStatements are not sorted"))
	}

	// Validate that the statements refer exclusively to targets in this
	// descriptor.
	statementsExpected := map[uint32]*intsets.Fast{}
	for i := range scs.Targets {
		t := &scs.Targets[i]
		exp, ok := statementsExpected[t.Metadata.StatementID]
		if !ok {
			exp = &intsets.Fast{}
			statementsExpected[t.Metadata.StatementID] = exp
		}
		exp.Add(int(scs.TargetRanks[i]))
	}
	var statementRanks intsets.Fast
	for _, s := range scs.RelevantStatements {
		statementRanks.Add(int(s.StatementRank))
		if _, ok := statementsExpected[s.StatementRank]; !ok {
			report(errors.Errorf("unexpected statement %d (%s)",
				s.StatementRank, s.Statement.RedactedStatement))
		}
	}

	// Validate that there are no duplicate statements.
	if statementRanks.Len() != len(scs.RelevantStatements) {
		report(errors.Errorf("duplicates exist in RelevantStatements"))
	}

	// Validate that all targets have a corresponding statement.
	{
		var expected intsets.Fast
		stmts := statementRanks.Copy()
		for rank := range statementsExpected {
			expected.Add(int(rank))
		}
		stmts.ForEach(func(i int) { expected.Remove(i) })

		expected.ForEach(func(stmtRank int) {
			expectedTargetRanks := statementsExpected[uint32(stmtRank)]
			var ranks, elementStrs []string
			expectedTargetRanks.ForEach(func(targetRank int) {
				ranks = append(ranks, fmt.Sprint(targetRank))
				elementStrs = append(elementStrs,
					screl.ElementString(ranksToTarget[uint32(targetRank)].Element()))
			})
			report(errors.Errorf("missing statement for targets (%s) / (%s)",
				redact.SafeString(strings.Join(ranks, ", ")),
				strings.Join(elementStrs, ", "),
			))
		})
	}

	// TODO(ajwerner): Consider asserting that the current statuses make sense
	// for the target elements.

}
