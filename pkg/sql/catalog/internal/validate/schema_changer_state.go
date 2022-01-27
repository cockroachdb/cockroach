package validate

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const invalidSchemaChangerStatePrefix = "invalid schema changer state"

func validateSchemaChangerState(d catalog.Descriptor, vea catalog.ValidationErrorAccumulator) {
	scs := d.GetDeclarativeSchemaChangerState()
	if scs == nil {
		return
	}
	reportf := func(format string, args ...interface{}) {
		vea.Report(errors.WrapWithDepth(1, errors.NewWithDepthf(1,
			format, args...,
		), invalidSchemaChangerStatePrefix))
	}

	// TODO(ajwerner): Add job validation to ensure that the job exists and is
	// in a non-terminal state. This would probably not be part of ValidateSelf.
	if scs.JobID == 0 {
		reportf("empty job ID")
	}

	if len(scs.Targets) == 0 {
		reportf("no targets")
	}

	// Validate that the targets correspond to this descriptor.
	for i, t := range scs.Targets {
		if got := screl.GetDescID(t.Element()); got != d.GetID() {
			reportf("target %d corresponds to descriptor %d != %d",
				i, got, d.GetID())
		}
	}

	// Validate that the various parallel fields are sound.
	{
		var haveProblem bool
		if nt, ntr := len(scs.Targets), len(scs.TargetRanks); nt != ntr {
			haveProblem = true
			reportf("number mismatch between Targets and TargetRanks: %d != %d",
				nt, ntr)
		}
		if nt, ns := len(scs.Targets), len(scs.CurrentStatuses); nt != ns {
			haveProblem = true
			reportf("number mismatch between Targets and CurentStatuses: %d != %d",
				nt, ns)
		}
		// If there's a mismatch, the validation below will not be sane.
		if haveProblem {
			return
		}
	}

	// Validate that the target ranks are unique.
	ranksToTarget := map[uint32]*scpb.Target{}
	{
		var duplicates util.FastIntSet
		for i, r := range scs.TargetRanks {
			if _, exists := ranksToTarget[r]; exists {
				duplicates.Add(int(r))
			} else {
				ranksToTarget[r] = &scs.Targets[i]
			}
		}
		if duplicates.Len() > 0 {
			reportf("TargetRanks contains duplicate entries %v",
				redact.SafeString(fmt.Sprint(duplicates.Ordered())))
		}
	}

	// Validate that the statements are sorted.
	if !sort.SliceIsSorted(scs.RelevantStatements, func(i, j int) bool {
		return scs.RelevantStatements[i].StatementRank < scs.RelevantStatements[j].StatementRank
	}) {
		reportf("RelevantStatements are not sorted")
	}

	// Validate that the statements refer exclusively to targets in this
	// descriptor.
	statementsExpected := map[uint32]*util.FastIntSet{}
	for i := range scs.Targets {
		t := &scs.Targets[i]
		exp, ok := statementsExpected[t.Metadata.StatementID]
		if !ok {
			exp = &util.FastIntSet{}
			statementsExpected[t.Metadata.StatementID] = exp
		}
		exp.Add(int(scs.TargetRanks[i]))
	}
	var statementRanks util.FastIntSet
	for _, s := range scs.RelevantStatements {
		statementRanks.Add(int(s.StatementRank))
		if _, ok := statementsExpected[s.StatementRank]; !ok {
			reportf("unexpected statement %d (%s)",
				s.StatementRank, s.Statement.Statement)
		}
	}

	// Validate that there are no duplicate statements.
	if statementRanks.Len() != len(scs.RelevantStatements) {
		reportf("duplicates exist in RelevantStatements")
	}

	// Validate that all targets have a corresponding statement.
	{
		var expected util.FastIntSet
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
			reportf("missing statement for targets (%s) / (%s)",
				redact.SafeString(strings.Join(ranks, ", ")),
				strings.Join(elementStrs, ", "),
			)
		})
	}

	// TODO(ajwerner): Consider asserting that the current statuses make sense
	// for the target elements.

}
