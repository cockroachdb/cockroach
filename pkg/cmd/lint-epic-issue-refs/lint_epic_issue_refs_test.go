// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractFixIssuesIDs(t *testing.T) {
	testCases := []struct {
		message  string
		expected map[string]int
	}{
		{
			message: `workload: Fix folder name generation.

Fixes #75200 #98922
close #75201
closed #592
RESOLVE #5555

Release Notes: None`,
			expected: map[string]int{"#75200": 1, "#98922": 1, "#75201": 1, "#592": 1, "#5555": 1},
		},
		{
			message: `logictestccl: skip flaky TestCCLLogic/fakedist-metadata/partitioning_enum

Informs #75227
Epic CRDB-491

Release note (bug fix): Fixin the bug`,
			expected: map[string]int{},
		},
		{
			message: `lots of variations

fixes #74932; we were reading from the replicas map without...
Closes #74889.
Resolves #74482, #74784  #65117   #79299.
Fix:  #73834
epic: CRDB-9234.
epic CRDB-235, CRDB-40192;   DEVINF-392
Fixed:  #29833 example/repo#941
see also:  #9243
informs: #912,   #4729   #2911  cockroachdb/cockroach#2934

Release note (sql change): Import now checks readability...`,
			expected: map[string]int{"#74932": 1, "#74889": 1, "#74482": 1, "#74784": 1, "#65117": 1, "#79299": 1, "#73834": 1, "#29833": 1, "example/repo#941": 1},
		},
		{
			message: `lots of variations 2

Resolved: #4921
This comes w/ support for Applie Silicon Macs. Closes #72829.
This doesn't completely fix #71901 in that some...
      Fixes #491
Resolves #71614, Resolves #71607
Thereby, fixes #69765
Informs #69765 (point 4).
Fixes #65200. The last remaining 21.1 version (V21_1) can be removed as

Release note (build change): Upgrade to Go 1.17.6
Release note (ops change): Added a metric
Release notes (enterprise change): Client certificates may...`,
			expected: map[string]int{"#4921": 1, "#72829": 1, "#71901": 1, "#491": 1, "#71614": 1, "#71607": 1, "#69765": 1, "#65200": 1},
		},
		{
			message: `testing JIRA tickets

Resolved: doc-4321
This fixes everything. Closes CC-1234.
      Fixes CRDB-12345
Resolves crdb-23456, Resolves DEVINFHD-12345
Fixes #12345
Release notes (sql change): Excellent sql change...`,
			expected: map[string]int{"doc-4321": 1, "CC-1234": 1, "CRDB-12345": 1, "crdb-23456": 1, "DEVINFHD-12345": 1, "#12345": 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.message, func(t *testing.T) {
			result := extractFixIssueIDs(tc.message)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractInformIssueIDs(t *testing.T) {
	testCases := []struct {
		message  string
		expected map[string]int
	}{
		{
			message: `logictestccl: skip flaky TestCCLLogic/fakedist-metadata/partitioning_enum

Informs #75227
Epic CRDB-491
Fix:  #73834

Release note (bug fix): Fixin the bug`,
			expected: map[string]int{"#75227": 1},
		},
		{
			message: `lots of variations

Fixed:  #29833 example/repo#941
see also:  #9243
informs: #912,   #4729   #2911  cockroachdb/cockroach#2934
Informs #69765 (point 4).
This informs #59293 with these additions:

Release note (sql change): Import now checks readability...`,
			expected: map[string]int{"#9243": 1, "#912": 1, "#4729": 1, "#2911": 1, "cockroachdb/cockroach#2934": 1, "#69765": 1, "#59293": 1},
		},
		{
			message: `testing JIRA keys with varying cases

Fixed:  CRDB-12345 example/repo#941
informs: doc-1234, crdb-24680
Informs DEVINF-123, #69765

Release note (sql change): Something something something...`,
			expected: map[string]int{"doc-1234": 1, "DEVINF-123": 1, "#69765": 1, "crdb-24680": 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.message, func(t *testing.T) {
			result := extractInformIssueIDs(tc.message)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractEpicIDs(t *testing.T) {
	testCases := []struct {
		message  string
		expected map[string]int
	}{
		{
			message: `logictestccl: skip flaky TestCCLLogic/fakedist-metadata/partitioning_enum

Informs #75227
Epic CRDB-491
Fix:  #73834

Release note (bug fix): Fixin the bug`,
			expected: map[string]int{"CRDB-491": 1},
		},
		{
			message: `lots of variations

epic: CRDB-9234.
epic CRDB-235, CRDB-40192;   DEVINF-392

Release note (sql change): Import now checks readability...`,
			expected: map[string]int{"CRDB-9234": 1, "CRDB-235": 1, "CRDB-40192": 1, "DEVINF-392": 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.message, func(t *testing.T) {
			result := extractEpicIDs(tc.message)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractEpicNone(t *testing.T) {
	testCases := []struct {
		message  string
		expected bool
	}{
		{
			message: `logictestccl: skip flaky TestCCLLogic/fakedist-metadata/partitioning_enum

Epic CRDB-491
Fix:  #73834

Release note (bug fix): Fixin the bug`,
			expected: false,
		},
		{
			message: `lots of variations

epic: None

Release note (sql change): Import now checks readability...`,
			expected: true,
		},
		{
			message: `another test

Epic nONE

Release note (sql change): Import now checks readability...`,
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.message, func(t *testing.T) {
			result := extractEpicNone(tc.message)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestCheckForMissingRefs(t *testing.T) {
	testCases := []struct {
		message     string
		prInfo      prBodyInfo
		commitInfos []commitInfo
		expected    bool
	}{
		{
			message:     "No refs set anywhere",
			prInfo:      prBodyInfo{},
			commitInfos: []commitInfo{{sha: "d53a4c0"}},
			expected:    false,
		},
		{
			message:     "One commit without refs",
			prInfo:      prBodyInfo{},
			commitInfos: []commitInfo{{sha: "d53a4c0"}, {sha: "128e2038", epicNone: true}},
			expected:    false,
		},
		{
			message:     "Epic ref in PR body",
			prInfo:      prBodyInfo{epicRefs: map[string]int{"CRDB-4123": 1}},
			commitInfos: []commitInfo{{}},
			expected:    true,
		},
		{
			message:     "Epic None set in PR body",
			prInfo:      prBodyInfo{epicNone: true},
			commitInfos: []commitInfo{{}},
			expected:    true,
		},
		{
			message:     "Close ref set in PR body",
			prInfo:      prBodyInfo{issueCloseRefs: map[string]int{"#942": 1}},
			commitInfos: []commitInfo{{}},
			expected:    true,
		},
		{
			message:     "Inform ref set in PR body",
			prInfo:      prBodyInfo{issueInformRefs: map[string]int{"#294": 1}},
			commitInfos: []commitInfo{{}},
			expected:    true,
		},
		{
			message:     "Epic set in commit",
			prInfo:      prBodyInfo{},
			commitInfos: []commitInfo{{sha: "128e2038", epicRefs: map[string]int{"CRDB-3919": 1}}},
			expected:    true,
		},
		{
			message:     "Epic None set in commit",
			prInfo:      prBodyInfo{},
			commitInfos: []commitInfo{{sha: "128e2038", epicNone: true}},
			expected:    true,
		},
		{
			message:     "Inform refs set in commit",
			prInfo:      prBodyInfo{},
			commitInfos: []commitInfo{{sha: "128e2038", issueInformRefs: map[string]int{"#491": 1}}},
			expected:    true,
		},
		{
			message:     "Close refs set in commit",
			prInfo:      prBodyInfo{},
			commitInfos: []commitInfo{{sha: "128e2038", issueCloseRefs: map[string]int{"#94712": 1}}},
			expected:    true,
		},
	}

	params := parameters{Repo: "cockroach", Org: "cockroachdb"}

	for _, tc := range testCases {
		t.Run(tc.message, func(t *testing.T) {
			result := checkForMissingRefs(tc.prInfo, tc.commitInfos, &params)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestCheckPrEpicsUsedInCommits(t *testing.T) {
	testCases := []struct {
		message     string
		prInfo      prBodyInfo
		commitInfos []commitInfo
		expected    bool
	}{
		// Failing checks
		{
			message:     "Only one epic used in a commit",
			prInfo:      prBodyInfo{epicRefs: map[string]int{"CRDB-4123": 1, "CRDB-1929": 1}},
			commitInfos: []commitInfo{{epicRefs: map[string]int{"CRDB-4123": 1}}},
			expected:    false,
		},
		{
			message: "An epic used in a commit is not used in the PR body",
			prInfo:  prBodyInfo{epicRefs: map[string]int{"CRDB-4123": 1, "CRDB-1929": 1}},
			commitInfos: []commitInfo{
				{epicRefs: map[string]int{"CRDB-4123": 1}},
				{epicRefs: map[string]int{"CRDB-1929": 1}},
				{epicRefs: map[string]int{"CRDB-94": 1}},
			},
			expected: false,
		},
		{
			message: "Commit without an epic",
			prInfo:  prBodyInfo{epicRefs: map[string]int{"CRDB-4123": 1, "CRDB-1929": 1}},
			commitInfos: []commitInfo{
				{epicRefs: map[string]int{"CRDB-4123": 1}},
				{epicRefs: map[string]int{"CRDB-1929": 1}},
				{},
			},
			expected: false,
		},
		// Succeeding checks
		{
			message:     "No epics in PR body",
			prInfo:      prBodyInfo{epicRefs: map[string]int{}},
			commitInfos: []commitInfo{{sha: "d53a4c0"}},
			expected:    true,
		},
		{
			message:     "One epic in PR body",
			prInfo:      prBodyInfo{epicRefs: map[string]int{"CRDB-48190": 1}},
			commitInfos: []commitInfo{{sha: "d53a4c0"}},
			expected:    true,
		},
		{
			message:     "Epics used in the same commit",
			prInfo:      prBodyInfo{epicRefs: map[string]int{"CRDB-4123": 1, "CRDB-1929": 1}},
			commitInfos: []commitInfo{{epicRefs: map[string]int{"CRDB-4123": 1, "CRDB-1929": 1}}},
			expected:    true,
		},
		{
			message: "Epics used in different commits",
			prInfo:  prBodyInfo{epicRefs: map[string]int{"CRDB-4123": 1, "CRDB-1929": 1}},
			commitInfos: []commitInfo{
				{epicRefs: map[string]int{"CRDB-4123": 1}},
				{epicRefs: map[string]int{"CRDB-1929": 1}},
			},
			expected: true,
		},
		{
			message: "Epics used in multiple different commits",
			prInfo:  prBodyInfo{epicRefs: map[string]int{"CRDB-4123": 1, "CRDB-1929": 1}},
			commitInfos: []commitInfo{
				{epicRefs: map[string]int{"CRDB-4123": 1}},
				{epicRefs: map[string]int{"CRDB-1929": 1}},
				{epicRefs: map[string]int{"CRDB-1929": 1}},
			},
			expected: true,
		},
		{
			message: "Epics used in different commits plus an Epic none used",
			prInfo:  prBodyInfo{epicRefs: map[string]int{"CRDB-4123": 1, "CRDB-1929": 1}},
			commitInfos: []commitInfo{
				{epicRefs: map[string]int{"CRDB-4123": 1}},
				{epicRefs: map[string]int{"CRDB-1929": 1}},
				{epicNone: true},
			},
			expected: true,
		},
	}

	params := parameters{Repo: "cockroach", Org: "cockroachdb"}

	for _, tc := range testCases {
		t.Run(tc.message, func(t *testing.T) {
			result := checkPrEpicsUsedInCommits(tc.prInfo, tc.commitInfos, &params)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestCheckMultipleEpicsInCommits(t *testing.T) {
	testCases := []struct {
		message     string
		commitInfos []commitInfo
		expected    bool
	}{
		{
			message:     "No epic used in a commit",
			commitInfos: []commitInfo{{epicRefs: map[string]int{}}},
			expected:    true,
		},
		{
			message:     "One epic used in a commit",
			commitInfos: []commitInfo{{epicRefs: map[string]int{"CRDB-4123": 1}}},
			expected:    true,
		},
		{
			message:     "Two epics used in a commit",
			commitInfos: []commitInfo{{epicRefs: map[string]int{"CRDB-4123": 1, "CC-4589": 1}}},
			expected:    false,
		},
		{
			message: "Multiple commits",
			commitInfos: []commitInfo{
				{epicRefs: map[string]int{"CRDB-4123": 1}},
				{epicRefs: map[string]int{"CRDB-1929": 1}},
				{epicRefs: map[string]int{}},
			},
			expected: true,
		},
	}

	params := parameters{Repo: "cockroach", Org: "cockroachdb"}

	for _, tc := range testCases {
		t.Run(tc.message, func(t *testing.T) {
			result := checkMultipleEpicsInCommits(tc.commitInfos, &params)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestMultipPrBackport(t *testing.T) {
	testCases := []struct {
		message  string
		expected bool
	}{
		{
			message:  "Backport 1/1 commits from #71039.",
			expected: false,
		},
		{
			message:  "Backport 2/2 commits from #79612",
			expected: false,
		},
		{
			message:  "Something else entirely",
			expected: false,
		},
		{
			message:  "Backport for 2 commits, 1/1 from #76516 and 1/1 from #79157.",
			expected: true,
		},
		{
			message: `Backport:
  * 1/1 commits from "sql: do not collect stats for virtual columns" (#68312)
  * 1/1 commits from "sql: do not collect statistics on virtual columns" (#71105)`,
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.message, func(t *testing.T) {
			result := isMultiPrBackport(tc.message)
			assert.Equal(t, tc.expected, result)
		})
	}
}
