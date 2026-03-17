// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// TestWorkflowFileStructure validates that the update_releases.yaml workflow file
// has the expected structure that update_workflow.go depends on.
//
// This test reads the actual workflow file from .github/workflows/ to ensure
// any structural changes are caught immediately. If this test fails, you need to:
// 1. Update the expectedPath in findBranchNode() to match the new structure
// 2. Update this test if needed
func TestWorkflowFileStructure(t *testing.T) {
	workflowPath := datapathutils.RewritableDataPath(t, ".github", "workflows", "update_releases.yaml")
	data, err := os.ReadFile(workflowPath)
	require.NoError(t, err, "workflow file may have been moved or renamed")

	var workflow yaml.Node
	require.NoError(t, yaml.Unmarshal(data, &workflow), "failed to parse workflow YAML") //nolint:yaml

	branchNode, err := findBranchNode(&workflow)
	require.NoError(t, err,
		"workflow file structure validation failed — structure of %s may have changed;\n"+
			"update expectedPath in findBranchNode() to match", workflowFile)

	require.Equal(t, yaml.SequenceNode, branchNode.Kind, "expected 'branch' to be a sequence")
	require.NotEmpty(t, branchNode.Content, "expected at least one branch in the workflow matrix")

	for i, node := range branchNode.Content {
		require.Equal(t, yaml.ScalarNode, node.Kind, "branch at index %d is not a string", i)
	}
}

// TestFindBranchNode_ExtractsValues verifies that findBranchNode returns a
// sequence node whose string values can be extracted correctly.
func TestFindBranchNode_ExtractsValues(t *testing.T) {
	workflowPath := datapathutils.RewritableDataPath(t, ".github", "workflows", "update_releases.yaml")
	data, err := os.ReadFile(workflowPath)
	require.NoError(t, err)

	var workflow yaml.Node
	require.NoError(t, yaml.Unmarshal(data, &workflow)) //nolint:yaml

	branchNode, err := findBranchNode(&workflow)
	require.NoError(t, err)
	require.NotEmpty(t, branchNode.Content)

	var branches []string
	for _, item := range branchNode.Content {
		if item.Kind == yaml.ScalarNode {
			branches = append(branches, item.Value)
		}
	}

	// the first branch should be "master"
	require.Equal(t, "master", branches[0], "expected master to be first branch")
	// every extracted value should be non-empty
	for _, b := range branches {
		require.NotEmpty(t, b)
	}
}

// TestSortBranches verifies ordering: master first, then release branches in
// ascending semantic-version order, with fallback to lexicographic order for
// unparseable entries.
func TestSortBranches(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "master first",
			input:    []string{"release-25.1", "master", "release-24.3"},
			expected: []string{"master", "release-24.3", "release-25.1"},
		},
		{
			name:     "semantic version order not lexicographic",
			input:    []string{"release-25.2", "release-9.1", "release-25.10"},
			expected: []string{"release-9.1", "release-25.2", "release-25.10"},
		},
		{
			name:     "two masters degenerate case",
			input:    []string{"master", "release-25.1", "master"},
			expected: []string{"master", "master", "release-25.1"},
		},
		{
			name:     "unparseable falls back to string compare",
			input:    []string{"release-25.1", "not-a-release", "release-24.3"},
			expected: []string{"not-a-release", "release-24.3", "release-25.1"},
		},
		{
			name:     "single entry",
			input:    []string{"master"},
			expected: []string{"master"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := make([]string, len(tc.input))
			copy(got, tc.input)
			sortBranches(got)
			require.Equal(t, tc.expected, got)
		})
	}
}

// TestSelectLatestBranch verifies RC filtering and highest-version selection.
func TestSelectLatestBranch(t *testing.T) {
	tests := []struct {
		name        string
		input       []string
		expected    string
		expectError bool
	}{
		{
			name:     "selects highest version",
			input:    []string{"release-24.3", "release-25.1", "release-25.2"},
			expected: "release-25.2",
		},
		{
			name:     "filters RC branches",
			input:    []string{"release-25.1", "release-25.2-rc", "release-25.2"},
			expected: "release-25.2",
		},
		{
			name:        "all RC branches returns error",
			input:       []string{"release-25.1-rc", "release-25.2-rc"},
			expectError: true,
		},
		{
			name:        "empty input returns error",
			input:       []string{},
			expectError: true,
		},
		{
			name:     "semantic version not lexicographic",
			input:    []string{"release-9.1", "release-25.2", "release-25.10"},
			expected: "release-25.10",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := selectLatestBranch(tc.input)
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, got)
			}
		})
	}
}

// TestAddBranchToWorkflowFile verifies the full mutation path: reading the YAML,
// adding a branch, sorting, re-encoding, and writing back — using a temp copy of
// the real workflow file so the live file is never modified.
func TestAddBranchToWorkflowFile(t *testing.T) {
	workflowPath := datapathutils.RewritableDataPath(t, ".github", "workflows", "update_releases.yaml")
	original, err := os.ReadFile(workflowPath)
	require.NoError(t, err)

	// work on a temp copy
	tmp := filepath.Join(t.TempDir(), "update_releases.yaml")
	require.NoError(t, os.WriteFile(tmp, original, 0644))

	t.Run("adds new branch", func(t *testing.T) {
		require.NoError(t, os.WriteFile(tmp, original, 0644))
		require.NoError(t, addBranchToWorkflowFile("release-99.9", tmp))

		data, err := os.ReadFile(tmp)
		require.NoError(t, err)

		var workflow yaml.Node
		require.NoError(t, yaml.Unmarshal(data, &workflow)) //nolint:yaml
		branchNode, err := findBranchNode(&workflow)
		require.NoError(t, err)

		var branches []string
		for _, item := range branchNode.Content {
			if item.Kind == yaml.ScalarNode {
				branches = append(branches, item.Value)
			}
		}
		require.Contains(t, branches, "release-99.9")
		// master should still be first
		require.Equal(t, "master", branches[0])
	})

	t.Run("idempotent for existing branch", func(t *testing.T) {
		require.NoError(t, os.WriteFile(tmp, original, 0644))
		// add "master" which is already present
		require.NoError(t, addBranchToWorkflowFile("master", tmp))

		data, err := os.ReadFile(tmp)
		require.NoError(t, err)
		// file should be unchanged
		require.Equal(t, string(original), string(data))
	})
}
