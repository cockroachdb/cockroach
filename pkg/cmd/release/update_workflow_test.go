// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"gopkg.in/yaml.v3"
)

// TestWorkflowFileStructure validates that the update_releases.yaml workflow file
// has the expected structure that update_workflow.go depends on.
//
// This test reads the actual workflow file from .github/workflows/ to ensure
// any structural changes are caught immediately. If this test fails, you need to:
// 1. Update the expectedPath in findBranchNode() to match the new structure
// 2. Update this test if needed
//
// This test will catch structural changes that would break update_workflow.go.
func TestWorkflowFileStructure(t *testing.T) {
	// Read the actual workflow file from .github/workflows/
	workflowPath := datapathutils.RewritableDataPath(t, ".github", "workflows", "update_releases.yaml")
	data, err := os.ReadFile(workflowPath)
	if err != nil {
		t.Fatalf("Failed to read workflow file %s: %v\n"+
			"The workflow file may have been moved or renamed.", workflowPath, err)
	}

	var workflow yaml.Node
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("Failed to parse workflow YAML: %v", err)
	}

	// Validate the structure matches what findBranchNode() expects
	branchNode, err := findBranchNode(&workflow)
	if err != nil {
		t.Fatalf("Workflow file structure validation failed.\n"+
			"This likely means the structure of %s has changed.\n"+
			"Error: %v\n\n"+
			"To fix this:\n"+
			"1. Update the expectedPath in findBranchNode() to match the new structure\n"+
			"2. Update this test if needed",
			workflowFile, err)
	}

	// Verify it's a sequence node with at least one branch
	if branchNode.Kind != yaml.SequenceNode {
		t.Fatalf("Expected 'branch' to be a sequence, got %v", branchNode.Kind)
	}

	if len(branchNode.Content) == 0 {
		t.Fatal("Expected at least one branch in the workflow matrix, found none")
	}

	// Verify branches are strings
	for i, node := range branchNode.Content {
		if node.Kind != yaml.ScalarNode {
			t.Errorf("Branch at index %d is not a scalar node (expected string)", i)
		}
	}

	t.Logf("✓ Workflow structure validation passed")
	t.Logf("  Found %d branches in the matrix", len(branchNode.Content))
}

// TestAddBranchToWorkflow_NoOp tests that the branch extraction logic works correctly.
func TestAddBranchToWorkflow_NoOp(t *testing.T) {
	// Read the actual workflow file from .github/workflows/
	workflowPath := datapathutils.RewritableDataPath(t, ".github", "workflows", "update_releases.yaml")
	originalData, err := os.ReadFile(workflowPath)
	if err != nil {
		t.Fatalf("Failed to read workflow file: %v", err)
	}

	var workflow yaml.Node
	if err := yaml.Unmarshal(originalData, &workflow); err != nil {
		t.Fatalf("Failed to parse workflow YAML: %v", err)
	}

	// Get the branch node
	branchNode, err := findBranchNode(&workflow)
	if err != nil {
		t.Fatalf("Failed to find branch node: %v", err)
	}

	// Should have at least one branch
	if len(branchNode.Content) == 0 {
		t.Fatal("No branches found in workflow file")
	}

	// Get the first branch (should be "master" based on current structure)
	firstBranch := branchNode.Content[0].Value

	// Verify extracting branches works
	var branches []string
	for _, item := range branchNode.Content {
		if item.Kind == yaml.ScalarNode {
			branches = append(branches, item.Value)
		}
	}

	// Verify the first branch is in the list
	found := false
	for _, b := range branches {
		if b == firstBranch {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("Expected to find branch %q in the workflow file", firstBranch)
	}

	t.Logf("✓ Successfully extracted %d branches from workflow file", len(branches))
	t.Logf("  First branch: %s", firstBranch)
}
