// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/cockroachdb/version"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const workflowFile = ".github/workflows/update_releases.yaml"

var updateWorkflowBranchesCmd = &cobra.Command{
	Use:   "update-workflow-branches",
	Short: "Update the branch matrix in update_releases.yaml workflow",
	Long:  "Detects the latest release branch and adds it to the GitHub Actions workflow if not present",
	RunE:  updateWorkflowBranches,
}

// updateWorkflowBranches is the main command handler.
func updateWorkflowBranches(_ *cobra.Command, _ []string) error {
	fmt.Println("Finding latest release branch...")
	latestBranch, err := findLatestReleaseBranch()
	if err != nil {
		return fmt.Errorf("failed to find latest release branch: %w", err)
	}

	fmt.Printf("Latest release branch: %s\n", latestBranch)

	fmt.Println("Updating workflow file...")
	if err := addBranchToWorkflow(latestBranch); err != nil {
		return fmt.Errorf("failed to update workflow file: %w", err)
	}

	fmt.Println("Successfully updated workflow file")
	return nil
}

// findLatestReleaseBranch detects the latest release branch by querying git remote branches.
func findLatestReleaseBranch() (string, error) {
	// Get all release branches
	branches, err := listRemoteBranches("release-*")
	if err != nil {
		return "", fmt.Errorf("failed to list remote branches: %w", err)
	}

	// Filter out RC branches - we only want major release branches like "release-25.4"
	var releaseBranches []string
	for _, branch := range branches {
		if !strings.Contains(branch, "-rc") {
			releaseBranches = append(releaseBranches, branch)
		}
	}

	if len(releaseBranches) == 0 {
		return "", fmt.Errorf("no release branches found")
	}

	// Parse versions and sort
	type branchVersion struct {
		branch  string
		version version.Version
	}
	var versions []branchVersion

	for _, branch := range releaseBranches {
		// Extract version from "release-X.Y" format
		versionStr := strings.TrimPrefix(branch, "release-")
		// Add .0 for patch to make it a valid semantic version
		v, err := version.Parse("v" + versionStr + ".0")
		if err != nil {
			fmt.Printf("WARNING: cannot parse version from branch %s: %v\n", branch, err)
			continue
		}
		versions = append(versions, branchVersion{branch, v})
	}

	if len(versions) == 0 {
		return "", fmt.Errorf("no valid version branches found")
	}

	// Sort by version
	slices.SortFunc(versions, func(a, b branchVersion) int {
		return a.version.Compare(b.version)
	})

	// Return highest version
	return versions[len(versions)-1].branch, nil
}

// sortBranches sorts branches with master first, then release branches in version order.
func sortBranches(branches []string) {
	slices.SortFunc(branches, func(a, b string) int {
		if a == "master" {
			return -1
		}
		if b == "master" {
			return 1
		}

		// Compare release versions
		aVer := strings.TrimPrefix(a, "release-")
		bVer := strings.TrimPrefix(b, "release-")

		va, err1 := version.Parse("v" + aVer + ".0")
		vb, err2 := version.Parse("v" + bVer + ".0")

		// If either fails to parse, fall back to string comparison
		if err1 != nil || err2 != nil {
			return strings.Compare(a, b)
		}

		return va.Compare(vb)
	})
}

// addBranchToWorkflow adds the specified branch to the workflow file if not already present.
func addBranchToWorkflow(branch string) error {
	// Read and parse the workflow file
	data, err := os.ReadFile(workflowFile)
	if err != nil {
		return fmt.Errorf("failed to read workflow file: %w", err)
	}

	var workflow yaml.Node
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		return fmt.Errorf("failed to parse workflow YAML: %w", err)
	}

	// Navigate to jobs.update-crdb-releases-yaml.strategy.matrix.branch
	branchNode, err := findBranchNode(&workflow)
	if err != nil {
		return err
	}

	// Extract current branches
	var currentBranches []string
	for _, item := range branchNode.Content {
		if item.Kind == yaml.ScalarNode {
			currentBranches = append(currentBranches, item.Value)
		}
	}

	// Check if branch already exists
	if slices.Contains(currentBranches, branch) {
		fmt.Printf("Branch %s is already in the workflow file\n", branch)
		return nil
	}

	// Add the new branch and sort
	currentBranches = append(currentBranches, branch)
	sortBranches(currentBranches)

	// Update the YAML node with sorted branches
	branchNode.Content = make([]*yaml.Node, 0, len(currentBranches))
	for _, b := range currentBranches {
		branchNode.Content = append(branchNode.Content, &yaml.Node{
			Kind:  yaml.ScalarNode,
			Style: yaml.DoubleQuotedStyle,
			Value: b,
		})
	}

	// Marshal back to YAML
	var buf strings.Builder
	encoder := yaml.NewEncoder(&buf)
	encoder.SetIndent(2)
	if err := encoder.Encode(&workflow); err != nil {
		return fmt.Errorf("failed to encode YAML: %w", err)
	}
	encoder.Close()

	// Write back to file
	if err := os.WriteFile(workflowFile, []byte(buf.String()), 0644); err != nil {
		return fmt.Errorf("failed to write workflow file: %w", err)
	}

	fmt.Printf("Added branch %s to workflow file\n", branch)
	return nil
}

// findBranchNode navigates the YAML tree to find the branch array node.
func findBranchNode(root *yaml.Node) (*yaml.Node, error) {
	// Navigate: root -> jobs (map key) -> jobs (map value) ->
	// update-crdb-releases-yaml (key) -> update-crdb-releases-yaml (value) ->
	// strategy (key) -> strategy (value) -> matrix (key) -> matrix (value) ->
	// branch (key) -> branch (value - sequence)

	if root.Kind != yaml.DocumentNode || len(root.Content) == 0 {
		return nil, fmt.Errorf("invalid YAML structure: expected document node")
	}

	topMap := root.Content[0]
	if topMap.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("invalid YAML structure: expected mapping at top level")
	}

	// Find "jobs" key
	jobsNode := findMapValue(topMap, "jobs")
	if jobsNode == nil {
		return nil, fmt.Errorf("'jobs' key not found in workflow")
	}

	// Find "update-crdb-releases-yaml" key
	jobNode := findMapValue(jobsNode, "update-crdb-releases-yaml")
	if jobNode == nil {
		return nil, fmt.Errorf("'update-crdb-releases-yaml' job not found")
	}

	// Find "strategy" key
	strategyNode := findMapValue(jobNode, "strategy")
	if strategyNode == nil {
		return nil, fmt.Errorf("'strategy' key not found in job")
	}

	// Find "matrix" key
	matrixNode := findMapValue(strategyNode, "matrix")
	if matrixNode == nil {
		return nil, fmt.Errorf("'matrix' key not found in strategy")
	}

	// Find "branch" key
	branchNode := findMapValue(matrixNode, "branch")
	if branchNode == nil {
		return nil, fmt.Errorf("'branch' key not found in matrix")
	}

	if branchNode.Kind != yaml.SequenceNode {
		return nil, fmt.Errorf("'branch' value is not a sequence")
	}

	return branchNode, nil
}

// findMapValue finds a value in a mapping node by key.
func findMapValue(mapNode *yaml.Node, key string) *yaml.Node {
	if mapNode.Kind != yaml.MappingNode {
		return nil
	}

	// Mapping nodes have alternating key-value pairs in Content
	for i := 0; i < len(mapNode.Content); i += 2 {
		if mapNode.Content[i].Value == key {
			return mapNode.Content[i+1]
		}
	}

	return nil
}
