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

var printBranchOnly bool

var updateWorkflowBranchesCmd = &cobra.Command{
	Use:   "update-workflow-branches",
	Short: "Update the branch matrix in update_releases.yaml workflow",
	Long:  "Detects the latest release branch and adds it to the GitHub Actions workflow if not present",
	RunE:  updateWorkflowBranches,
}

func init() {
	updateWorkflowBranchesCmd.Flags().BoolVar(
		&printBranchOnly, "print-branch", false,
		"print only the detected branch name to stdout and exit without modifying any files",
	)
}

// updateWorkflowBranches is the main command handler.
func updateWorkflowBranches(_ *cobra.Command, _ []string) error {
	if !printBranchOnly {
		fmt.Fprintln(os.Stderr, "Finding latest release branch...")
	}
	latestBranch, err := findLatestReleaseBranch()
	if err != nil {
		return fmt.Errorf("failed to find latest release branch: %w", err)
	}

	if printBranchOnly {
		fmt.Println(latestBranch)
		return nil
	}

	fmt.Fprintf(os.Stderr, "Latest release branch: %s\n", latestBranch)

	fmt.Fprintln(os.Stderr, "Updating workflow file...")
	if err := addBranchToWorkflow(latestBranch); err != nil {
		return fmt.Errorf("failed to update workflow file: %w", err)
	}

	fmt.Fprintln(os.Stderr, "Successfully updated workflow file")
	return nil
}

// findLatestReleaseBranch detects the latest release branch by querying git remote branches.
func findLatestReleaseBranch() (string, error) {
	branches, err := listRemoteBranches("release-*")
	if err != nil {
		return "", fmt.Errorf("failed to list remote branches: %w", err)
	}
	return selectLatestBranch(branches)
}

// selectLatestBranch returns the highest-version release branch from the
// provided list, filtering out -rc branches.
func selectLatestBranch(branches []string) (string, error) {
	// filter out RC branches; we only want major release branches like "release-25.4"
	var releaseBranches []string
	for _, branch := range branches {
		if !strings.HasSuffix(branch, "-rc") {
			releaseBranches = append(releaseBranches, branch)
		}
	}

	if len(releaseBranches) == 0 {
		return "", fmt.Errorf("no release branches found")
	}

	// parse versions and sort
	type branchVersion struct {
		branch  string
		version version.Version
	}
	var versions []branchVersion

	for _, branch := range releaseBranches {
		// extract version from "release-X.Y" format
		versionStr := strings.TrimPrefix(branch, "release-")
		// add .0 for patch to make it a valid semantic version
		v, err := version.Parse("v" + versionStr + ".0")
		if err != nil {
			// Silently skip branches with non-parseable version strings (e.g. release-1.0, release-2.0).
			continue
		}
		versions = append(versions, branchVersion{branch, v})
	}

	if len(versions) == 0 {
		return "", fmt.Errorf("no valid version branches found")
	}

	slices.SortFunc(versions, func(a, b branchVersion) int {
		return a.version.Compare(b.version)
	})

	return versions[len(versions)-1].branch, nil
}

// sortBranches sorts branches with master first, then release branches in
// ascending version order.
func sortBranches(branches []string) {
	slices.SortFunc(branches, func(a, b string) int {
		if a == "master" {
			if b == "master" {
				return 0
			}
			return -1
		}
		if b == "master" {
			return 1
		}

		// compare release versions
		aVer := strings.TrimPrefix(a, "release-")
		bVer := strings.TrimPrefix(b, "release-")

		va, err1 := version.Parse("v" + aVer + ".0")
		vb, err2 := version.Parse("v" + bVer + ".0")

		// if either fails to parse, fall back to string comparison
		if err1 != nil || err2 != nil {
			return strings.Compare(a, b)
		}

		return va.Compare(vb)
	})
}

// addBranchToWorkflow adds the specified branch to the workflow file if not
// already present.
func addBranchToWorkflow(branch string) error {
	return addBranchToWorkflowFile(branch, workflowFile)
}

// addBranchToWorkflowFile adds the specified branch to the given workflow file
// path if not already present. Exposed separately from addBranchToWorkflow to
// allow tests to operate on a temp file.
func addBranchToWorkflowFile(branch, path string) error {
	// read and parse the workflow file
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read workflow file: %w", err)
	}

	var workflow yaml.Node
	if err := yaml.Unmarshal(data, &workflow); err != nil { //nolint:yaml
		return fmt.Errorf("failed to parse workflow YAML: %w", err)
	}

	// navigate to jobs.update-crdb-releases-yaml.strategy.matrix.branch
	branchNode, err := findBranchNode(&workflow)
	if err != nil {
		return err
	}

	// extract current branches
	var currentBranches []string
	for _, item := range branchNode.Content {
		if item.Kind == yaml.ScalarNode {
			currentBranches = append(currentBranches, item.Value)
		}
	}

	// check if branch already exists
	if slices.Contains(currentBranches, branch) {
		fmt.Fprintf(os.Stderr, "Branch %s is already in the workflow file\n", branch)
		return nil
	}

	// add the new branch and sort
	currentBranches = append(currentBranches, branch)
	sortBranches(currentBranches)

	// update the YAML node with sorted branches
	branchNode.Content = make([]*yaml.Node, 0, len(currentBranches))
	for _, b := range currentBranches {
		branchNode.Content = append(branchNode.Content, &yaml.Node{
			Kind:  yaml.ScalarNode,
			Style: yaml.DoubleQuotedStyle,
			Value: b,
		})
	}

	// marshal back to YAML
	var buf strings.Builder
	encoder := yaml.NewEncoder(&buf)
	encoder.SetIndent(2)
	if err := encoder.Encode(&workflow); err != nil {
		return fmt.Errorf("failed to encode YAML: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return fmt.Errorf("failed to close YAML encoder: %w", err)
	}

	// write back to file
	if err := os.WriteFile(path, []byte(buf.String()), 0644); err != nil {
		return fmt.Errorf("failed to write workflow file: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Added branch %s to workflow file\n", branch)
	return nil
}

// findBranchNode navigates the YAML tree to find the branch array node.
//
// IMPORTANT: This function expects the workflow YAML to have the following structure:
//
//	jobs:
//	  update-crdb-releases-yaml:
//	    strategy:
//	      matrix:
//	        branch:
//	          - "master"
//	          - "release-X.Y"
//
// If the workflow structure changes (job name, nesting, etc.), this function will
// return an error. Update the expectedPath below if the structure needs to change.
func findBranchNode(root *yaml.Node) (*yaml.Node, error) {
	if root.Kind != yaml.DocumentNode || len(root.Content) == 0 {
		return nil, fmt.Errorf("invalid YAML structure: expected document node")
	}

	topMap := root.Content[0]
	if topMap.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("invalid YAML structure: expected mapping at top level")
	}

	// navigate to jobs.update-crdb-releases-yaml.strategy.matrix.branch
	// If this path changes in the workflow file, update it here and in the comment above.
	expectedPath := []string{"jobs", "update-crdb-releases-yaml", "strategy", "matrix", "branch"}
	branchNode, err := navigateYAMLPath(topMap, expectedPath)
	if err != nil {
		return nil, fmt.Errorf("workflow structure does not match expected format.\n"+
			"Expected path: %s\n"+
			"Error: %w\n\n"+
			"If the workflow file structure has changed, update the expectedPath in findBranchNode()",
			strings.Join(expectedPath, " -> "), err)
	}

	if branchNode.Kind != yaml.SequenceNode {
		return nil, fmt.Errorf("expected 'branch' to be a sequence (array), but found %v.\n"+
			"The workflow file structure may have changed. Update the code in findBranchNode() if needed",
			branchNode.Kind)
	}

	return branchNode, nil
}

// navigateYAMLPath navigates through a sequence of keys in a YAML mapping structure.
func navigateYAMLPath(start *yaml.Node, path []string) (*yaml.Node, error) {
	current := start
	for i, key := range path {
		next := findMapValue(current, key)
		if next == nil {
			completedPath := strings.Join(path[:i], " -> ")
			if completedPath != "" {
				completedPath += " -> "
			}
			return nil, fmt.Errorf("key '%s' not found after navigating: %s", key, completedPath+"[HERE]")
		}
		current = next
	}
	return current, nil
}

// findMapValue finds a value in a mapping node by key.
func findMapValue(mapNode *yaml.Node, key string) *yaml.Node {
	if mapNode.Kind != yaml.MappingNode {
		return nil
	}

	// mapping nodes have alternating key-value pairs in Content
	for i := 0; i < len(mapNode.Content); i += 2 {
		if mapNode.Content[i].Value == key {
			return mapNode.Content[i+1]
		}
	}

	return nil
}
