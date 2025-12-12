// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/cockroachdb/version"
	"github.com/spf13/cobra"
)

const workflowFile = ".github/workflows/update_releases.yaml"

const (
	beginMarker = "# BEGIN MANAGED BRANCHES"
	endMarker   = "# END MANAGED BRANCHES"
)

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
	// Read the workflow file
	content, err := os.ReadFile(workflowFile)
	if err != nil {
		return fmt.Errorf("failed to read workflow file: %w", err)
	}

	contentStr := string(content)

	// Find the section between markers
	startIdx := strings.Index(contentStr, beginMarker)
	endIdx := strings.Index(contentStr, endMarker)

	if startIdx == -1 || endIdx == -1 {
		return fmt.Errorf("could not find branch section markers in workflow file")
	}

	// Extract the section between markers
	section := contentStr[startIdx+len(beginMarker) : endIdx]

	// Extract current branches from the section
	branches, indent := extractBranches(section)

	// Check if branch already exists
	if slices.Contains(branches, branch) {
		fmt.Printf("Branch %s is already in the workflow file\n", branch)
		return nil
	}

	// Add the new branch and sort
	branches = append(branches, branch)
	sortBranches(branches)

	// Generate new branch section
	newSection := generateBranchSection(branches, indent)

	// Replace the section between markers
	newContent := contentStr[:startIdx+len(beginMarker)] + newSection + contentStr[endIdx:]

	// Write back to file
	if err := os.WriteFile(workflowFile, []byte(newContent), 0644); err != nil {
		return fmt.Errorf("failed to write workflow file: %w", err)
	}

	fmt.Printf("Added branch %s to workflow file\n", branch)
	return nil
}

// extractBranches extracts the list of branches from the section and determines indentation.
func extractBranches(section string) (branches []string, indent string) {
	// Match lines like:   - "master"
	branchPattern := regexp.MustCompile(`(?m)^(\s+)- "([^"]+)"`)
	matches := branchPattern.FindAllStringSubmatch(section, -1)

	for _, match := range matches {
		if len(match) >= 3 {
			if indent == "" {
				// Capture indentation from first match
				indent = match[1]
			}
			branches = append(branches, match[2])
		}
	}

	return branches, indent
}

// generateBranchSection generates the branch section with proper formatting.
func generateBranchSection(branches []string, indent string) string {
	var lines []string
	lines = append(lines, "\n        branch:")
	for _, branch := range branches {
		lines = append(lines, fmt.Sprintf("%s- %q", indent, branch))
	}
	lines = append(lines, "        ")
	return strings.Join(lines, "\n")
}
