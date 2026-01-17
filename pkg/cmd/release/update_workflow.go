// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"embed"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"text/template"

	"github.com/cockroachdb/version"
	"github.com/spf13/cobra"
)

const workflowFile = ".github/workflows/update_releases.yaml"

//go:embed templates/*.tmpl
var templatesFS embed.FS

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

	// Extract current branches from the branch section
	currentBranches, err := extractBranches(string(content))
	if err != nil {
		return err
	}

	// Check if branch already exists
	if slices.Contains(currentBranches, branch) {
		fmt.Printf("Branch %s is already in the workflow file\n", branch)
		return nil
	}

	// Add the new branch and sort
	currentBranches = append(currentBranches, branch)
	sortBranches(currentBranches)

	// Generate new branch section from template
	newBranchSection, err := renderBranchTemplate(currentBranches)
	if err != nil {
		return fmt.Errorf("failed to render branch template: %w", err)
	}

	// Replace the branch section in the workflow file
	newContent, err := replaceBranchSection(string(content), newBranchSection)
	if err != nil {
		return err
	}

	// Write back to file
	if err := os.WriteFile(workflowFile, []byte(newContent), 0644); err != nil {
		return fmt.Errorf("failed to write workflow file: %w", err)
	}

	fmt.Printf("Added branch %s to workflow file\n", branch)
	return nil
}

// extractBranches extracts the list of branches from the workflow file.
func extractBranches(content string) ([]string, error) {
	// Match the branch array in the YAML
	// Looking for:
	//   branch:
	//     - "master"
	//     - "release-X.Y"
	//     ...
	branchPattern := regexp.MustCompile(`(?m)^\s+branch:\s*\n((?:\s+- "[^"]+"\s*\n)+)`)
	matches := branchPattern.FindStringSubmatch(content)
	if len(matches) < 2 {
		return nil, fmt.Errorf("could not find branch section in workflow file")
	}

	branchListStr := matches[1]

	// Extract individual branch names
	branchItemPattern := regexp.MustCompile(`- "([^"]+)"`)
	itemMatches := branchItemPattern.FindAllStringSubmatch(branchListStr, -1)

	var branches []string
	for _, match := range itemMatches {
		if len(match) >= 2 {
			branches = append(branches, match[1])
		}
	}

	if len(branches) == 0 {
		return nil, fmt.Errorf("no branches found in workflow file")
	}

	return branches, nil
}

// renderBranchTemplate renders the branch template with the given branches.
func renderBranchTemplate(branches []string) (string, error) {
	tmpl, err := template.ParseFS(templatesFS, "templates/workflow_branches.tmpl")
	if err != nil {
		return "", fmt.Errorf("failed to parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, map[string]interface{}{
		"Branches": branches,
	}); err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return buf.String(), nil
}

// replaceBranchSection replaces the branch section in the workflow content.
func replaceBranchSection(content, newBranchSection string) (string, error) {
	// Replace the branch list while preserving everything else
	branchPattern := regexp.MustCompile(`(?m)(^\s+branch:\s*\n)(?:\s+- "[^"]+"\s*\n)+`)

	if !branchPattern.MatchString(content) {
		return "", fmt.Errorf("could not find branch section to replace in workflow file")
	}

	newContent := branchPattern.ReplaceAllString(content, "${1}"+newBranchSection)
	return newContent, nil
}
