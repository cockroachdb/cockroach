// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"log"
	"os"
	"slices"
	"strings"

	"github.com/cockroachdb/version"
	"github.com/spf13/cobra"
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
			log.Printf("WARNING: cannot parse version from branch %s: %v", branch, err)
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

// addBranchToWorkflow adds the specified branch to the workflow file if not already present.
func addBranchToWorkflow(branch string) error {
	// Read the workflow file
	rawData, err := os.ReadFile(workflowFile)
	if err != nil {
		return fmt.Errorf("failed to read workflow file: %w", err)
	}

	lines := strings.Split(string(rawData), "\n")

	// Find the branch section and extract current branches
	currentBranches, branchStart, branchEnd, indent, err := parseBranchSection(lines)
	if err != nil {
		return err
	}

	// Check if branch already exists
	for _, b := range currentBranches {
		if b == branch {
			fmt.Printf("Branch %s is already in the workflow file\n", branch)
			return nil
		}
	}

	// Add the new branch
	currentBranches = append(currentBranches, branch)

	// Sort branches: master first, then release branches in version order
	slices.SortFunc(currentBranches, func(a, b string) int {
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

	// Write the updated file
	if err := writeBranchSection(lines, currentBranches, branchStart, branchEnd, indent); err != nil {
		return err
	}

	fmt.Printf("Added branch %s to workflow file\n", branch)
	return nil
}

// parseBranchSection parses the workflow file to find the branch matrix section.
func parseBranchSection(
	lines []string,
) (branches []string, start int, end int, indent string, err error) {
	start = -1
	end = -1

	for i, line := range lines {
		// Look for "branch:" within the matrix section
		if strings.Contains(line, "branch:") && !strings.HasPrefix(strings.TrimSpace(line), "#") {
			start = i + 1
			// Determine indentation from the next line
			if i+1 < len(lines) {
				nextLine := lines[i+1]
				// Count leading spaces
				trimmed := strings.TrimLeft(nextLine, " ")
				indent = strings.Repeat(" ", len(nextLine)-len(trimmed))
			}
		} else if start != -1 && end == -1 {
			// Check if this line is still part of branch list
			trimmed := strings.TrimSpace(line)
			if trimmed == "" {
				// Empty line, continue
				continue
			}
			if !strings.HasPrefix(trimmed, "-") {
				// Found the end of the branch list
				end = i
				break
			} else {
				// Extract branch name from line like '- "release-25.4"'
				// Remove leading "- " and quotes
				branchLine := strings.TrimSpace(trimmed)
				branchLine = strings.TrimPrefix(branchLine, "- ")
				branchLine = strings.Trim(branchLine, "\"")
				branches = append(branches, branchLine)
			}
		}
	}

	if start == -1 {
		return nil, 0, 0, "", fmt.Errorf("branch section not found in workflow file")
	}

	// If we reached end of file, set end to len(lines)
	if end == -1 {
		end = len(lines)
	}

	return branches, start, end, indent, nil
}

// writeBranchSection writes the updated branch list back to the workflow file.
func writeBranchSection(
	lines []string, branches []string, start int, end int, indent string,
) error {
	// Build new lines
	var newLines []string
	newLines = append(newLines, lines[:start]...)

	// Add branch lines
	for _, branch := range branches {
		newLines = append(newLines, fmt.Sprintf("%s- %q", indent, branch))
	}

	// Add remaining lines
	newLines = append(newLines, lines[end:]...)

	// Write back to file using atomic write pattern
	// Create temp file in the same directory as target to avoid cross-device link errors
	dir := ".github/workflows"
	f, err := os.CreateTemp(dir, "update_releases_*.yaml")
	if err != nil {
		return fmt.Errorf("could not create temporary file: %w", err)
	}
	tmpName := f.Name()
	defer func() {
		if err != nil {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err = f.WriteString(strings.Join(newLines, "\n")); err != nil {
		f.Close()
		return fmt.Errorf("error writing to temp file: %w", err)
	}

	if err = f.Close(); err != nil {
		return fmt.Errorf("error closing temp file: %w", err)
	}

	if err = os.Rename(tmpName, workflowFile); err != nil {
		return fmt.Errorf("error moving file to final destination: %w", err)
	}

	return nil
}
