// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"errors"
	"fmt"
	"log"
	"os/exec"
	"slices"
	"strings"

	"github.com/cockroachdb/version"
)

const remoteOrigin = "origin"

func findVersions(text string) []version.Version {
	var versions []version.Version
	for _, line := range strings.Split(text, "\n") {
		trimmedLine := strings.TrimSpace(line)
		if trimmedLine == "" {
			continue
		}
		// Skip builds before alpha.1
		if strings.Contains(trimmedLine, "-alpha.0000") {
			continue
		}
		version, err := version.Parse(trimmedLine)
		if err != nil {
			fmt.Printf("WARNING: cannot parse version '%s'\n", trimmedLine)
			continue
		}
		versions = append(versions, version)
	}
	return versions
}

// findPreviousRelease finds the latest version tag for a particular release series.
// It ignores non-semantic versions and tags with the alpha.0* suffix.
// if ignorePrereleases is set to true, only stable versions are returned.
func findPreviousRelease(releaseSeries string, ignorePrereleases bool) (string, error) {
	// TODO: filter version using semantic version, not a git pattern
	pattern := "v*"
	if releaseSeries != "" {
		pattern = fmt.Sprintf("v%s.*", releaseSeries)
	}
	cmd := exec.Command("git", "tag", "--list", pattern)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("cannot get version from tags: %w", err)
	}
	versions := findVersions(string(output))
	if len(versions) == 0 {
		return "", fmt.Errorf("zero versions found")
	}
	if ignorePrereleases {
		var filteredVersions []version.Version
		for _, v := range versions {
			if !v.IsPrerelease() {
				filteredVersions = append(filteredVersions, v)
			}
		}
		versions = filteredVersions
	}
	slices.SortFunc(versions, func(a, b version.Version) int {
		return a.Compare(b)
	})
	return versions[len(versions)-1].String(), nil
}

// bumpVersion increases the patch release version (the last digit) of a given version.
// For pre-release versions, the pre-release part is bumped.
func bumpVersion(versionStr string) (version.Version, error) {
	// special case for versions like v23.2.0-alpha.00000000
	if strings.HasSuffix(versionStr, "-alpha.00000000") {
		// reset the version to something we can parse and bump
		versionStr = strings.TrimSuffix(versionStr, ".00000000") + ".0"
	}
	ver, err := version.Parse(versionStr)
	if err != nil {
		return version.Version{}, fmt.Errorf("cannot parse version: %w", err)
	}
	if ver.IsPrerelease() {
		return ver.IncPreRelease()
	}
	return ver.IncPatch()
}

// listRemoteBranches retrieves a list of remote branches using a pattern, assuming the remote name is `origin`.
func listRemoteBranches(pattern string) ([]string, error) {
	cmd := exec.Command("git", "ls-remote", "--refs", remoteOrigin, "refs/heads/"+pattern)
	out, err := cmd.Output()
	if err != nil {
		return []string{}, fmt.Errorf("git ls-remote: %w", err)
	}
	log.Printf("git ls-remote for %s returned: %s", pattern, out)
	var remoteBranches []string
	// Example output:
	// $ git ls-remote origin "refs/heads/release-23.1*"
	// 0175d195d544b77b286d56703aa5c9f74fb74367	refs/heads/release-23.1
	// eee56b2379446c0a115e6d2cd30735a7efe4fad0	refs/heads/release-23.1.12-rc
	for _, line := range strings.Split(string(out), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return []string{}, fmt.Errorf("cannot find branch specification for %s in `%s`", pattern, line)
		}
		remoteBranches = append(remoteBranches, strings.TrimPrefix(fields[1], "refs/heads/"))
	}
	return remoteBranches, nil
}

// fileContent uses `git cat-file -p ref:file` to get to the file contents without `git checkout`.
func fileContent(ref string, f string) (string, error) {
	cmd := exec.Command("git", "cat-file", "-p", ref+":"+f)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git cat-file %s:%s: %w, `%s`", ref, f, err, out)
	}
	return string(out), nil
}

// isAncestor checks if ref1 is an ancestor of ref2.
// Returns true if ref1 is an ancestor of ref2, false if not, and error if the command fails.
func isAncestor(ref1, ref2 string) (bool, error) {
	cmd := exec.Command("git", "merge-base", "--is-ancestor", remoteOrigin+"/"+ref1, remoteOrigin+"/"+ref2)
	err := cmd.Run()
	if err != nil {
		// Treat exit code 1 as false, as it means that ref1 is not an ancestor of ref2.
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return false, nil
		}
		return false, fmt.Errorf("checking ancestry relationship between %s and %s: %w", ref1, ref2, err)
	}
	return true, nil
}

// mergeCreatesContentChanges checks if a merge commit introduces changes to the branch.
// Returns true if the merge commit introduces changes, false if not, and error if the command fails.
func mergeCreatesContentChanges(branch, intoBranch string, ignoredPatterns []string) (bool, error) {
	// Make sure the working directory is clean
	if err := exec.Command("git", "clean", "-fd").Run(); err != nil {
		return false, fmt.Errorf("cleaning working directory: %w", err)
	}
	// Get the current branch name before we start
	currentBranchCmd := exec.Command("git", "rev-parse", "--abbrev-ref", "HEAD")
	currentBranch, err := currentBranchCmd.Output()
	if err != nil {
		return false, fmt.Errorf("getting current branch: %w", err)
	}
	originalBranch := strings.TrimSpace(string(currentBranch))

	// Checkout the branch to merge into. Use a temporary branch to avoid
	// conflicts with the current branch name.
	tmpIntoBranch := intoBranch + "-tmp"
	checkoutCmd := exec.Command("git", "checkout", "-b", tmpIntoBranch, remoteOrigin+"/"+intoBranch)
	if err := checkoutCmd.Run(); err != nil {
		return false, fmt.Errorf("running checkout: %w", err)
	}

	// Run the merge command without committing and without fast-forward. If
	// fast-forward is allowed and the current branch can fast forward, there
	// will be no merge commit, so the --no-commit option won't work.
	// We need to use the ours strategy to avoid conflicts. In the next step we
	// will checkout the ignored files from the current branch (like version.txt).
	mergeCmd := exec.Command("git", "merge", "--no-commit", "--no-ff", "--strategy=recursive", "-X", "ours", remoteOrigin+"/"+branch)
	if err := mergeCmd.Run(); err != nil {
		return false, fmt.Errorf("running merge: %w", err)
	}
	if len(ignoredPatterns) > 0 {
		coCmd := exec.Command("git", "checkout", tmpIntoBranch, "--")
		coCmd.Args = append(coCmd.Args, ignoredPatterns...)

		if err := coCmd.Run(); err != nil {
			return false, fmt.Errorf("running checkout: %w", err)
		}
	}

	// Check if there are any content changes. The exit code will be analyzed to
	// determine if there are changes after we clean up the current repo.
	diffCmd := exec.Command("git", "diff", "--staged", "--quiet")
	diffErr := diffCmd.Run()

	// Always abort the merge attempt to clean up
	if err := exec.Command("git", "merge", "--abort").Run(); err != nil {
		return false, fmt.Errorf("aborting merge: %w", err)
	}
	if err := exec.Command("git", "checkout", originalBranch).Run(); err != nil {
		return false, fmt.Errorf("running original branch checkout: %w", err)
	}
	// If diff returns no error (exit code 0), there are no changes
	// If diff returns error with exit code 1, there are changes
	// Any other error is unexpected
	if diffErr == nil {
		return false, nil // No changes
	}
	var exitErr *exec.ExitError
	if errors.As(diffErr, &exitErr) && exitErr.ExitCode() == 1 {
		return true, nil // Has changes
	}
	return false, fmt.Errorf("checking diff: %w", diffErr)
}
