// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/Masterminds/semver/v3"
)

const remoteOrigin = "origin"

type releaseInfo struct {
	prevReleaseVersion string
	nextReleaseVersion string
	buildInfo          buildInfo
	// candidateCommits contains all merge commits that can be considered as release candidates
	candidateCommits []string
	// releaseSeries represents the major release prefix, e.g. 21.2
	releaseSeries string
}

// findNextVersion returns the next release version for given releaseSeries.
func findNextVersion(releaseSeries string) (string, error) {
	prevReleaseVersion, err := findPreviousRelease(releaseSeries, false)
	if err != nil {
		return "", fmt.Errorf("cannot find previous release: %w", err)
	}
	nextReleaseVersion, err := bumpVersion(prevReleaseVersion)
	if err != nil {
		return "", fmt.Errorf("cannot bump version: %w", err)
	}
	return nextReleaseVersion, nil
}

// findNextRelease finds all required information for the next release.
func findNextRelease(releaseSeries string) (releaseInfo, error) {
	prevReleaseVersion, err := findPreviousRelease(releaseSeries, false)
	if err != nil {
		return releaseInfo{}, fmt.Errorf("cannot find previous release: %w", err)
	}
	nextReleaseVersion, err := bumpVersion(prevReleaseVersion)
	if err != nil {
		return releaseInfo{}, fmt.Errorf("cannot bump version: %w", err)
	}
	candidateCommits, err := findCandidateCommits(prevReleaseVersion, nextReleaseVersion)
	if err != nil {
		return releaseInfo{}, fmt.Errorf("cannot find candidate commits: %w", err)
	}
	info, err := findHealthyBuild(candidateCommits)
	if err != nil {
		return releaseInfo{}, fmt.Errorf("cannot find healthy build: %w", err)
	}
	releasedVersions, err := getVersionsContainingRef(info.SHA)
	if err != nil {
		return releaseInfo{}, fmt.Errorf("cannot check if the candidate sha was released: %w", err)
	}
	if len(releasedVersions) > 0 {
		return releaseInfo{}, fmt.Errorf("%s has been already released as a part of the following tags: %s",
			info.SHA, strings.Join(releasedVersions, ", "))
	}
	return releaseInfo{
		prevReleaseVersion: prevReleaseVersion,
		nextReleaseVersion: nextReleaseVersion,
		buildInfo:          info,
		candidateCommits:   candidateCommits,
		releaseSeries:      releaseSeries,
	}, nil
}

func getVersionsContainingRef(ref string) ([]string, error) {
	cmd := exec.Command("git", "tag", "--contains", ref)
	out, err := cmd.Output()
	if err != nil {
		return []string{}, fmt.Errorf("cannot list tags containing %s: %w", ref, err)
	}
	var versions []string
	for _, v := range findVersions(string(out)) {
		versions = append(versions, v.Original())
	}
	return versions, nil
}

func findVersions(text string) []*semver.Version {
	var versions []*semver.Version
	for _, line := range strings.Split(text, "\n") {
		trimmedLine := strings.TrimSpace(line)
		if trimmedLine == "" {
			continue
		}
		// Skip builds before alpha.1
		if strings.Contains(trimmedLine, "-alpha.0000") {
			continue
		}
		version, err := semver.NewVersion(trimmedLine)
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
		var filteredVersions []*semver.Version
		for _, v := range versions {
			if v.Prerelease() == "" {
				filteredVersions = append(filteredVersions, v)
			}
		}
		versions = filteredVersions
	}
	sort.Sort(semver.Collection(versions))
	return versions[len(versions)-1].Original(), nil
}

// bumpVersion increases the patch release version (the last digit) of a given version.
// For pre-release versions, the pre-release part is bumped.
func bumpVersion(version string) (string, error) {
	// special case for versions like v23.2.0-alpha.00000000
	if strings.HasSuffix(version, "-alpha.00000000") {
		// reset the version to something we can parse and bump
		version = strings.TrimSuffix(version, ".00000000") + ".0"
	}
	semanticVersion, err := semver.NewVersion(version)
	if err != nil {
		return "", fmt.Errorf("cannot parse version: %w", err)
	}
	var nextVersion semver.Version
	if semanticVersion.Prerelease() == "" {
		// For regular releases we can use IncPatch without any modification
		nextVersion = semanticVersion.IncPatch()
	} else {
		// For pre-releases (alpha, beta, rc), we need to implement our own bumper. It takes the last digit and increments it.
		pre := semanticVersion.Prerelease()
		preType, digit, found := strings.Cut(pre, ".")
		if !found {
			return "", fmt.Errorf("parsing prerelease %s", semanticVersion.Original())
		}
		preVersion, err := strconv.Atoi(digit)
		if err != nil {
			return "", fmt.Errorf("atoi prerelease error %s: %w", semanticVersion.Original(), err)
		}
		preVersion++
		nextVersion, err = semanticVersion.SetPrerelease(fmt.Sprintf("%s.%d", preType, preVersion))
		if err != nil {
			return "", fmt.Errorf("bumping prerelease %s: %w", semanticVersion.Original(), err)
		}
	}
	return nextVersion.Original(), nil
}

// filterPullRequests finds commits with a particular merge pattern in the commit message.
// GitHub uses "Merge pull request #NNN" and Bors uses "Merge #NNN" in the generated commit messages.
func filterPullRequests(text string) []string {
	var shas []string
	matchMerge := regexp.MustCompile(`Merge (#|pull request)`)
	for _, line := range strings.Split(text, "\n") {
		if !matchMerge.MatchString(line) {
			continue
		}
		sha := strings.Fields(line)[0]
		shas = append(shas, sha)
	}
	return shas
}

// getMergeCommits lists all merge commits within a range of two refs.
func getMergeCommits(fromRef, toRef string) ([]string, error) {
	cmd := exec.Command("git", "log", "--merges", "--format=format:%H %s", "--ancestry-path",
		fmt.Sprintf("%s..%s", fromRef, toRef))
	out, err := cmd.Output()
	if err != nil {
		return []string{}, fmt.Errorf("cannot read git log output: %w", err)
	}
	return filterPullRequests(string(out)), nil
}

func getCommonBaseRef(fromRef, toRef string) (string, error) {
	cmd := exec.Command("git", "merge-base", fromRef, toRef)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// findReleaseBranch finds the release branch for a version based on a list of branch patterns.
func findReleaseBranch(version string) (string, error) {
	semVersion, err := parseVersion(version)
	if err != nil {
		return "", fmt.Errorf("cannot parse version %s: %w", version, err)
	}
	// List of potential release branches by their priority. The first found will be used as the release branch.
	maybeReleaseBranches := []string{
		// staging-vx.y.z is usually used by extraordinary releases. Top priority.
		fmt.Sprintf("staging-v%d.%d.%d", semVersion.Major(), semVersion.Minor(), semVersion.Patch()),
		// release-x.y.z-rc us used by baking releases.
		fmt.Sprintf("release-%d.%d.%d-rc", semVersion.Major(), semVersion.Minor(), semVersion.Patch()),
		fmt.Sprintf("release-%d.%d", semVersion.Major(), semVersion.Minor()),
		// TODO: add master for alphas
	}
	for _, branch := range maybeReleaseBranches {
		remoteBranches, err := listRemoteBranches(branch)
		if err != nil {
			return "", fmt.Errorf("listing release branch %s: %w", branch, err)
		}
		if len(remoteBranches) > 1 {
			return "", fmt.Errorf("found more than one release branches for %s: %s", branch, strings.Join(remoteBranches, ", "))
		}
		if len(remoteBranches) > 0 {
			return remoteBranches[0], nil
		}
	}
	return "", fmt.Errorf("cannot find release branch for %s", version)
}

// findCandidateCommits finds all potential merge commits that can be used for the current release.
// It includes all merge commits since previous release.
func findCandidateCommits(prevRelease string, version string) ([]string, error) {
	releaseBranch, err := findReleaseBranch(version)
	if err != nil {
		return []string{}, fmt.Errorf("cannot find release branch for %s", version)
	}
	releaseBranch = fmt.Sprintf("%s/%s", remoteOrigin, releaseBranch)
	commonBaseRef, err := getCommonBaseRef(prevRelease, releaseBranch)
	if err != nil {
		return []string{}, fmt.Errorf("cannot find common base ref: %w", err)
	}
	refs, err := getMergeCommits(commonBaseRef, releaseBranch)
	if err != nil {
		return []string{}, fmt.Errorf("cannot get merge commits: %w", err)
	}
	return refs, nil
}

// findHealthyBuild walks all potentials merge commits in reverse order and tries to find the latest healthy build.
// The assumption is that every healthy build has a corresponding metadata file published to the release
// qualification bucket.
func findHealthyBuild(potentialRefs []string) (buildInfo, error) {
	for _, ref := range potentialRefs {
		fmt.Println("Fetching release qualification metadata for", ref)
		meta, err := getBuildInfo(context.Background(), pickSHAFlags.qualifyBucket,
			fmt.Sprintf("%s/%s.json", pickSHAFlags.qualifyObjectPrefix, ref))
		if err != nil {
			// TODO: retry if error is not 404
			fmt.Println("no metadata qualification for", ref, err)
			continue
		}
		return meta, nil
	}
	return buildInfo{}, fmt.Errorf("no ref found")
}

// listRemoteBranches retrieves a list of remote branches using a pattern, assuming the remote name is `origin`.
func listRemoteBranches(pattern string) ([]string, error) {
	cmd := exec.Command("git", "ls-remote", "--refs", remoteOrigin, "refs/heads/"+pattern)
	out, err := cmd.Output()
	if err != nil {
		return []string{}, fmt.Errorf("git ls-remote: %w", err)
	}
	log.Printf("git ls-remote returned: %s", out)
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

// fileExistsInGit checks if a file exists in a local repository, assuming the remote name is `origin`.
func fileExistsInGit(branch string, f string) (bool, error) {
	cmd := exec.Command("git", "ls-tree", remoteOrigin+"/"+branch, f)
	out, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("git ls-tree: %s %s %w, `%s`", branch, f, err, out)
	}
	if len(out) == 0 {
		return false, nil
	}
	return true, nil
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
