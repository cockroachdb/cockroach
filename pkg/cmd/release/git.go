// Copyright 2019 The Cockroach Authors.
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
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/Masterminds/semver/v3"
)

type releaseTypeOpt string

const (
	releaseTypeAlpha  = releaseTypeOpt("alpha")
	releaseTypeBeta   = releaseTypeOpt("beta")
	releaseTypeRC     = releaseTypeOpt("rc")
	releaseTypeStable = releaseTypeOpt("stable")
)

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
func findNextVersion(releaseSeries string, releaseType releaseTypeOpt) (string, error) {
	prevReleaseVersion, err := findPreviousRelease(releaseSeries, false)
	if err != nil {
		return "", fmt.Errorf("cannot find previous release: %w", err)
	}
	nextReleaseVersion, err := bumpVersion(prevReleaseVersion, releaseType)
	if err != nil {
		return "", fmt.Errorf("cannot bump version: %w", err)
	}
	return nextReleaseVersion, nil
}

// findNextRelease finds all required information for the next release.
func findNextRelease(releaseSeries string, releaseType releaseTypeOpt) (releaseInfo, error) {
	prevReleaseVersion, err := findPreviousRelease(releaseSeries, false)
	if err != nil {
		return releaseInfo{}, fmt.Errorf("cannot find previous release: %w", err)
	}
	nextReleaseVersion, err := bumpVersion(prevReleaseVersion, releaseType)
	if err != nil {
		return releaseInfo{}, fmt.Errorf("cannot bump version: %w", err)
	}
	candidateCommits, err := findCandidateCommits(prevReleaseVersion, releaseSeries)
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
		// Semantic version doesn't support multiple zeros
		if strings.HasSuffix(trimmedLine, "-alpha.00000000") {
			trimmedLine = strings.ReplaceAll(trimmedLine, "-alpha.00000000", "-alpha.0")
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

// bumpPrelease increases the prerelease number part. For example, given beta.2 it returns beta.3.
func bumpPrerelease(prerelease string) (string, error) {
	fields := strings.SplitN(prerelease, ".", 2)
	if len(fields) != 2 {
		return "", fmt.Errorf("prerelease part should contain 2 fields: %s", prerelease)
	}
	prereleaseNumber, err := strconv.Atoi(fields[1])
	if err != nil {
		return "", fmt.Errorf("prerelease second part is not numeric: %s", prerelease)
	}
	prereleaseNumber++
	return fmt.Sprintf("%s.%d", fields[0], prereleaseNumber), nil
}

// bumpVersion increases the patch release version (the last digit) of a given version
func bumpVersion(version string, releaseType releaseTypeOpt) (string, error) {
	semanticVersion, err := semver.NewVersion(version)
	if err != nil {
		return "", fmt.Errorf("cannot parse version: %w", err)
	}
	if releaseType == releaseTypeStable {
		nextVersion := semanticVersion.IncPatch()
		return nextVersion.Original(), nil
	}
	prerelease := semanticVersion.Prerelease()
	if prerelease == "" {
		return "", fmt.Errorf("cannot create a prerelease version from stable %s", semanticVersion.Original())
	}
	// betas can be derived from alphas or other betas only
	if releaseType == releaseTypeBeta && strings.HasPrefix(prerelease, string(releaseTypeRC)) {
		return "", fmt.Errorf("cannot create a beta version from rc %s", semanticVersion.Original())
	}
	// alphas cn be created from alphas only
	if releaseType == releaseTypeAlpha && !strings.HasPrefix(prerelease, string(releaseTypeAlpha)) {
		return "", fmt.Errorf("cannot create an alpha version from non-alpha %s", semanticVersion.Original())
	}
	if !strings.HasPrefix(prerelease, string(releaseType)) {
		// The case we switch alpha -> beta -> rc
		prerelease = fmt.Sprintf("%s.0", releaseType)
	}
	nextPrerelease, err := bumpPrerelease(prerelease)
	if err != nil {
		return "", fmt.Errorf("cannot bump to next prerelease version from %s: %w", semanticVersion.Original(), err)
	}
	nextVersion, err := semver.NewVersion(fmt.Sprintf("v%d.%d.%d-%s", semanticVersion.Major(), semanticVersion.Minor(),
		semanticVersion.Patch(), nextPrerelease))
	if err != nil {
		return "", fmt.Errorf("cannot set prerelease: %w", err)
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

func branchExists(branch string) bool {
	cmd := exec.Command("git", "rev-parse", "--quiet", "--verify", "origin/"+branch)
	if err := cmd.Run(); err != nil {
		return false
	}
	return true
}

// findCandidateCommits finds all potential merge commits that can be used for the current release.
// It includes all merge commits since previous release.
func findCandidateCommits(prevRelease string, releaseSeries string) ([]string, error) {
	// TODO: for prereleases allow to use master. aplha.1 is a special case,
	// where we go back in history until we find an appropriate green build. Also,
	// the "changes since last" release should be empty?
	// We may need to use some kind of iterator instead of a list of merges in between.
	// For alpha.1 we should use a common base between master and the previous release branch?
	releaseBranch := fmt.Sprintf("origin/release-%s", releaseSeries)
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
