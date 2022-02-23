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
	"strings"

	"github.com/Masterminds/semver/v3"
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

// findNextRelease finds all required information for the next release.
func findNextRelease(releaseSeries string) (releaseInfo, error) {
	prevReleaseVersion, err := findPreviousRelease(releaseSeries)
	if err != nil {
		return releaseInfo{}, fmt.Errorf("cannot find previous release: %w", err)
	}
	nextReleaseVersion, err := bumpVersion(prevReleaseVersion)
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
func findPreviousRelease(releaseSeries string) (string, error) {
	// TODO: filter version using semantic version, not a git pattern
	cmd := exec.Command("git", "tag", "--list", fmt.Sprintf("v%s.*", releaseSeries))
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("cannot get version from tags: %w", err)
	}
	versions := findVersions(string(output))
	if len(versions) == 0 {
		return "", fmt.Errorf("zero versions found")
	}
	sort.Sort(semver.Collection(versions))
	return versions[len(versions)-1].Original(), nil
}

// bumpVersion increases the patch release version (the last digit) of a given version
func bumpVersion(version string) (string, error) {
	semanticVersion, err := semver.NewVersion(version)
	if err != nil {
		return "", fmt.Errorf("cannot parse version: %w", err)
	}
	nextVersion := semanticVersion.IncPatch()
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

// findCandidateCommits finds all potential merge commits that can be used for the current release.
// It includes all merge commits since previous release.
func findCandidateCommits(prevRelease string, releaseSeries string) ([]string, error) {
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
