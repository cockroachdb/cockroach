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

type release struct {
	prevReleaseVersion  string
	nextReleaseVersion  string
	nextReleaseMetadata metadata
	potentialRefs       []string
	releaseSeries       string
}

func getNextRelease(releaseSeries string) (release, error) {
	prevReleaseVersion, err := findLastRelease(releaseSeries)
	if err != nil {
		return release{}, err
	}
	nextReleaseVersion, err := nextReleaseVersion(prevReleaseVersion)
	if err != nil {
		return release{}, err
	}
	potentialRefs, err := potentialRefs(prevReleaseVersion, releaseSeries)
	if err != nil {
		return release{}, err
	}
	nextReleaseMeta, err := getReleaseMetadata(potentialRefs)
	if err != nil {
		return release{}, err
	}
	return release{
		prevReleaseVersion:  prevReleaseVersion,
		nextReleaseVersion:  nextReleaseVersion,
		nextReleaseMetadata: nextReleaseMeta,
		potentialRefs:       potentialRefs,
		releaseSeries:       releaseSeries,
	}, nil
}

func findLastRelease(releaseSeries string) (string, error) {
	cmd := exec.Command("git", "tag", "--list", fmt.Sprintf("v%s.*", releaseSeries))
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("cannot get version from tags: %w", err)
	}
	var versions []*semver.Version
	for _, line := range strings.Split(string(output), "\n") {
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
	if len(versions) == 0 {
		return "", fmt.Errorf("zero versions found")
	}
	sort.Sort(semver.Collection(versions))
	return versions[len(versions)-1].Original(), nil
}

func nextReleaseVersion(version string) (string, error) {
	semanticVersion, err := semver.NewVersion(version)
	if err != nil {
		return "", fmt.Errorf("cannot parse version: %w", err)
	}
	nextVersion := semanticVersion.IncPatch()
	return nextVersion.Original(), nil
}

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

func getRefs(fromRef, toRef string) ([]string, error) {
	cmd := exec.Command("git", "log", "--merges", "--format=format:%H %s", "--ancestry-path",
		fmt.Sprintf("%s..%s", fromRef, toRef))
	out, err := cmd.Output()
	if err != nil {
		return []string{}, err
	}
	return filterPullRequests(string(out)), nil
}

func potentialRefs(prevRelease string, releaseSeries string) ([]string, error) {
	refs, err := getRefs(prevRelease, fmt.Sprintf("origin/release-%s", releaseSeries))
	if err != nil {
		return []string{}, err
	}
	return refs, nil
}

func getReleaseMetadata(potentialRefs []string) (metadata, error) {
	for _, ref := range potentialRefs {
		fmt.Println("Fetching release qualification metadata for", ref)
		meta, err := fetchVersionJSON(context.Background(), qualifyBucket,
			fmt.Sprintf("%s/%s.json", qualifyObjectPrefix, ref))
		if err != nil {
			// TODO: retry if error is not 404
			fmt.Println("no release qualification for", ref, err)
			continue
		}
		return meta, nil
	}
	return metadata{}, fmt.Errorf("no ref found")
}
