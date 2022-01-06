package main

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

type release struct {
	prevReleaseVersion  string
	nextReleaseVersion  string
	nextReleaseMetadata metadata
	potentialRefs       []string
}

func getNextRelease(releaseSeries string) (release, error) {
	prevReleaseVersion, err := findLastRelease(releaseSeries)
	if err != nil {
		return release{}, err
	}
	nextReleaseVersion, err := nextRelease(prevReleaseVersion)
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
	}, nil
}

func findLastRelease(releaseSeries string) (string, error) {
	// TODO: move the logic from bash to go
	cmd := exec.Command("bash", "-c",
		fmt.Sprintf("git tag --list v%s.* | grep -v alpha | grep -v rc | sort --version-sort | tail -n1",
			releaseSeries))
	version, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(version)), nil
}

func nextRelease(version string) (string, error) {
	// TODO: use semantic version to bump
	fields := strings.Split(version, ".")
	if len(fields) != 3 {
		return "", fmt.Errorf("version should contain 3 parts")
	}
	patch, err := strconv.ParseInt(fields[2], 10, 0)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s.%s.%d", fields[0], fields[1], patch+1), nil
}

func filterPullRequests(text string) []string {
	var shas []string
	for _, line := range strings.Split(text, "\n") {
		if !strings.Contains(line, "Merge pull request") {
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
		meta, err := fetchVersionJSON(context.Background(), qualifyBucket,
			fmt.Sprintf("%s/%s.json", qualifyObjectPrefix, ref))
		if err != nil {
			return metadata{}, err
		}
		return meta, nil
	}
	return metadata{}, fmt.Errorf("no ref found")
}
