// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package release

import (
	_ "embed"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/version"
	"gopkg.in/yaml.v2"
)

// Series contains information about a cockroachdb release
// series. Specifically, it includes what patch release is the latest
// for this series, which patch releases were withdrawn, and what is
// predecessor series.
type Series struct {
	Latest      string   `yaml:"latest,omitempty"`
	Withdrawn   []string `yaml:"withdrawn,omitempty"`
	Predecessor string   `yaml:"predecessor,omitempty"`
}

var (
	//go:embed cockroach_releases.yaml
	rawReleases []byte

	// releaseData contains the parsed release data as contained in the
	// cockroach_releases.yaml file embedded in the binary.
	releaseData = func() map[string]Series {
		releases, err := parseReleases()
		if err != nil {
			panic(err)
		}

		return releases
	}()
)

func parseReleases() (map[string]Series, error) {
	var result map[string]Series
	err := yaml.UnmarshalStrict(rawReleases, &result)
	if err != nil {
		return nil, fmt.Errorf("invalid cockroach_releases.yaml: %w", err)
	}

	return result, nil
}

// LatestPredecessor returns the latest non-withdrawn predecessor of
// the version passed. For example, if the version is "v19.2.0", this
// will return the latest 19.1 patch release.
func LatestPredecessor(v *version.Version) (string, error) {
	history, err := LatestPredecessorHistory(v, 1)
	if err != nil {
		return "", err
	}

	return history[0], nil
}

// LatestPredecessorHistory returns the last consecutive `k` releases
// that precede the given version in the upgrade order (as dictated by
// cockroach_releases.yaml). E.g., if v=22.2.3 and k=2, then this
// function will return, for example, ["21.2.7", "22.1.6"].
func LatestPredecessorHistory(v *version.Version, k int) ([]string, error) {
	return predecessorHistory(v, k, func(releaseSeries Series) string {
		activeReleases := activePatchReleases(releaseSeries)
		return activeReleases[len(activeReleases)-1]
	})
}

// RandomPredecessor is like LatestPredecessor, but instead of
// returning the latest patch version, it will return a random one.
func RandomPredecessor(rng *rand.Rand, v *version.Version) (string, error) {
	history, err := RandomPredecessorHistory(rng, v, 1)
	if err != nil {
		return "", err
	}

	return history[0], nil
}

// RandomPredecessorHistory is like `LatestPredecessorHistory`, but
// instead of returning a list of the latest patch releases, it will
// return a random non-withdrawn patch release for each release series.
func RandomPredecessorHistory(rng *rand.Rand, v *version.Version, k int) ([]string, error) {
	return predecessorHistory(v, k, func(releaseSeries Series) string {
		activeReleases := activePatchReleases(releaseSeries)
		return activeReleases[rng.Intn(len(activeReleases))]
	})
}

// predecessorHistory computes the history of size `k` for a given
// version (from least to most recent, using the order an actual
// upgrade would have to follow). The `releasePicker` function can be
// used to select which patch release is used at each step.
func predecessorHistory(
	v *version.Version, k int, releasePicker func(Series) string,
) ([]string, error) {
	history := make([]string, k)
	currentV := v
	for i := k - 1; i >= 0; i-- {
		predecessor, err := predecessorSeries(currentV)
		if err != nil {
			return nil, err
		}
		history[i] = releasePicker(predecessor)
		currentV = mustParseVersion(predecessor.Latest)
	}

	return history, nil
}

// activePatchReleases returns a list of patch releases for the given
// release series, filtering out releases that have been withdrawn.
func activePatchReleases(releaseSeries Series) []string {
	isWithdrawn := func(r string) bool {
		for _, w := range releaseSeries.Withdrawn {
			if r == w {
				return true
			}
		}

		return false
	}

	latestVersion := mustParseVersion(releaseSeries.Latest)
	var releases []string
	for patch := 0; patch <= latestVersion.Patch(); patch++ {
		patchVersion := fmt.Sprintf("%d.%d.%d", latestVersion.Major(), latestVersion.Minor(), patch)
		if !isWithdrawn(patchVersion) {
			releases = append(releases, patchVersion)
		}
	}

	return releases
}

// predecessorSeries retrieves the corresponding `Series` data for the
// version passed. Returns an error if the data is not available.
func predecessorSeries(v *version.Version) (Series, error) {
	var empty Series
	seriesStr := versionSeries(v)
	series, ok := releaseData[seriesStr]
	if !ok {
		return empty, fmt.Errorf("no release information for %q (%q series)", v, seriesStr)
	}

	if series.Predecessor == "" {
		return empty, fmt.Errorf("no known predecessor for %q (%q series)", v, seriesStr)
	}

	predSeries, ok := releaseData[series.Predecessor]
	if !ok {
		return empty, fmt.Errorf("no release information for %q (predecessor of %q)", series.Predecessor, v)
	}

	return predSeries, nil
}

func mustParseVersion(str string) *version.Version {
	return version.MustParse("v" + str)
}

func versionSeries(v *version.Version) string {
	return fmt.Sprintf("%d.%d", v.Major(), v.Minor())
}
