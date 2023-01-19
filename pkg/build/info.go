// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package build

import (
	"bytes"
	_ "embed"
	"fmt"
	"runtime"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
)

// TimeFormat is the reference format for build.Time. Make sure it stays in sync
// with the string passed to the linker in the root Makefile.
const TimeFormat = "2006/01/02 15:04:05"

var (
	// These variables are initialized by Bazel via the linker -X flag
	// when compiling release binaries.
	tag             = "unknown" // Tag of this build (git describe --tags w/ optional '-dirty' suffix)
	utcTime         string      // Build time in UTC (year/month/day hour:min:sec)
	rev             string      // SHA-1 of this build (git rev-parse)
	cgoCompiler     = cgoVersion()
	cgoTargetTriple string
	platform        = fmt.Sprintf("%s %s", runtime.GOOS, runtime.GOARCH)
	// Distribution is changed by the CCL init-time hook in non-APL builds.
	Distribution         = "OSS"
	typ                  string // Type of this build: <empty>, "development", or "release"
	channel              = "unknown"
	envChannel           = envutil.EnvOrDefaultString("COCKROACH_CHANNEL", "unknown")
	releaseBinaryVersion = computeVersion(tag)
	//go:embed release-series.txt
	releaseSeries string
)

// IsRelease returns true if the binary was produced by a "release" build.
func IsRelease() bool {
	return typ == "release"
}

// SeemsOfficial reports whether this binary is likely to have come from an
// official release channel.
func SeemsOfficial() bool {
	return channel == "official-binary" || channel == "source-archive"
}

func computeVersion(tag string) string {
	v, err := version.Parse(tag)
	if err != nil {
		if IsRelease() {
			panic(fmt.Sprintf("invalid version for release binary: %v", err))
		}

		return "dev"
	}
	return v.String()
}

// BinaryVersion returns the version precise binary version (including
// patch number) for released binaries; otherwise, it returns the
// release series along with the revision used to build cockroach.
func BinaryVersion() string {
	// Use the binary version computed from a tag as that will be more
	// descriptive, i.e., it will include a patch number and prerelease
	// information. Released binaries are generated from TeamCity builds
	// and should have reliably up-to-date tags.
	if IsRelease() {
		return releaseBinaryVersion
	}

	return fmt.Sprintf("v%s-%s", releaseSeries, rev)
}

// BinaryVersionPrefix returns the version prefix of the current build.
func BinaryVersionPrefix() string {
	return fmt.Sprintf("v%s", releaseSeries)
}

func ReleaseSeries() string {
	return releaseSeries
}

func init() {
	// Allow tests to override the tag.
	if tagOverride := envutil.EnvOrDefaultString(
		"COCKROACH_TESTING_VERSION_TAG", ""); tagOverride != "" {
		tag = tagOverride
	}
}

// Short returns a pretty printed build and version summary.
func (b Info) Short() string {
	plat := b.Platform
	if b.CgoTargetTriple != "" {
		plat = b.CgoTargetTriple
	}
	return fmt.Sprintf("CockroachDB %s %s (%s, built %s, %s)",
		b.Distribution, b.Tag, plat, b.Time, b.GoVersion)
}

// Long returns a pretty printed build summary
func (b Info) Long() string {
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
	fmt.Fprintf(tw, "Build Tag:        %s\n", b.Tag)
	fmt.Fprintf(tw, "Build Time:       %s\n", b.Time)
	fmt.Fprintf(tw, "Distribution:     %s\n", b.Distribution)
	fmt.Fprintf(tw, "Platform:         %s", b.Platform)
	if b.CgoTargetTriple != "" {
		fmt.Fprintf(tw, " (%s)", b.CgoTargetTriple)
	}
	fmt.Fprintln(tw)
	fmt.Fprintf(tw, "Go Version:       %s\n", b.GoVersion)
	fmt.Fprintf(tw, "C Compiler:       %s\n", b.CgoCompiler)
	fmt.Fprintf(tw, "Build Commit ID:  %s\n", b.Revision)
	fmt.Fprintf(tw, "Build Type:       %s", b.Type) // No final newline: cobra prints one for us.
	_ = tw.Flush()
	return buf.String()
}

// GoTime parses the utcTime string and returns a time.Time.
func (b Info) GoTime() time.Time {
	val, err := time.Parse(TimeFormat, b.Time)
	if err != nil {
		return time.Time{}
	}
	return val
}

// Timestamp parses the utcTime string and returns the number of seconds since epoch.
func (b Info) Timestamp() (int64, error) {
	val, err := time.Parse(TimeFormat, b.Time)
	if err != nil {
		return 0, err
	}
	return val.Unix(), nil
}

// GetInfo returns an Info struct populated with the build information.
func GetInfo() Info {
	return Info{
		GoVersion:       runtime.Version(),
		Tag:             tag,
		Time:            utcTime,
		Revision:        rev,
		CgoCompiler:     cgoCompiler,
		CgoTargetTriple: cgoTargetTriple,
		Platform:        platform,
		Distribution:    Distribution,
		Type:            typ,
		Channel:         channel,
		EnvChannel:      envChannel,
	}
}

// TestingOverrideTag allows tests to override the build tag.
func TestingOverrideTag(t string) func() {
	prev := tag
	prevVersion := releaseBinaryVersion
	tag = t
	releaseBinaryVersion = computeVersion(tag)
	return func() { tag = prev; releaseBinaryVersion = prevVersion }
}

// MakeIssueURL produces a URL to a CockroachDB issue.
func MakeIssueURL(issue int) string {
	return fmt.Sprintf("https://go.crdb.dev/issue-v/%d/%s", issue, BinaryVersionPrefix())
}
