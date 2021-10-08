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
	// These variables are initialized via the linker -X flag in the
	// top-level Makefile when compiling release binaries.
	tag             = "unknown" // Tag of this build (git describe --tags w/ optional '-dirty' suffix)
	utcTime         string      // Build time in UTC (year/month/day hour:min:sec)
	rev             string      // SHA-1 of this build (git rev-parse)
	cgoCompiler     = cgoVersion()
	cgoTargetTriple string
	platform        = fmt.Sprintf("%s %s", runtime.GOOS, runtime.GOARCH)
	// Distribution is changed by the CCL init-time hook in non-APL builds.
	Distribution  = "OSS"
	typ           string // Type of this build: <empty>, "development", or "release"
	channel       = "unknown"
	envChannel    = envutil.EnvOrDefaultString("COCKROACH_CHANNEL", "unknown")
	binaryVersion = computeVersion(tag)
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
		return "dev"
	}
	return v.String()
}

// BinaryVersion returns the version prefix, patch number and metadata of the current build.
func BinaryVersion() string {
	return binaryVersion
}

// BinaryVersionPrefix returns the version prefix of the current build.
func BinaryVersionPrefix() string {
	v, err := version.Parse(tag)
	if err != nil {
		return "dev"
	}
	return fmt.Sprintf("v%d.%d", v.Major(), v.Minor())
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
	prevVersion := binaryVersion
	tag = t
	binaryVersion = computeVersion(tag)
	return func() { tag = prev; binaryVersion = prevVersion }
}

// MakeIssueURL produces a URL to a CockroachDB issue.
func MakeIssueURL(issue int) string {
	return fmt.Sprintf("https://go.crdb.dev/issue-v/%d/%s", issue, BinaryVersionPrefix())
}
