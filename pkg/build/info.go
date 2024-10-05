// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package build

import (
	"bytes"
	_ "embed"
	"fmt"
	"runtime"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
)

// TimeFormat is the reference format for build.Time. Make sure it stays in sync
// with the string passed to the linker in the root Makefile.
const TimeFormat = "2006/01/02 15:04:05"

var (
	// These variables are initialized by Bazel via the linker -X flag
	// when compiling release binaries.
	utcTime          string // Build time in UTC (year/month/day hour:min:sec)
	rev              string // SHA-1 of this build (git rev-parse)
	buildTagOverride string
	cgoCompiler      = cgoVersion()
	cgoTargetTriple  string
	platform         = fmt.Sprintf("%s %s", runtime.GOOS, runtime.GOARCH)
	// Distribution is changed by the CCL init-time hook in non-APL builds.
	Distribution      = "OSS"
	typ               string // Type of this build: <empty>, "development", or "release"
	channel           string
	envChannel        = envutil.EnvOrDefaultString("COCKROACH_CHANNEL", "unknown")
	enabledAssertions = buildutil.CrdbTestBuild
	//go:embed version.txt
	cockroachVersion string
	binaryVersion    = computeBinaryVersion(cockroachVersion, rev)
)

const (
	DefaultTelemetryChannel = "official-binary"
	FIPSTelemetryChannel    = "official-fips-binary"
)

// IsRelease returns true if the binary was produced by a "release" build.
func IsRelease() bool {
	return typ == "release"
}

// SeemsOfficial reports whether this binary is likely to have come from an
// official release channel.
func SeemsOfficial() bool {
	return channel == DefaultTelemetryChannel || channel == FIPSTelemetryChannel
}

func computeBinaryVersion(versionTxt, revision string) string {
	if buildTagOverride != "" {
		return buildTagOverride
	}
	txt := strings.TrimSuffix(versionTxt, "\n")
	v, err := version.Parse(txt)
	if err != nil {
		panic(fmt.Errorf("could not parse version.txt: %w", err))
	}
	if IsRelease() {
		return v.String()
	}
	if revision != "" {
		return fmt.Sprintf("%s-dev-%s", v.String(), revision)
	}
	return fmt.Sprintf("%s-dev", v.String())
}

// BinaryVersion returns the version prefix, patch number and metadata of the current build.
func BinaryVersion() string {
	return binaryVersion
}

// BinaryVersionPrefix returns the version prefix of the current build.
func BinaryVersionPrefix() string {
	v, err := version.Parse(binaryVersion)
	if err != nil {
		return "dev"
	}
	return fmt.Sprintf("v%d.%d", v.Major(), v.Minor())
}

func init() {
	// Allow tests to override the version.txt contents.
	if versionOverride := envutil.EnvOrDefaultString(
		"COCKROACH_TESTING_VERSION_OVERRIDE", ""); versionOverride != "" {
		TestingOverrideVersion(versionOverride)
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
	fmt.Fprintf(tw, "Build Type:       %s\n", b.Type)
	fmt.Fprintf(tw, "Enabled Assertions: %t", b.EnabledAssertions) // No final newline: cobra prints one for us.
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
	ch := channel
	if ch == "" {
		ch = "unknown"
	}
	return Info{
		GoVersion:         runtime.Version(),
		Tag:               binaryVersion,
		Time:              utcTime,
		Revision:          rev,
		CgoCompiler:       cgoCompiler,
		CgoTargetTriple:   cgoTargetTriple,
		Platform:          platform,
		Distribution:      Distribution,
		Type:              typ,
		Channel:           ch,
		EnvChannel:        envChannel,
		EnabledAssertions: enabledAssertions,
	}
}

// TestingOverrideVersion allows tests to override the binary version
// reported by cockroach.
func TestingOverrideVersion(v string) func() {
	prevBinaryVersion := binaryVersion
	binaryVersion = v
	return func() { binaryVersion = prevBinaryVersion }
}

// MakeIssueURL produces a URL to a CockroachDB issue.
func MakeIssueURL(issue int) string {
	return fmt.Sprintf("https://go.crdb.dev/issue-v/%d/%s", issue, BinaryVersionPrefix())
}
